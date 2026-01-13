//! A pre-allocated pool of buffers for I/O operations.

use super::IoRuntime;
use memmap2::{MmapMut, MmapOptions};
use std::ffi::c_void;
use std::io::{self, Result};
use std::ops::{Deref, DerefMut};

/// Options to customize the behavior of buffer pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolOptions {
    /// Maximum number of pre-allocated buffers in the pool.
    pub pool_size: u16,

    /// Size of buffers used to perform I/O.
    pub buf_capacity: usize,

    /// Enable huge pages when allocating buffers.
    pub huge_buf: bool,
}

/// A simple buffer pool of fixed size pre-allocated memory.
///
/// This buffer pool does not allocations. Once constructed more allocated buffers
/// cannot be merged into this pool. So, given enough concurrency, this pool can get
/// exhausted. Borrow from pool is quite cheap, for most workloads the recommendation
/// is to release borrowed memory (via drop) when no longer necessary.
#[derive(Debug, Clone)]
pub struct BufPool {
    tx: flume::Sender<RawBytes>,
    rx: flume::Receiver<RawBytes>,
}

impl BufPool {
    /// Create a new buffer pool that is registered to an I/O runtime.
    ///
    /// # Arguments
    ///
    /// * `opts` - Options to use when creating buffer pool.
    /// * `runtime` - I/O runtime to register allocated buffers.
    pub fn registered<A>(opts: PoolOptions, runtime: &mut IoRuntime<A>) -> io::Result<Self> {
        // Allocate all memory for buffer pool.
        let pool_size = usize::from(opts.pool_size);
        let mut buffers = (0..pool_size)
            .map(|_| RawBytes::allocate(opts.buf_capacity, opts.huge_buf))
            .collect::<io::Result<Vec<_>>>()?;

        // Register allocated memory with io uring runtime.
        unsafe { runtime.register_bufs(&mut buffers)? };

        // Populate internal buffers and return.
        let (tx, rx) = flume::bounded(buffers.len());
        for buf in buffers {
            if tx.try_send(buf).is_err() {
                unreachable!("Should have enough space and receivers");
            }
        }

        Ok(Self { tx, rx })
    }

    /// Create a new buffer pool that is not registered to a I/O runtime.
    ///
    /// # Arguments
    ///
    /// * `opts` - Options to use when creating buffer pool.
    pub fn unregistered(opts: PoolOptions) -> io::Result<Self> {
        // Allocate all memory for buffer pool.
        let pool_size = usize::from(opts.pool_size);
        let buffers = (0..pool_size)
            .map(|_| RawBytes::allocate(opts.buf_capacity, opts.huge_buf))
            .collect::<io::Result<Vec<_>>>()?;

        // Populate internal buffers and return.
        let (tx, rx) = flume::bounded(buffers.len());
        for buf in buffers {
            if tx.try_send(buf).is_err() {
                unreachable!("Should have enough space and receivers");
            }
        }

        Ok(Self { tx, rx })
    }

    /// Take a block of pre-allocated memory from the pool.
    ///
    /// Note that this operation blocks till memory is available in the pool.
    /// For a non-blocking variant use [`BufPool::try_take`] or [`BufPool::take_async`] for async.
    pub fn take(&self) -> IoBuf {
        match self.rx.recv() {
            Ok(bytes) => Bytes::new(bytes, &self.tx).into(),
            Err(flume::RecvError::Disconnected) => unreachable!("Have reference to both ends of channel"),
        }
    }

    /// Take a block of pre-allocated memory from the pool, if one is currently available.
    ///
    /// Note that this operation does not block waiting for memory to be available from the pool.
    /// For a blocking variant use [`BufPool::take`] or [`BufPool::take_async`] for async.
    #[allow(dead_code)]
    pub fn try_take(&self) -> Option<IoBuf> {
        match self.rx.try_recv() {
            Err(flume::TryRecvError::Empty) => None,
            Ok(bytes) => Some(Bytes::new(bytes, &self.tx).into()),
            Err(flume::TryRecvError::Disconnected) => unreachable!("Have reference to both ends of channel"),
        }
    }

    /// Take a block of pre-allocated memory from the pool.
    pub async fn take_async(&self) -> IoBuf {
        match self.rx.recv_async().await {
            Ok(bytes) => Bytes::new(bytes, &self.tx).into(),
            Err(flume::RecvError::Disconnected) => unreachable!("Have reference to both ends of channel"),
        }
    }
}

/// A Vec like growable array of bytes with a fixed maximum size.
#[derive(Debug)]
pub struct IoBuf {
    // Page aligned memory that backs this buffer.
    bytes: Bytes,

    // Number of bytes written into buffer.
    len: usize,
}

impl IoBuf {
    /// Number of bytes currently held in buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// True if the buffer is empty, false otherwise.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Maximum number of bytes that can be held in buffer.
    pub fn capacity(&self) -> usize {
        self.bytes.len()
    }

    /// More bytes that the buffer can hold without overflow.
    pub fn remaining(&self) -> usize {
        self.capacity() - self.len
    }

    /// Extend buffer with contents of a byte slice.
    ///
    /// If buffer has enough remaining capacity, copies the slice into
    /// buffer and returns number of bytes consumed. Otherwise returns
    /// None.
    ///
    /// # Arguments
    ///
    /// * `src` - Source byte slice to copy bytes from.
    pub fn extend_from_slice(&mut self, src: &[u8]) {
        // Make sure the buffer has enough space for the slice.
        if self.remaining() < src.len() {
            return;
        }

        // Initialize the range of bytes.
        let new_len = self.len + src.len();
        self.bytes[self.len..new_len].copy_from_slice(src);

        // Update state and return.
        self.len = new_len;
    }

    /// Resize buffer to new length.
    ///
    /// If new length if <= current length, this is a no-op. Otherwise
    /// extends the buffer with the provided till it gets to new length.
    /// Does not exceed allocated capacity.
    ///
    /// # Arguments
    ///
    /// * `len` - New length of the buffer.
    /// * `value` - Value to fill for excess bytes.
    pub fn resize(&mut self, len: usize, value: u8) {
        let new_len = std::cmp::min(len, self.capacity());

        // Initialize the range of bytes.
        self.bytes[self.len..new_len].fill(value);

        // Update state and return.
        self.len = new_len;
    }

    /// Set length without (re) initializing bytes.
    ///
    /// Note that length cannot be set beyond the capacity of allocated bytes.
    /// New length is lower of the provided length or buffer capacity.
    ///
    /// This allows for reads into the buffer without first resizing it,
    /// if you're careful enough, it's not necessary (and never unsafe).
    ///
    /// # Arguments
    ///
    /// * `len` - New length of the buffer.
    pub fn set_len(&mut self, len: usize) {
        self.len = std::cmp::min(len, self.capacity());
    }

    /// Truncate buffer to new size.
    ///
    /// If the new length is >= current length, it is a no-op operation.
    /// However if new length is lesser, the difference is discarded and
    /// lost forever and ever.
    ///
    /// # Arguments
    ///
    /// * `len` - New length of the buffer.
    pub fn truncate(&mut self, len: usize) {
        // Return early if no-op.
        if len >= self.len {
            return;
        }

        // Set new length of the buffer.
        self.len = len;
    }

    /// Set range of bytes visible in the buffer.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// I/O index of the buffer in the runtime, if registered.
    pub(crate) fn io_index(&self) -> Option<u16> {
        self.bytes.io_index()
    }
}

impl Deref for IoBuf {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.bytes[..self.len]
    }
}

impl DerefMut for IoBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes[..self.len]
    }
}

impl From<Bytes> for IoBuf {
    fn from(value: Bytes) -> Self {
        Self { len: 0, bytes: value }
    }
}

/// Pre-allocated memory borrowed from buffer pool.
///
/// Memory is returned back to the pool when it goes out of scope.
#[derive(Debug)]
pub struct Bytes {
    bytes: Option<RawBytes>,
    tx: flume::Sender<RawBytes>,
}

impl Bytes {
    fn new(slice: RawBytes, tx: &flume::Sender<RawBytes>) -> Self {
        Self {
            bytes: Some(slice),
            tx: tx.clone(),
        }
    }
}

impl Drop for Bytes {
    fn drop(&mut self) {
        // Return allocated memory back to the pool.
        if let Some(bytes) = self.bytes.take() {
            // If the pool has been dropped, then, release the allocated memory.
            // Nothing else we can do, graceful shutdown is probably in progress.
            if let Err(flume::SendError(bytes)) = self.tx.send(bytes) {
                drop(bytes); // Explicit drop is not necessary.
            }
        }
    }
}

impl Deref for Bytes {
    type Target = RawBytes;

    fn deref(&self) -> &Self::Target {
        self.bytes.as_ref().expect("no reference to allocated memory")
    }
}

impl DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.bytes.as_mut().expect("no reference to allocated memory")
    }
}

/// Raw blob of allocated bytes.
///
/// Note that allocated bytes have starting offset that is guaranteed to be
/// page aligned, if that matters for you.
#[derive(Debug)]
pub struct RawBytes {
    mmap: MmapMut,
    io_index: Option<u16>,
}

impl RawBytes {
    /// Allocate bytes from the operating system.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Number of bytes to allocate.
    /// * `huge` - True to allocate with huge pages, false otherwise.
    pub fn allocate(capacity: usize, huge: bool) -> Result<Self> {
        let mut mmap = MmapOptions::new();
        mmap.populate();
        mmap.len(capacity);

        // If huge pages is enabled, then request memory to be allocated
        // with system default huge page size. Perhaps we should allow this
        // to be configurable?
        if huge {
            mmap.huge(None);
        }

        // Return newly allocated memory.
        Ok(Self {
            io_index: None,
            mmap: mmap.map_anon()?,
        })
    }

    /// Get index in io runtime where this buffer is registered.
    pub(super) fn io_index(&self) -> Option<u16> {
        self.io_index
    }

    /// Set index in io runtime where this buffer is registered.
    ///
    /// # Arguments
    ///
    /// * `index` - Index of the registered buffer in I/O runtime.
    pub(super) fn set_io_index(&mut self, index: u16) {
        self.io_index = Some(index);
    }
}

impl Deref for RawBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.mmap
    }
}

impl DerefMut for RawBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mmap
    }
}

impl From<&mut RawBytes> for libc::iovec {
    fn from(value: &mut RawBytes) -> Self {
        libc::iovec {
            iov_base: value.as_mut_ptr() as *mut c_void,
            iov_len: value.len(),
        }
    }
}
