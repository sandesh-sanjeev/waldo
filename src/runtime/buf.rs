//! Buffers that can be used with the I/O runtime.

use memmap2::{MmapMut, MmapOptions};
use std::{
    ffi::c_void,
    io::Result,
    ops::{Deref, DerefMut},
};

/// Different types of supported buffers.
#[derive(Debug)]
pub enum Buf {
    /// A fixed size buffer, not registered with io-uring.
    Io(IoBuf),

    /// A growable buffer, not registered with io_uring.
    Vec(Vec<u8>),

    /// A fixed size buffer, registered with io-uring.
    Fixed(IoFixedBuf),
}

impl From<IoBuf> for Buf {
    fn from(value: IoBuf) -> Self {
        Self::Io(value)
    }
}

impl From<Vec<u8>> for Buf {
    fn from(value: Vec<u8>) -> Self {
        Self::Vec(value)
    }
}

impl From<IoFixedBuf> for Buf {
    fn from(value: IoFixedBuf) -> Self {
        Self::Fixed(value)
    }
}

impl Memory for Buf {
    fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Io(buf) => buf.as_ptr(),
            Self::Vec(buf) => buf.as_ptr(),
            Self::Fixed(buf) => buf.as_ptr(),
        }
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            Self::Io(buf) => buf.as_mut_ptr(),
            Self::Vec(buf) => buf.as_mut_ptr(),
            Self::Fixed(buf) => buf.as_mut_ptr(),
        }
    }

    fn length(&self) -> u32 {
        match self {
            Self::Io(buf) => buf.length(),
            Self::Vec(buf) => buf.length(),
            Self::Fixed(buf) => buf.length(),
        }
    }
}

/// A slice of bytes that can be shared with kernel.
pub trait Memory {
    /// Raw pointer to the byte slice.
    fn as_ptr(&self) -> *const u8;

    /// Raw mutable pointer to the byte slice.
    fn as_mut_ptr(&mut self) -> *mut u8;

    ///  Number of bytes in the byte slice.
    fn length(&self) -> u32;
}

impl Memory for Vec<u8> {
    fn as_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    fn length(&self) -> u32 {
        u32::try_from(self.len()).unwrap_or(u32::MAX)
    }
}

/// An IoBuf that is registered with the I/O runtime.
#[derive(Debug)]
pub struct IoFixedBuf {
    buf: IoBuf,
    buf_index: u16,
}

impl IoFixedBuf {
    /// Create a new fixed buffer.
    pub(super) fn new(buf: IoBuf, buf_index: u16) -> Self {
        IoFixedBuf { buf, buf_index }
    }

    /// Registered index of the buffer.
    pub(super) fn buf_index(&self) -> u16 {
        self.buf_index
    }
}

impl Memory for IoFixedBuf {
    fn as_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    fn length(&self) -> u32 {
        self.buf.length()
    }
}

impl Deref for IoFixedBuf {
    type Target = IoBuf;
    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl DerefMut for IoFixedBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}

/// A piece of memory that can be shared with the kernel.
///
/// Address of starting offset in memory is guaranteed to be page aligned.
/// It is also (strongly) recommended to allocate memory in multiples of page
/// size, potentially a power of 2 as well. This piece of memory cannot grow
/// or shrink, so, choosing a reason amount of memory to allocate is important.
#[derive(Debug)]
pub struct IoBuf {
    // Page aligned memory that backs this buffer.
    mmap: MmapMut,

    // Number of bytes written into buffer.
    len: u32,
}

impl IoBuf {
    /// Allocate some amount of memory.
    ///
    /// Memory is allocated with huge pages, capacity must be multiples
    /// of the system default page size. Maybe we'll allow for this to
    /// be configurable in the future.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Number of bytes to allocate.
    /// * `huge_pages` - true if memory should be allocated with huge pages.
    pub fn allocate(capacity: u32, huge_pages: bool) -> Result<Self> {
        let capacity = usize::try_from(capacity).expect("Capacity exceeds allocatable memory");

        // Start building options for allocation.
        let mut mmap = MmapOptions::new();
        mmap.populate();
        mmap.len(capacity);

        // If huge pages is enabled, then request memory to be allocated
        // with system default huge page size. Perhaps we should allow this
        // to be configurable?
        if huge_pages {
            mmap.huge(None);
        }

        // Return newly allocated memory.
        Ok(Self {
            len: 0,
            mmap: mmap.map_anon()?,
        })
    }

    /// Maximum number of bytes that can be held in buffer.
    pub fn capacity(&self) -> u32 {
        self.mmap.len() as u32
    }

    /// More bytes that the buffer can hold without overflow.
    pub fn remaining(&self) -> u32 {
        self.capacity().saturating_sub(self.len)
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
    pub fn extend_from_slice(&mut self, src: &[u8]) -> Option<usize> {
        // Make sure the buffer has enough space for the slice.
        let src_len = u32::try_from(src.len()).ok()?;
        let new_len = self.len.checked_add(src_len)?;
        if new_len > self.capacity() {
            return None;
        }

        // Range of bytes to initialize.
        let start = self.len as usize;
        let end = new_len as usize;
        let dst = &mut self.mmap[start..end];

        // Update state and return.
        dst.copy_from_slice(src);
        self.len = new_len;
        Some(new_len as usize)
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
        let new_len = u32::try_from(len).unwrap_or(u32::MAX);
        let new_len = std::cmp::min(new_len, self.capacity());

        // Range of bytes to initialize.
        let start = self.len as usize;
        let end = new_len as usize;
        let dst = &mut self.mmap[start..end];

        // Update state and return.
        dst.fill(value);
        self.len = new_len;
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
    pub fn truncate(&mut self, len: u32) {
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

    /// Create an I/O vec based on this memory.
    ///
    /// Note that this always gives the true range of allocated memory,
    /// regardless of visibility set on the buffer. That this because
    /// the only use-case for this is to register buffers with the runtime.
    pub(super) fn io_vec(&mut self) -> libc::iovec {
        libc::iovec {
            iov_base: self.mmap.as_mut_ptr() as *mut c_void,
            iov_len: self.mmap.len(),
        }
    }

    /// Raw pointer to the current start of the byte slice.
    pub(super) fn as_ptr(&self) -> *const u8 {
        self.mmap.as_ptr()
    }

    /// Raw mutable pointer to the current start of the byte slice.
    pub(super) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.mmap.as_mut_ptr()
    }
}

impl Memory for IoBuf {
    fn as_ptr(&self) -> *const u8 {
        self.mmap.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.mmap.as_mut_ptr()
    }

    fn length(&self) -> u32 {
        self.len
    }
}

impl Deref for IoBuf {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.mmap[..self.len as usize]
    }
}

impl DerefMut for IoBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mmap[..self.len as usize]
    }
}
