//! Buffers that can be used with the I/O runtime.

use memmap2::{MmapMut, MmapOptions};
use std::{
    io::Result,
    ops::{Deref, DerefMut},
};

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

    /// Raw pointer to the byte slice.
    pub(super) fn as_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    /// Raw mutable pointer to the byte slice.
    pub(super) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    ///  Number of bytes in the byte slice.
    pub(super) fn io_len(&self) -> u32 {
        self.buf.io_len()
    }
}

impl Deref for IoFixedBuf {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.buf.as_ref()
    }
}

impl DerefMut for IoFixedBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_mut()
    }
}

/// A piece of memory that can be shared with the kernel.
///
/// Address of starting offset in memory is guaranteed to be page aligned.
/// It is also (strongly) recommended to allocate memory in multiples of page
/// size, potentially a power of 2 as well. This piece of memory cannot grow
/// or shrink, so, choosing a reason amount of memory to allocate is important.
///
/// With that out of the way, there is really only one advantage to using this,
/// buffers can be used to perform DMA against disk (via O_DIRECT). Might also
/// help with heap fragmentation depending on how buffers are allocated.
#[derive(Debug)]
pub struct IoBuf(MmapMut);

impl IoBuf {
    /// Allocate some amount of memory.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Number of bytes to allocate.
    pub fn allocate(capacity: usize) -> Result<Self> {
        let mmap = MmapOptions::new()
            // TODO: Support huge pages, maybe?
            .populate()
            .len(capacity)
            .map_anon()?;

        // Return newly allocated memory.
        Ok(Self(mmap))
    }

    /// Raw pointer to the byte slice.
    pub(super) fn as_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }

    /// Raw mutable pointer to the byte slice.
    pub(super) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }

    ///  Number of bytes in the byte slice.
    pub(super) fn io_len(&self) -> u32 {
        u32::try_from(self.0.len()).expect("I/O buf should be <= u32::MAX")
    }
}

impl Deref for IoBuf {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for IoBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<[u8]> for IoBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsMut<[u8]> for IoBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}
