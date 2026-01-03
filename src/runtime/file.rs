//! Reference to an open file descriptor used with I/O runtime.

use std::fs::File;
use std::os::fd::{AsRawFd, RawFd};

/// References to different types of files that can participate in I/O.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IoFile {
    /// A file (potentially) unregistered with the I/O runtime.
    Fd(IoFileFd),

    /// A file that is registered with the I/O runtime.
    /// It's sometimes more performant to use registered files.
    Fixed(IoFixedFd),
}

impl From<&File> for IoFile {
    fn from(value: &File) -> Self {
        Self::Fd(IoFileFd(value.as_raw_fd()))
    }
}

impl From<&IoFixedFd> for IoFile {
    fn from(value: &IoFixedFd) -> Self {
        Self::Fixed(*value)
    }
}

/// Reference to a file based on file handle.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct IoFileFd(pub(super) RawFd);

impl From<IoFileFd> for IoFile {
    fn from(value: IoFileFd) -> Self {
        Self::Fd(value)
    }
}

/// Index to a file registered in I/O uring.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct IoFixedFd(pub(super) u32);

impl From<IoFixedFd> for IoFile {
    fn from(value: IoFixedFd) -> Self {
        Self::Fixed(value)
    }
}
