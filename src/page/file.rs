//! Persistent disk storage for a page.

use crate::runtime::{IoAction, IoBuf, IoFile, IoFixedFd};
use memmap2::{Advice, Mmap, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Result};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

/// Options for file backing a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FileOptions {
    /// Maximum size of a page file.
    pub(super) capacity: u64,

    /// Equivalent to enabling O_DSYNC with file handle.
    pub(super) o_dsync: bool,
}

/// An append only file backing a page.
///
/// Since this is an append only page, it might run out of capacity.
/// When that happens, the page must be rotated/reset to make space
/// for new log records.
#[derive(Debug)]
pub(super) struct PageFile {
    file: File,
    io_file: IoFile,
    state: FileState,
    pending: PendingIo,
}

impl PageFile {
    /// Open file for a page.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file on disk.
    /// * `opts` - Options for the file.
    pub(super) fn open<P: AsRef<Path>>(path: P, opts: FileOptions) -> Result<Self> {
        // All the default options for a page fie.
        let mut options = OpenOptions::new();
        options.truncate(false);
        options.create(true);
        options.write(true);
        options.read(true);

        // Check if O_DSYNC has to be enabled.
        if opts.o_dsync {
            options.custom_flags(libc::O_DSYNC);
        }

        // Open file and fetch associated metadata.
        let file = options.open(path)?;
        let metadata = file.metadata()?;

        // This might be overwritten with an io file
        // that is registered with I/O runtime.
        let io_file = IoFile::from(&file);

        Ok(Self {
            file,
            io_file,
            pending: Default::default(),
            state: FileState::new(metadata.len(), opts),
        })
    }

    /// Current state of the file.
    pub(super) fn state(&self) -> FileState {
        self.state
    }

    /// Pending I/O actions on the file.
    pub(super) fn pending(&self) -> PendingIo {
        self.pending
    }

    /// A immutable memory mapped reference to underlying file.
    ///
    /// It is optimized for sequential read across the entire file. As of now,
    /// the only use for this method is during initialization of storage.
    pub(super) fn mmap(&self) -> Result<Mmap> {
        let mmap = unsafe { MmapOptions::new().map(&self.file)? };
        mmap.advise(Advice::Sequential)?;
        Ok(mmap)
    }

    /// Raw file descriptor to the underlying file.
    pub(super) fn raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }

    /// Set registered file in io-uring runtime.
    ///
    /// # Arguments
    ///
    /// * `file` - File registered in io-uring runtime.
    pub(super) fn set_io_file(&mut self, file: IoFixedFd) {
        self.io_file = file.into();
    }

    /// Shorten the length of file.
    ///
    /// Note that the operation is no-op if new length is >= current.
    ///
    /// # Arguments
    ///
    /// * `len` - New length of the file.
    pub(super) fn truncate(&mut self, len: u64) -> Result<()> {
        // Do not extend beyond current size of the file.
        if self.state.offset <= len {
            return Ok(());
        }

        // Trim the underlying file.
        self.file.set_len(0)?;
        self.file.sync_all()?;

        // Update state and return.
        self.state.resize(0);
        Ok(())
    }

    /// Gracefully close the underlying file.
    pub(super) fn close(self) -> Result<()> {
        // If size of the file as known by the OS is > known size,
        // it means there were unfinished/unsuccessful writes to file.
        // We truncate those excess bytes here. This is not strictly
        // necessary since maintenance activities are performed when open.
        let metadata = self.file.metadata()?;
        if metadata.len() > self.state.offset {
            self.file.set_len(self.state.offset)?;
        }

        // Make sure all changes are flushed to disk.
        self.file.sync_all()
    }

    /// Append a blob of bytes.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer of bytes.
    pub(super) fn append(&mut self, buf: IoBuf) -> IoAction {
        self.pending.append = true;
        IoAction::write_at(self.io_file, self.state.offset, buf)
    }

    /// Read a range of bytes.
    ///
    /// # Arguments
    ///
    /// * `offset` - Offset to begin read.
    /// * `buf` - Destination buffer to write bytes.
    pub(super) fn query(&mut self, offset: u64, buf: IoBuf) -> IoAction {
        self.pending.query += 1;
        IoAction::read_at(self.io_file, offset, buf)
    }

    /// Flush any intermediate buffers and sync file changes to disk.
    pub(super) fn fsync(&mut self) -> IoAction {
        self.pending.fsync = true;
        IoAction::fsync(self.io_file)
    }

    /// Clear all bytes from file.
    ///
    /// This effectively truncates the file to size 0.
    pub(super) fn clear(&mut self) -> IoAction {
        self.pending.reset = true;
        IoAction::resize(self.io_file, 0)
    }

    /// Apply results from a successful async I/O operation.
    ///
    /// # Arguments
    ///
    /// * `result` - Result from the I/O operation.
    /// * `action` - Async I/O operation completed.
    pub(super) fn apply(&mut self, result: u32, action: &mut IoAction) -> Result<()> {
        match action {
            IoAction::Fsync { .. } => {
                self.pending.fsync = false;
            }

            IoAction::Resize { len, .. } => {
                self.pending.reset = false;
                self.state.resize(*len);
            }

            IoAction::Read { buf, .. } => {
                self.pending.query -= 1;

                // Trim the excess initialized bytes.
                if buf.len() > result as usize {
                    buf.truncate(result as _);
                }
            }

            IoAction::Write { buf, .. } => {
                self.pending.append = false;

                // Make sure all the bytes were written.
                if buf.len() != result as usize {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Incomplete write"));
                }

                // Apply the state updates.
                self.state.apply(buf.len());
            }
        };

        Ok(())
    }

    /// Apply results from a failed async I/O operation.
    ///
    /// # Arguments
    ///
    /// * `action` - Async I/O operation completed.
    pub(super) fn abort(&mut self, action: &mut IoAction) {
        match action {
            IoAction::Fsync { .. } => {
                self.pending.fsync = false;
            }

            IoAction::Resize { .. } => {
                self.pending.reset = false;
            }

            IoAction::Read { .. } => {
                self.pending.query -= 1;
            }

            IoAction::Write { .. } => {
                self.pending.append = false;
            }
        };
    }
}

/// Current state of a sparse index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FileState {
    pub(super) offset: u64,
    pub(super) opts: FileOptions,
}

impl FileState {
    fn new(offset: u64, opts: FileOptions) -> Self {
        Self { opts, offset }
    }

    /// true if new bytes overflow the file, false otherwise.
    ///
    /// # Arguments
    ///
    /// * `len` - Number of bytes attempting to be appended.
    pub(super) fn is_overflow(&self, len: usize) -> bool {
        (self.offset + len as u64) > self.opts.capacity
    }

    /// Apply some size of bytes.
    ///
    /// # Arguments
    ///
    /// * `len` - Number of bytes appended.
    pub(super) fn apply(&mut self, len: usize) {
        self.offset += len as u64;
    }

    /// Resize the file.
    ///
    /// # Arguments
    ///
    /// * `len` - New length of the file.
    pub(super) fn resize(&mut self, len: u64) {
        self.offset = len;
    }
}

/// Status around pending I/O actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct PendingIo {
    pub(super) reset: bool,
    pub(super) query: u32,
    pub(super) append: bool,
    pub(super) fsync: bool,
}

impl PendingIo {
    /// true if there is any pending I/O actions on the file, false otherwise.
    pub(super) fn has_pending(&self) -> bool {
        self.reset || self.append || self.fsync || self.query > 0
    }
}
