//! Persistent disk storage for a page.

use crate::runtime::{IoAction, IoBuf, IoFile, IoFixedFd};
use memmap2::{Advice, Mmap, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    io::{Error, ErrorKind, Result},
    os::{
        fd::{AsRawFd, RawFd},
        unix::fs::OpenOptionsExt,
    },
    path::Path,
};

/// Options to influence working of file backing a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileOpts {
    /// Maximum size of a page file.
    pub capacity: u64,

    /// Equivalent to enabling O_DSYNC with file handle.
    ///
    /// If enabled (i.e true), writes won't complete unless updates bytes
    /// and related metadata are guaranteed to be flushed to disk from
    /// kernel metadata tables. Enable if durablity if critical for you.
    /// There is a huge performance penalty associated with it, so not
    /// enabled by default.
    pub o_dsync: bool,
}

#[derive(Debug)]
pub(super) struct PageFile {
    file: File,
    io_file: IoFile,
    state: FileState,
    pending: PendingIo,
}

impl PageFile {
    pub(super) fn new<P: AsRef<Path>>(path: P, opts: FileOpts) -> Result<Self> {
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

    pub(super) fn raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }

    pub(super) fn set_io_file(&mut self, io_file: IoFixedFd) {
        self.io_file = io_file.into();
    }

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

    pub(super) fn close(self) -> Result<()> {
        let metadata = self.file.metadata()?;
        if metadata.len() > self.state.offset {
            self.file.set_len(self.state.offset)?;
        }

        self.file.sync_all()
    }

    pub(super) fn append(&mut self, buf: IoBuf) -> IoAction {
        self.pending.append = true;
        IoAction::write_at(self.io_file, self.state.offset, buf)
    }

    pub(super) fn query(&mut self, offset: u64, buf: IoBuf) -> IoAction {
        self.pending.query += 1;
        IoAction::read_at(self.io_file, offset, buf)
    }

    pub(super) fn fsync(&mut self) -> IoAction {
        self.pending.fsync = true;
        IoAction::fsync(self.io_file)
    }

    pub(super) fn reset(&mut self) -> IoAction {
        self.pending.reset = true;
        IoAction::resize(self.io_file, 0)
    }

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

    pub(super) fn abort(&mut self, action: &mut IoAction) {
        match action {
            IoAction::Fsync { .. } => {
                self.pending.fsync = false;
            }

            IoAction::Resize { .. } => {
                self.pending.reset = false;
            }

            IoAction::Read { buf, .. } => {
                self.pending.query -= 1;
                buf.clear(); // To make sure no garbage bytes.
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
    pub(super) opts: FileOpts,
}

impl FileState {
    fn new(offset: u64, opts: FileOpts) -> Self {
        Self { opts, offset }
    }

    pub(super) fn is_overflow(&self, size: usize) -> bool {
        (self.offset + size as u64) > self.opts.capacity
    }

    pub(super) fn apply(&mut self, len: usize) {
        self.offset += len as u64;
    }

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
