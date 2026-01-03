//! A minimal Async I/O runtime backed by io-uring.

mod alloc;
mod file;

pub use alloc::{BufPool, Bytes, IoBuf, PoolOptions, RawBytes};
pub use file::{IoFile, IoFileFd, IoFixedFd};

use io_uring::squeue::{self, Flags};
use io_uring::{IoUring, opcode, types};
use std::{collections::HashMap, io, os::fd::RawFd};
use thiserror::Error;

/// Error result from an async I/O operation.
#[derive(Debug, Error)]
#[error("I/O error from the runtime: {error}")]
pub struct IoError<A> {
    pub error: io::Error,
    pub attachment: A,
    pub action: IoAction,
}

/// Successful result from an async I/O operation.
#[derive(Debug)]
pub struct IoResponse<A> {
    pub result: u32,
    pub attachment: A,
    pub action: IoAction,
}

/// An io-uring based runtime to execute async file I/O.
pub struct IoRuntime<A> {
    // A monotonically increasing clock in the runtime.
    io_clock: u64,

    // Ring buffers to communicate with the kernel.
    io_ring: IoUring,

    // Flags to add to all sqe.
    flags: Option<Flags>,

    // I/O requests that are currently in progress.
    io_futures: HashMap<u64, (A, IoAction)>,
}

impl<A> IoRuntime<A> {
    /// Create a new async I/O runtime.
    pub fn new(queue_depth: u32) -> io::Result<Self> {
        let mut io_ring = IoUring::builder()
            .setup_single_issuer()
            .setup_coop_taskrun()
            .build(queue_depth)?;

        // Our limit on maximum pending I/O.
        // To protect against completion queue overflow.
        let cq_capacity = io_ring.completion().capacity();

        Ok(Self {
            io_ring,
            io_clock: 0,
            // TODO: Make this configurable.
            // Perhaps this is a per I/O flag instead?
            flags: Some(Flags::ASYNC),
            io_futures: HashMap::with_capacity(cq_capacity),
        })
    }

    /// Register files with the I/O runtime.
    ///
    /// Sometimes this results in more efficient file I/O because kernel can
    /// hold reference to a file across multiple I/O requests.
    ///
    /// # Arguments
    ///
    /// * `files` - Files to register with the runtime.
    pub fn register_files(&mut self, files: &[RawFd]) -> io::Result<Vec<IoFixedFd>> {
        self.io_ring.submitter().register_files(files)?;
        Ok(files.iter().enumerate().map(|(i, _)| IoFixedFd(i as _)).collect())
    }

    /// Register buffers with the I/O runtime.
    ///
    /// Sometimes this results in more efficient file I/O because kernel can
    /// map buffer address space once across multiple I/O requests.
    ///
    /// # Safety
    ///
    /// Registered buffers must remain stable and valid while I/O runtime is active.
    ///
    /// # Arguments
    ///
    /// * `bufs` - Buffers to register with the runtime.
    pub unsafe fn register_bufs(&mut self, bufs: &mut [RawBytes]) -> io::Result<()> {
        // The kind of sized buffers kernel understands.
        let io_bufs: Vec<_> = bufs.iter_mut().map(libc::iovec::from).collect();

        // Safety is upheld by the caller.
        unsafe { self.io_ring.submitter().register_buffers(&io_bufs)? };

        // Assign I/O index for the registered buffer.
        bufs.iter_mut()
            .enumerate()
            .for_each(|(i, buf)| buf.set_io_index(i as _));

        Ok(())
    }

    /// Maximum number of entries that can be fit into submission queue.
    pub fn sq_capacity(&mut self) -> usize {
        self.io_ring.submission().capacity()
    }

    /// Maximum number of entries that can be fit into completion queue.
    pub fn cq_capacity(&mut self) -> usize {
        self.io_ring.completion().capacity()
    }

    /// Number of I/O actions runtime can submitted without sq overflow.
    pub fn sq_remaining(&mut self) -> usize {
        let sq = self.io_ring.submission();
        sq.capacity().saturating_sub(sq.len())
    }

    /// Number of I/O actions runtime can submitted without cq overflow.
    pub fn cq_remaining(&mut self) -> usize {
        let cq = self.io_ring.completion();
        cq.capacity().saturating_sub(cq.len())
    }

    /// Number of I/O actions more that can be completed without cq overflow.
    pub fn pending_io(&self) -> usize {
        self.io_futures.len()
    }

    /// Push an I/O request into the runtime.
    ///
    /// Note that this does not submit the I/O request, just enqueues it. Use
    /// [`IoRuntime::submit_and_wait`] to submit the I/O request to kernel.
    /// But that is a syscall and it would be efficient to enqueues as many I/O
    /// requests are possible before a submit.
    ///
    /// # Arguments
    ///
    /// * `action` - I/O action to enqueue.
    /// * `attachment` - An opaque attachment to include with the request.
    pub fn push(&mut self, mut action: IoAction, attachment: A) -> Result<(), (A, IoAction)> {
        // First progress clock in the runtime.
        let epoch = self.io_clock_tick();

        // Take care to not overflow the completion queue.
        if self.io_futures.len() >= self.cq_capacity() {
            return Err((attachment, action));
        }

        // An I/O request that io-uring understands.
        let sqe = action.io_sqe().user_data(epoch);
        let sqe = match self.flags {
            None => sqe,
            Some(flags) => sqe.flags(flags),
        };

        // Attempt to enqueue the I/O request.
        unsafe {
            // Safety: We have exclusive ownership of the buffer.
            // Buffer will remain at stable address and will not be mutated in user space.
            if self.io_ring.submission().push(&sqe).is_err() {
                // Action is rejected if the submission queue is full.
                return Err((attachment, action));
            }
        };

        // Keep track of the I/O request and return.
        let prev = self.io_futures.insert(epoch, (attachment, action));
        assert!(prev.is_none(), "Overwriting a pending I/O request");
        Ok(())
    }

    /// Get the next completed event from the runtime.
    pub fn pop(&mut self) -> Option<Result<IoResponse<A>, IoError<A>>> {
        // Fetch the next completion event from the queue.
        let cqe = self.io_ring.completion().next()?;

        // Get the associated I/O request.
        let epoch = cqe.user_data();
        let (attachment, action) = self
            .io_futures
            .remove(&epoch)
            .expect("Received completion for an unknown request");

        // Return early if there was an error.
        if cqe.result() < 0 {
            return Some(Err(IoError {
                action,
                attachment,
                error: io::Error::from_raw_os_error(cqe.result()),
            }));
        }

        // Unpack and return the result.
        Some(Ok(IoResponse {
            action,
            attachment,
            result: cqe.result() as u32,
        }))
    }

    /// Submit all the pending I/O requests.
    ///
    /// In addition to submission of the pending requests, also waits for 0
    /// or more requests in flight to complete. Completed requests must be
    /// consumed via [`IoRuntime::pop`].
    ///
    /// To get the best out of IoUring, batch together as many submissions as
    /// possible before issuing a submit and wait (which is a syscall).
    pub fn submit_and_wait(&self, want: usize) -> io::Result<usize> {
        self.io_ring.submit_and_wait(want)
    }

    /// Progress I/O runtime clock by 1 tick.
    fn io_clock_tick(&mut self) -> u64 {
        self.io_clock += 1;
        self.io_clock
    }
}

/// Type of I/O action against a file (as of now).
#[derive(Debug)]
pub enum IoAction {
    /// Flush contents of a file to disk.
    Fsync { file: IoFile },

    /// Resize a file to new length.
    Resize { file: IoFile, len: u64 },

    /// Action to read from file starting at a specific offset.
    Read { file: IoFile, offset: u64, buf: IoBuf },

    /// Action to write into file starting at a specific offset.
    Write { file: IoFile, offset: u64, buf: IoBuf },
}

impl IoAction {
    /// Sync file data and metadata to disk.
    ///
    /// This flushes any intermediate buffers between the application and
    /// disk. If this call completes successfully, any changes made to file
    /// are guaranteed to be stored durably.
    ///
    /// # Arguments
    ///
    /// * `file` - File to read from.
    pub fn fsync<F: Into<IoFile>>(file: F) -> Self {
        Self::Fsync { file: file.into() }
    }

    /// Resize file to a new length.
    ///
    /// If the current size of file is less than length, extra null bytes
    /// are padded to the file to get it to new length. If the current size
    /// is greater, excess bytes are discarded.
    ///
    /// # Arguments
    ///
    /// * `file` - File to read from.
    /// * `len` - New length of the file.
    pub fn resize<F: Into<IoFile>>(file: F, len: u64) -> Self {
        Self::Resize { file: file.into(), len }
    }

    /// Read from file at a specific offset.
    ///
    /// # Arguments
    ///
    /// * `file` - File to read from.
    /// * `offset` - Offset on file to begin reads.
    /// * `buf` - Buffer of bytes to copy bytes into.
    pub fn read_at<F: Into<IoFile>>(file: F, offset: u64, buf: IoBuf) -> Self {
        Self::Read {
            buf,
            offset,
            file: file.into(),
        }
    }

    /// Write to file at a specific offset.
    ///
    /// # Arguments
    ///
    /// * `file` - File to write into.
    /// * `offset` - Offset on file to begin writes.
    /// * `buf` - Buffer of bytes to copy bytes from.
    pub fn write_at<F: Into<IoFile>>(file: F, offset: u64, buf: IoBuf) -> Self {
        Self::Write {
            buf,
            offset,
            file: file.into(),
        }
    }

    /// Take ownership of the buffer held in the action, if any.
    pub fn take_buf(self) -> Option<IoBuf> {
        match self {
            Self::Read { buf, .. } => Some(buf),
            Self::Write { buf, .. } => Some(buf),
            _ => None,
        }
    }

    /// Generate the submission queue entry for the I/O action.
    ///
    /// # Safety
    ///
    /// A mutable alias is potentially shared with the kernel. It should remain valid
    /// until the I/O request is cancelled or completed.
    fn io_sqe(&mut self) -> squeue::Entry {
        match self {
            Self::Fsync { file } => match file {
                IoFile::Fd(fd) => opcode::Fsync::new(types::Fd(fd.0)).build(),
                IoFile::Fixed(fd) => opcode::Fsync::new(types::Fixed(fd.0)).build(),
            },

            Self::Resize { file, len } => match file {
                IoFile::Fd(fd) => opcode::Ftruncate::new(types::Fd(fd.0), *len).build(),
                IoFile::Fixed(fd) => opcode::Ftruncate::new(types::Fixed(fd.0), *len).build(),
            },

            Self::Read { file, offset, buf } => {
                let ptr = buf.as_mut_ptr();
                let len = buf.len() as u32;
                match (file, buf.io_index()) {
                    (IoFile::Fd(fd), None) => opcode::Read::new(types::Fd(fd.0), ptr, len).offset(*offset).build(),

                    (IoFile::Fixed(fd), None) => {
                        opcode::Read::new(types::Fixed(fd.0), ptr, len).offset(*offset).build()
                    }

                    (IoFile::Fd(fd), Some(index)) => opcode::ReadFixed::new(types::Fd(fd.0), ptr, len, index)
                        .offset(*offset)
                        .build(),

                    (IoFile::Fixed(fd), Some(index)) => opcode::ReadFixed::new(types::Fixed(fd.0), ptr, len, index)
                        .offset(*offset)
                        .build(),
                }
            }

            Self::Write { file, offset, buf } => {
                let ptr = buf.as_mut_ptr();
                let len = buf.len() as u32;
                match (file, buf.io_index()) {
                    (IoFile::Fd(fd), None) => opcode::Write::new(types::Fd(fd.0), ptr, len).offset(*offset).build(),

                    (IoFile::Fixed(fd), None) => {
                        opcode::Write::new(types::Fixed(fd.0), ptr, len).offset(*offset).build()
                    }

                    (IoFile::Fd(fd), Some(index)) => opcode::WriteFixed::new(types::Fd(fd.0), ptr, len, index)
                        .offset(*offset)
                        .build(),

                    (IoFile::Fixed(fd), Some(index)) => opcode::WriteFixed::new(types::Fixed(fd.0), ptr, len, index)
                        .offset(*offset)
                        .build(),
                }
            }
        }
    }
}
