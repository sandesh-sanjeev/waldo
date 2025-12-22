mod action;
mod fate;
mod page;
mod worker;

use self::page::Page;
use crate::{
    log::{Log, LogIter},
    runtime::{BufPool, IoBuf, IoRuntime, RawBytes},
    storage::{
        action::{Action, Append, Query},
        fate::{AsyncFate, FateError},
        page::PageIo,
        worker::{Worker, WorkerState},
    },
};
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io,
    path::Path,
    sync::Arc,
};

pub type BufResult<T, E> = (IoBuf, Result<T, E>);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Fate of an append operation was lost")]
    ActionLost,

    #[error("Action rejected because storage is closing")]
    UnexpectedClose,
}

impl From<FateError> for Error {
    fn from(_value: FateError) -> Self {
        Error::ActionLost
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Storage(#[from] Error),
}

#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    #[error(transparent)]
    Storage(#[from] Error),

    #[error("Log record: {0} has prev: {1}, but expected prev: {2}")]
    Sequence(u64, u64, u64),

    #[error("Log record: {0} has size: {1} that exceeds limit: {2}")]
    ExceedsLimit(u64, usize, usize),
}

#[derive(Debug, Clone)]
pub struct Storage {
    buf_pool: BufPool,
    _worker: Arc<Worker>,
    tx: flume::Sender<Action>,
}

impl Storage {
    pub fn open<P: AsRef<Path>>(path: P, after_seq_no: u64, opts: Options) -> io::Result<Self> {
        // Allocate all memory for buffer pool.
        let pool_size = usize::from(opts.pool_size);
        let mut buffers = (0..pool_size)
            .map(|_| RawBytes::allocate(opts.buf_capacity, opts.huge_buf))
            .collect::<io::Result<Vec<_>>>()?;

        // Open all the storage pages.
        let files = (0..opts.ring_size)
            .map(|index| Self::open_page_file(path.as_ref(), index))
            .collect::<io::Result<Vec<_>>>()?;

        // Create I/O runtime for storage.
        let mut runtime = IoRuntime::new(opts.queue_depth.into())?;

        // Register buffers and files with the runtime.
        let io_files = runtime.register_files(&files)?;

        // Create buffer pool from pre-allocated capacity.
        unsafe { runtime.register_bufs(&mut buffers)? };
        let buf_pool = BufPool::new(buffers);

        // Create all the pages in ring buffer.
        let mut pages: Vec<_> = io_files
            .into_iter()
            .enumerate()
            .map(|(index, file)| Page::new(index as _, file, opts.index_opts))
            .collect();

        // Initialize the first page with starting seq_no.
        // FIXME: Obviously we need to do this better.
        if let Some(page) = pages.first_mut() {
            page.initialize(after_seq_no);
        }

        // Create starting state for the background storage worker.
        let io_queue = VecDeque::with_capacity(pool_size);
        let (tx, rx) = flume::bounded(pool_size);
        let worker = WorkerState {
            rx,
            pages,
            runtime,
            io_queue,
        };

        // Return newly opened storage.
        Ok(Self {
            tx,
            buf_pool,
            _worker: Arc::new(worker.spawn()),
        })
    }

    pub async fn session(&self) -> Session<'_> {
        Session {
            action_tx: &self.tx,
            buf: Some(self.buf_pool.take_async().await),
        }
    }

    fn open_page_file<P: AsRef<Path>>(path: P, index: u32) -> io::Result<File> {
        OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .read(true)
            .open(path.as_ref().join(format!("{index:0>10}.page")))
    }
}

/// An open session to read and write from storage.
pub struct Session<'a> {
    buf: Option<IoBuf>,
    action_tx: &'a flume::Sender<Action>,
}

impl Session<'_> {
    pub async fn append(&mut self, logs: &[Log<'_>]) -> Result<(), AppendError> {
        // TODO: Perform log validations.

        // Write out all the logs to storage.
        let mut logs = logs;
        while !logs.is_empty() {
            // Fetch buffer to bytes to submit to storage.
            let mut buf = self.buf.take().ok_or(Error::UnexpectedClose)?;
            buf.clear(); // Clear any state from previous action.

            // Populate the buffer, with as many logs as possible.
            let mut consumed = 0;
            for log in logs {
                // We've consumed enough if buffer doesn't have space for the log.
                if !log.write(&mut buf) {
                    break;
                }

                // Track the last log written into the buffer.
                consumed += 1;
            }

            // Append buffer bytes to storage.
            // Submit action to append bytes into storage.
            let (tx, rx) = AsyncFate::channel();
            if let Err(error) = self.action_tx.send_async(Action::Append(Append { buf, tx })).await {
                // Reclaim shared memory.
                let Action::Append(append) = error.0 else {
                    unreachable!("Should not get different variant of the same enum");
                };

                // Replace the borrowed buffer and return.
                self.buf.replace(append.buf);
                return Err(Error::UnexpectedClose.into());
            }

            // Await and return result from storage.
            let (buf, result) = rx.recv_async().await.map_err(Error::from)?;
            self.buf.replace(buf);

            // Process results from append operation.
            result?;
            logs = logs.split_at(consumed).1;
        }

        // All the log records were successfully appended.
        Ok(())
    }

    pub async fn query(&mut self, after_seq_no: u64) -> Result<LogIter<'_>, QueryError> {
        // Fetch buffer to bytes to submit to storage.
        let mut buf = self.buf.take().ok_or(Error::UnexpectedClose)?;
        buf.clear(); // Clear any state from previous action.

        // Submit action to append bytes into storage.
        let (tx, rx) = AsyncFate::channel();
        if let Err(error) = self
            .action_tx
            .send_async(Action::Query(Query { buf, tx, after_seq_no }))
            .await
        {
            // Reclaim shared memory.
            let Action::Query(query) = error.0 else {
                unreachable!("Should not different variant of the same enum");
            };

            // Replace the borrowed buffer and return.
            self.buf.replace(query.buf);
            return Err(Error::UnexpectedClose.into());
        }

        // Await and return result from storage.
        let (buf, result) = rx.recv_async().await.map_err(Error::from)?;
        self.buf.replace(buf);
        result?; // First return error, if any, from storage.

        // Return reference to all the log records read from query.
        let buf = self.buf.as_ref().expect("Associated buffer must exist");
        Ok(LogIter::iter_after(Some(after_seq_no), buf))
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Options {
    /// Maximum number of pre-allocated buffers in the pool.
    pub pool_size: u16,

    /// Number of concurrent asynchronous actions that can happen at once.
    pub queue_depth: u16,

    /// Size of buffers used to perform I/O.
    pub buf_capacity: usize,

    /// Enable huge pages when allocating buffers.
    pub huge_buf: bool,

    /// Maximum number of pages in ring buffer.
    pub ring_size: u32,

    /// Options for the sparse index.
    pub index_opts: IndexOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexOptions {
    pub capacity: usize,
    pub sparse_count: usize,
    pub sparse_bytes: usize,
}
