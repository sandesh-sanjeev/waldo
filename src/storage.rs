mod action;
mod page;
mod worker;

use crate::{
    log::Log,
    runtime::{BufPool, IoBuf, IoRuntime, RawBytes},
    storage::{
        action::{Action, Append, AsyncFate, FateError, IoQueue, Query},
        page::Page,
        worker::{Worker, WorkerState},
    },
};
use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
    sync::Arc,
};

pub type BufResult<T, E> = (IoBuf, Result<T, E>);

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Storage(#[from] Error),

    #[error("Attempted to read {0} bytes, but only {1} was read")]
    IncompleteRead(usize, u32),
}

#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    #[error(transparent)]
    Storage(#[from] Error),

    #[error("Log record: {0} has prev: {1}, but expected prev: {2}")]
    Sequence(u64, u64, u64),

    #[error("Log record: {0} has size: {1} that exceeds limit: {2}")]
    ExceedsLimit(u64, usize, usize),

    #[error("Attempted to write {0} bytes, but only {1} was written")]
    IncompleteWrite(usize, u32),
}

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
            .map(|(index, file)| Page::new_empty(index as _, file, opts.page_options))
            .collect();

        // Initialize the first page with starting seq_no.
        // FIXME: Obviously we need to do this better.
        if let Some(page) = pages.first_mut() {
            page.initialize(after_seq_no);
        }

        // Create starting state for the background storage worker.
        let io_queue = IoQueue::with_capacity(pool_size);
        let (tx, rx) = flume::bounded(pool_size);
        let worker = WorkerState {
            rx,
            pages,
            runtime,
            io_queue,
            next: 0,
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
            self.action_tx
                .send_async(Action::Append(Append { buf, tx }))
                .await
                .map_err(|_| Error::UnexpectedClose)?;

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

    pub async fn query(&mut self, after_seq_no: u64) -> Result<impl Iterator<Item = Log<'_>>, QueryError> {
        // Fetch buffer to bytes to submit to storage.
        let mut buf = self.buf.take().ok_or(Error::UnexpectedClose)?;
        buf.clear(); // Clear any state from previous action.

        // Submit action to append bytes into storage.
        let (tx, rx) = AsyncFate::channel();
        self.action_tx
            .send_async(Action::Query(Query { buf, tx, after_seq_no }))
            .await
            .map_err(|_| Error::UnexpectedClose)?;

        // Await and return result from storage.
        let (buf, result) = rx.recv_async().await.map_err(Error::from)?;
        self.buf.replace(buf);
        result?; // First return error, if any, from storage.

        // Return reference to all the log records read from query.
        // A filter is necessary here because storage can read slightly more logs are
        // the beginning. It's more efficient to filter it out here
        let buf = self.buf.as_ref().expect("Associated buffer must exist");
        Ok(buf.into_iter().filter(move |log| log.seq_no() > after_seq_no))
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
    pub page_options: PageOptions,
}

/// Options to customize the behavior of a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageOptions {
    pub page_capacity: u64,
    pub file_capacity: u64,
    pub index_capacity: usize,
    pub index_sparse_count: usize,
    pub index_sparse_bytes: usize,
}
