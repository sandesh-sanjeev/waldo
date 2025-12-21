mod action;
mod fate;
mod page;
mod worker;

use self::page::Page;
use crate::{
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
};

pub type BufResult<T, E> = (IoBuf, Result<T, E>);

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Action rejected because storage is closing")]
    Closing,

    #[error("Fate of an append operation was lost, probably due to closing")]
    ActionLost(#[from] FateError),
}

#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Action rejected because storage is closing")]
    Closing,

    #[error("Fate of an append operation was lost, probably due to closing")]
    ActionLost(#[from] FateError),
}

#[derive(Debug, Clone)]
pub struct Storage {
    buf_pool: BufPool,
    tx: flume::Sender<Action>,
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
}

impl Storage {
    pub fn open<P: AsRef<Path>>(path: P, opts: Options) -> io::Result<(Self, Worker)> {
        // Allocate all memory for buffer pool.
        let buffer_size = opts.buf_capacity;
        let pool_size = usize::from(opts.pool_size);
        let mut buffers = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            buffers.push(RawBytes::allocate(buffer_size, opts.huge_buf)?);
        }

        // Open all the storage pages.
        let ring_size = usize::try_from(opts.ring_size).unwrap_or(usize::MAX);
        let mut files = Vec::with_capacity(ring_size);
        for index in 0..opts.ring_size {
            files.push(Self::open_page_file(path.as_ref(), index)?);
        }

        // Create I/O runtime for storage.
        let mut runtime = IoRuntime::new(opts.queue_depth.into())?;

        // Register buffers and files with the runtime.
        let io_files = runtime.register_files(&files)?;

        // Create buffer pool from pre-allocated capacity.
        unsafe { runtime.register_bufs(&mut buffers)? };
        let buf_pool = BufPool::new(buffers);

        // Create all the pages in ring buffer.
        let pages = io_files
            .into_iter()
            .enumerate()
            .map(|(index, file)| Page::new(index as _, file))
            .collect();

        // Create starting state for the background storage worker.
        let io_queue = VecDeque::with_capacity(pool_size);
        let (tx, rx) = flume::bounded(pool_size);
        let worker = Worker::spawn(WorkerState {
            rx,
            pages,
            files,
            runtime,
            io_queue,
        });

        // Return newly opened storage.
        Ok((Self { tx, buf_pool }, worker))
    }

    pub async fn session(&self) -> Session<'_> {
        Session {
            action_tx: &self.tx,
            buf: Some(self.buf_pool.take_async().await),
        }
    }

    fn open_page_file<P: AsRef<Path>>(path: P, index: u32) -> io::Result<File> {
        OpenOptions::new()
            .truncate(true) // FIXME: Remove soon.
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
    pub async fn append(&mut self) -> Result<usize, AppendError> {
        // Fetch buffer to bytes to submit to storage.
        let Some(mut buf) = self.buf.take() else {
            return Err(AppendError::Closing);
        };

        // FIXME: Replace with actual log bytes.
        buf.set_len(buf.capacity());

        // Submit action to append bytes into storage.
        let (tx, rx) = AsyncFate::channel();
        if let Err(error) = self.action_tx.send_async(Action::Append(Append { buf, tx })).await {
            let Action::Append(append) = error.0 else {
                unreachable!("Should not get different variant of the same enum");
            };

            // Replace the borrowed buffer and return.
            self.buf.replace(append.buf);
            return Err(AppendError::Closing);
        }

        // Await and return result from storage.
        let (buf, result) = rx.recv_async().await?;
        let len = buf.len();
        self.buf.replace(buf);

        // Return the number of bytes appended into storage.
        result.map(|_| len)
    }

    pub async fn query(&mut self, after: u64) -> Result<usize, QueryError> {
        // Fetch buffer to bytes to submit to storage.
        let Some(mut buf) = self.buf.take() else {
            // Only way for this to happen is if all the senders were dropped
            // before sending result of action. We can only assume storage is closing.
            return Err(QueryError::Closing);
        };

        // Cause storage writes available bytes into the buffer.
        buf.clear();

        // Submit action to append bytes into storage.
        let (tx, rx) = AsyncFate::channel();
        if let Err(error) = self.action_tx.send_async(Action::Query(Query { buf, tx, after })).await {
            let Action::Query(query) = error.0 else {
                unreachable!("Should not different variant of the same enum");
            };

            // Replace the borrowed buffer and return.
            self.buf.replace(query.buf);
            return Err(QueryError::Closing);
        }

        // Await and return result from storage.
        let (buf, result) = rx.recv_async().await?;
        let len = buf.len();
        self.buf.replace(buf);

        // Return the number of bytes queried from storage.
        result.map(|_| len)
    }
}
