//! Storage engine to store sequential log records.

mod action;
mod page;
mod session;
mod worker;

pub use self::session::Session;

use crate::{
    runtime::{BufPool, IoBuf, IoRuntime, RawBytes},
    storage::{
        action::{Action, FateError, IoQueue},
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

type BufResult<T, E> = (IoBuf, Result<T, E>);

/// A ring buffer of sequential user supplied log records.
#[derive(Debug, Clone)]
pub struct Storage {
    buf_pool: BufPool,
    _worker: Arc<Worker>,
    tx: flume::Sender<Action>,
}

impl Storage {
    /// Create new storage instance in a directory.
    ///
    /// Note that this is a blocking I/O operation. To make sure your async runtime
    /// doesn't remain blocked, use something like `spawn_blocking`.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the directory on disk.
    /// * `after_seq_no` - Logs will be appended to storage after this sequence number.
    /// * `opts` - Options used to create storage.
    pub fn create<P: AsRef<Path>>(path: P, after_seq_no: u64, opts: Options) -> io::Result<Self> {
        // Allocate all memory for buffer pool.
        let pool_size = usize::from(opts.pool.pool_size);
        let mut buffers = (0..pool_size)
            .map(|_| RawBytes::allocate(opts.pool.buf_capacity, opts.pool.huge_buf))
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
            .map(|(index, file)| Page::new_empty(index as _, file, opts.page))
            .collect();

        // Initialize the first page with starting seq_no.
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

    /// Create a new session.
    ///
    /// Note that this operation blocks till memory is available in the pool.
    /// For a non-blocking variant use [`Storage::try_session`] or [`Storage::session_async`] for async.
    pub async fn session(&self) -> Session<'_> {
        let buf = self.buf_pool.take();
        Session::new(buf, &self.tx)
    }

    /// Create a new session if one can be created without blocking.
    ///
    /// Note that this operation does not block waiting for memory to be available from the pool.
    /// For a blocking variant use [`Storage::session`] or [`Storage::session_async`] for async.
    pub async fn try_session(&self) -> Option<Session<'_>> {
        let buf = self.buf_pool.try_take()?;
        Some(Session::new(buf, &self.tx))
    }

    /// Create a new session asynchronously.
    pub async fn session_async(&self) -> Session<'_> {
        let buf = self.buf_pool.take_async().await;
        Session::new(buf, &self.tx)
    }

    /// Open handle to file that backs a page at given index.
    fn open_page_file<P: AsRef<Path>>(path: P, index: u32) -> io::Result<File> {
        OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .read(true)
            .open(path.as_ref().join(format!("{index:0>10}.page")))
    }
}

/// Options to customize the behavior of storage.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Options {
    /// Maximum number of pages storage.
    pub ring_size: u32,

    /// Maximum concurrency allowed in storage.
    pub queue_depth: u16,

    /// Options for the buffer pool.
    pub pool: PoolOptions,

    /// Options for the sparse index.
    pub page: PageOptions,
}

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

/// Options to customize the behavior of a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageOptions {
    /// Maximum number of log records in a page.
    pub page_capacity: u64,

    /// Maximum size of file backing a page.
    pub file_capacity: u64,

    /// Maximum size of sparse index backing a page.
    pub index_capacity: usize,

    /// Maximum number of logs between indexed entries.
    pub index_sparse_count: usize,

    /// Maximum number of bytes between indexed entries.
    pub index_sparse_bytes: usize,
}
