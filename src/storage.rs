mod action;
mod page;
mod session;
mod worker;

pub use self::session::Session;

use crate::{
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

type BufResult<T, E> = (IoBuf, Result<T, E>);

#[derive(Debug, Clone)]
pub struct Storage {
    buf_pool: BufPool,
    _worker: Arc<Worker>,
    tx: flume::Sender<Action>,
}

impl Storage {
    pub fn create<P: AsRef<Path>>(path: P, after_seq_no: u64, opts: Options) -> io::Result<Self> {
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
        let buf = self.buf_pool.take();
        Session::new(buf, &self.tx)
    }

    pub async fn try_session(&self) -> Option<Session<'_>> {
        let buf = self.buf_pool.try_take()?;
        Some(Session::new(buf, &self.tx))
    }

    pub async fn session_async(&self) -> Session<'_> {
        let buf = self.buf_pool.take_async().await;
        Session::new(buf, &self.tx)
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
