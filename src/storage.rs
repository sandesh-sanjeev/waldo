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

/// Type alias for results that involve I/O buffers.
type BufResult<T, E> = (IoBuf, Result<T, E>);

/// A ring buffer of sequential user supplied log records.
#[derive(Debug, Clone)]
pub struct Storage {
    buf_pool: BufPool,
    _worker: Arc<Worker>,
    tx: flume::Sender<Action>,
}

impl Storage {
    /// Open an instance of storage at the given path.
    ///
    /// Note that this is a blocking I/O operation. To make sure your async runtime
    /// doesn't remain blocked, use something like `spawn_blocking`.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the directory on disk.
    /// * `opts` - Options used to create storage.
    pub fn open<P: AsRef<Path>>(path: P, opts: Options) -> io::Result<Self> {
        use rayon::prelude::*;

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

        // Open all pages backing storage.
        // In parallel to make best use of CPU and disk.
        let mut pages: Vec<_> = io_files
            .into_par_iter()
            .zip(files.into_par_iter())
            .enumerate()
            .map(|(index, (file, seed))| Page::open(index as _, file, seed, opts.page))
            .collect::<io::Result<_>>()?;

        // Find latest page in storage.
        let mut max = 0;
        let mut next = 0;
        for (i, page) in pages.iter().enumerate() {
            if let Some(state) = page.state()
                && state.prev_seq_no > max
            {
                next = i;
                max = state.prev_seq_no;
            }
        }

        // Figure out all the pages that can be preserved.
        let latest = &mut pages[next];
        if let Some(state) = latest.state() {
            let mut prev = next;
            let mut is_reset = false;
            let mut after_seq_no = state.after_seq_no;
            loop {
                // Next of the next page to check.
                let index = if prev == 0 { pages.len() - 1 } else { prev - 1 };
                if index == next {
                    break;
                }

                // Previous page in the ring buffer.
                let page = &mut pages[index];

                // An unconditional reset if a previous page was reset.
                if is_reset {
                    page.clear()?;
                } else if let Some(state) = page.state() {
                    // Make sure sequences match up with the earlier page.
                    if state.prev_seq_no != after_seq_no {
                        is_reset = true;
                        page.clear()?;
                    }

                    // Nice, everything checks out.
                    // We can use this page as is in storage.
                    after_seq_no = state.after_seq_no;
                } else {
                    // If this page is uninitialized, none of the pages
                    // before (in ring buffer) should be initialized either.
                    is_reset = true;
                    page.clear()?;
                }

                // For next iteration.
                prev = index;
            }
        } else {
            // In theory this is not necessary, for completeness.
            for page in pages.iter_mut() {
                if page.is_initialized() {
                    page.clear()?;
                }
            }
        }

        // Create starting state for the background storage worker.
        let io_queue = IoQueue::with_capacity(pool_size);
        let (tx, rx) = flume::bounded(pool_size);
        let worker = WorkerState {
            rx,
            pages,
            runtime,
            io_queue,
            next,
        };

        // Return newly opened storage.
        Ok(Self {
            tx,
            buf_pool,
            _worker: Arc::new(worker.spawn()),
        })
    }

    /// Read the latest state of storage.
    pub async fn state(&self) -> Option<StorageState> {
        let (action, rx) = Action::state();
        self.tx.send_async(action).await.ok()?;
        rx.recv_async().await.ok()?
    }

    /// Create a new session asynchronously.
    pub async fn session(&self) -> Session<'_> {
        let buf = self.buf_pool.take_async().await;
        Session::new(buf, &self.tx)
    }

    /// Open handle to file that backs a page at given index.
    fn open_page_file<P: AsRef<Path>>(path: P, index: u32) -> io::Result<File> {
        OpenOptions::new()
            .truncate(false)
            .create(true)
            .write(true)
            .read(true)
            .open(path.as_ref().join(format!("{index:0>10}.page")))
    }
}

/// Current state of storage.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct StorageState {
    pub after_seq_no: u64,
    pub prev_seq_no: u64,
    pub disk_size: u64,
    pub log_count: u64,
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
