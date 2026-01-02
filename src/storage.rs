//! Storage engine to store sequential log records.

mod action;
mod page;
mod session;
mod worker;

pub use self::page::{FileOpts, IndexOpts, PageOptions};
pub use self::session::Session;

use crate::runtime::{BufPool, IoBuf, IoRuntime, RawBytes};
use crate::storage::action::{Action, FateError, IoQueue};
use crate::storage::page::{Page, PageMetadata};
use crate::storage::worker::{Worker, WorkerState};
use std::ops::AddAssign;
use std::{io, path::Path, sync::Arc};

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
    pub async fn open<P: AsRef<Path>>(path: P, opts: Options) -> io::Result<Self> {
        // First open all the pages of storage.
        let (next, mut pages) = Self::open_pages(path.as_ref(), opts)?;

        // Create I/O runtime for storage.
        let mut runtime = IoRuntime::new(opts.queue_depth.into())?;

        // Allocate all memory for buffer pool.
        let pool_size = usize::from(opts.pool.pool_size);
        let mut buffers = (0..pool_size)
            .map(|_| RawBytes::allocate(opts.pool.buf_capacity, opts.pool.huge_buf))
            .collect::<io::Result<Vec<_>>>()?;

        // Register buffers and files with the runtime.
        let files: Vec<_> = pages.iter().map(Page::raw_fd).collect();
        let files = runtime.register_files(&files)?;
        for (file, page) in files.into_iter().zip(pages.iter_mut()) {
            page.set_io_file(file);
        }

        // Register allocated memory with io uring runtime and create buffer pool.
        unsafe { runtime.register_bufs(&mut buffers)? };
        let buf_pool = BufPool::new(buffers);

        // Allocate capacity for all the internal buffers.
        let queue_depth = usize::from(opts.queue_depth);
        let io_queue = IoQueue::with_capacity(queue_depth);
        let (tx, rx) = flume::bounded(queue_depth);

        // Initialize the worker.
        let state = WorkerState {
            next,
            pages,
            io_queue,
            rx,
            runtime,
        };

        Ok(Self {
            tx,
            buf_pool,
            _worker: Arc::new(state.spawn()),
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

    fn open_pages(path: &Path, opts: Options) -> io::Result<(usize, Vec<Page>)> {
        use rayon::prelude::*;

        // Initialize all the pages in parallel.
        let mut pages: Vec<_> = (0..opts.ring_size)
            .into_par_iter()
            .map(|id| {
                let file_name = format!("{id:0>10}.page");
                let file_path = path.join(file_name);
                Page::open(id, file_path, opts.page)
            })
            .collect::<io::Result<_>>()?;

        // Find latest page in storage.
        // That's the page with largest sequence number.
        let mut max = 0;
        let mut next = 0;
        for (i, page) in pages.iter().enumerate() {
            if let Some(state) = page.metadata()
                && state.prev_seq_no > max
            {
                next = i;
                max = state.prev_seq_no;
            }
        }

        // Figure out all the pages that can be preserved.
        let latest = &mut pages[next];
        if let Some(state) = latest.metadata() {
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
                } else if let Some(state) = page.metadata() {
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
                page.clear()?;
            }
        }

        // Return parsed pages along with starting index.
        Ok((next, pages))
    }
}

/// Current state of storage.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct StorageState {
    pub after_seq_no: u64,
    pub prev_seq_no: u64,
    pub log_count: u64,
    pub index_count: u64,
    pub disk_size: u64,
    pub index_size: u64,
}

impl AddAssign<PageMetadata> for StorageState {
    fn add_assign(&mut self, rhs: PageMetadata) {
        self.log_count += rhs.count;
        self.index_count += rhs.index_count;
        self.disk_size += rhs.file_size;
        self.index_size += rhs.index_size;
        self.prev_seq_no = std::cmp::max(self.prev_seq_no, rhs.prev_seq_no);
        self.after_seq_no = std::cmp::min(self.after_seq_no, rhs.after_seq_no);
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
