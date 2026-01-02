//! Storage engine to store sequential log records.

mod action;
mod page;
mod session;
mod worker;

pub use self::page::{FileOpts, IndexOpts, PageOptions};
pub use self::session::Session;

use crate::{
    runtime::{BufPool, IoBuf},
    storage::{
        action::{Action, FateError},
        page::Page,
        worker::Worker,
    },
};
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
        Worker::spawn(path, opts).await
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
