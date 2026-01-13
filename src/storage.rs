//! Storage engine that backs Waldo.

mod action;
mod page;
mod queue;
mod ring;
mod worker;

use self::action::{Action, FateError};
use self::page::{Page, PageMetadata, PageOptions};
use self::worker::Worker;
use crate::Log;
use crate::runtime::{BufPool, IoBuf, PoolOptions};
use crate::storage::action::FateReceiver;
use std::ops::AddAssign;
use std::{path::Path, sync::Arc};
use tokio::sync::watch;

type ActionTx = flume::Sender<Action>;
type ActionRx = flume::Receiver<Action>;

type WatchTx = watch::Sender<Option<u64>>;
type WatchRx = watch::Receiver<Option<u64>>;

/// Different types of errors when opening or closing Waldo.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error when I/O syscalls results in error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Error when an unsupported option is provided.
    #[error("Option not supported: {0}")]
    Option(String),

    /// Error when Waldo storage has been closed.
    #[error("Underlying Waldo storage instance closed")]
    Closed,
}

impl From<watch::error::RecvError> for Error {
    fn from(_value: watch::error::RecvError) -> Self {
        Error::Closed
    }
}

/// Different types of errors when querying for logs from storage.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// Error when I/O syscalls results in error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Error when unexpected things happen in storage.
    #[error("Fault in storage: {0}")]
    Fault(&'static str),

    /// Error when Waldo storage has been closed.
    #[error("Underlying Waldo storage instance closed")]
    Closed,

    /// Error when requested logs are trimmed from storage.
    #[error("Requested range of log after: {0} are trimmed")]
    Trimmed(u64),

    /// Error when logs are appended out of sequence.
    #[error("Log record: {0} has prev: {1}, but expected prev: {2}")]
    Sequence(u64, u64, u64),
}

impl From<AsyncError> for QueryError {
    fn from(_value: AsyncError) -> Self {
        QueryError::Closed
    }
}

/// Different types of errors when appending logs to storage.
#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    /// Error when I/O syscalls results in error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Error when unexpected things happen in storage.
    #[error("Fault in storage: {0}")]
    Fault(&'static str),

    /// Error when Waldo storage has been closed.
    #[error("Underlying Waldo storage instance closed")]
    Closed,

    /// Error when conflicting appends are performed.
    #[error("A conflicting append is already in progress")]
    Conflict,

    /// Error when logs are appended out of sequence.
    #[error("Log record: {0} has prev: {1}, but expected prev: {2}")]
    Sequence(u64, u64, u64),

    /// Error when log record exceeds size limits.
    #[error("Log record: {0} has size: {1} that exceeds limit: {limit}", limit = Log::SIZE_LIMIT)]
    ExceedsLimit(u64, usize),
}

impl From<AsyncError> for AppendError {
    fn from(_value: AsyncError) -> Self {
        AppendError::Closed
    }
}

/// Error when an async action couldn't be issued.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
#[error("Async action couldn't be issued because closing")]
pub(crate) struct AsyncError;

impl From<FateError> for AsyncError {
    fn from(_value: FateError) -> Self {
        AsyncError
    }
}

impl<T> From<flume::SendError<T>> for AsyncError {
    fn from(_value: flume::SendError<T>) -> Self {
        AsyncError
    }
}

/// An action initiated against storage.
///
/// Note that this action is not yet executed. It either needs to be executed synchronously
/// with [`Self::run`], or [`Self::run_async`] to execute the action asynchronously.
pub(crate) struct AsyncAction<'a, T> {
    action: Action,
    tx: &'a ActionTx,
    rx: FateReceiver<T>,
}

impl<T> AsyncAction<'_, T> {
    /// Run the asynchronous action synchronously, i.e, blocking the caller.
    #[allow(dead_code)]
    pub(crate) fn run(self) -> Result<T, AsyncError> {
        self.tx.send(self.action)?;
        Ok(self.rx.recv()?)
    }

    /// Run the asynchronous action asynchronously, i.e, does not block runtime.
    pub(crate) async fn run_async(self) -> Result<T, AsyncError> {
        self.tx.send_async(self.action).await?;
        Ok(self.rx.recv_async().await?)
    }
}

/// Storage engine that backs Waldo.
///
/// It is just a thin wrapper around storage worker along with a set of channels
/// to communicate with the worker and coordination across threads, tasks and worker.
#[derive(Debug, Clone)]
pub(crate) struct Storage {
    rx: WatchRx,
    tx: ActionTx,
    buf_pool: BufPool,
    _worker: Arc<Worker>,
}

impl Storage {
    /// Open storage in the given home directory.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to home directory of the storage instance.
    /// * `opts` - Options to open storage.
    pub(crate) async fn open<P: AsRef<Path>>(path: P, opts: Options) -> Result<Self, Error> {
        // Otherwise we can get into a situation where a batch cannot be appended unless
        // split into two or more parts. This makes sure a single batch can also be appended
        // into an empty file.
        if opts.file_capacity < opts.buf_capacity as u64 {
            return Err(Error::Option(format!(
                "Buffer capacity {} > File capacity {}",
                opts.buf_capacity, opts.file_capacity
            )));
        }

        // Otherwise we can get into a situation where a valid log can never be written into buffer.
        if opts.buf_capacity < Log::SIZE_LIMIT {
            return Err(Error::Option(format!(
                "Buffer capacity {} < Log size limit {}",
                opts.buf_capacity,
                Log::SIZE_LIMIT
            )));
        }

        // To make sure a query does not need more than one lookup to fetch at least one log record.
        if opts.index_sparse_bytes > (Log::SIZE_LIMIT / 2) {
            return Err(Error::Option(format!(
                "Index is too sparse {} < (Log size limit / 2) {}",
                opts.index_sparse_bytes,
                Log::SIZE_LIMIT / 2
            )));
        }

        // Index is required because we never read more than once per batch.
        if opts.index_capacity == 0 || opts.index_sparse_bytes == 0 || opts.index_sparse_count == 0 {
            return Err(Error::Option(String::from("Page index cannot be empty")));
        }

        // Spawn the background worker and initialize storage.
        let (buf_pool, tx, watch_rx, worker) = Worker::spawn(path.as_ref(), opts).await?;
        Ok(Self {
            tx,
            buf_pool,
            rx: watch_rx,
            _worker: Arc::new(worker),
        })
    }

    /// Reference to buffer pool tracked by storage.
    pub(crate) fn buf_pool(&self) -> &BufPool {
        &self.buf_pool
    }

    /// Reference to the watcher tracking appends in storage.
    pub(crate) fn watcher(&self) -> &WatchRx {
        &self.rx
    }

    /// Mutable reference to the watcher tracking appends in storage.
    pub(crate) fn watcher_mut(&mut self) -> &mut WatchRx {
        &mut self.rx
    }

    /// Create an action to fetch storage metadata.
    pub(crate) fn metadata(&self) -> AsyncAction<'_, Option<Metadata>> {
        let (action, rx) = Action::metadata();
        AsyncAction {
            rx,
            action,
            tx: &self.tx,
        }
    }

    /// Create an action to append a buffer of logs into storage.
    ///
    /// # Arguments
    ///
    /// * `buf` - Source buffer with serialized log records.
    pub(crate) fn append(&self, buf: IoBuf) -> AsyncAction<'_, (IoBuf, Result<(), AppendError>)> {
        let (action, rx) = Action::append(buf);
        AsyncAction {
            rx,
            action,
            tx: &self.tx,
        }
    }

    /// Create an action to query log from storage.
    ///
    /// # Arguments
    ///
    /// * `after_seq_no` - Query for logs after this sequence number.
    /// * `buf` - Destination buffer to fill with log records.
    pub(crate) fn query(&self, after_seq_no: u64, buf: IoBuf) -> AsyncAction<'_, (IoBuf, Result<(), QueryError>)> {
        let (action, rx) = Action::query(after_seq_no, buf);
        AsyncAction {
            rx,
            action,
            tx: &self.tx,
        }
    }

    /// Initiate graceful shutdown of storage.
    pub(crate) async fn close(self) {
        // Drop rid of any shared reference.
        drop(self.tx);
        drop(self.rx);
        drop(self.buf_pool);

        // If we have the last reference to worker, we'll close the worker now.
        if let Ok(worker) = Arc::try_unwrap(self._worker) {
            worker.close().await;
        }
    }
}

/// Metadata of a Waldo instance.
///
/// Provides a rich summary of the current state of Waldo:
///
/// * Range of log records.
/// * Number of logs and index entries.
/// * Disk and memory footprint.
/// * Pending I/O operations.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Metadata {
    /// Storage contains logs after this sequence number.
    pub after_seq_no: u64,

    /// Storage contains logs before (inclusive) this sequence number.
    pub prev_seq_no: u64,

    /// Number of log records in storage.
    pub log_count: u64,

    /// Number of index entries in storage.
    pub index_count: u64,

    /// Total disk space occupied by storage.
    pub disk_size: u64,

    /// Total memory allocated for index.
    pub index_size: u64,

    /// Total number of page resets in progress.
    pub pending_resets: u64,

    /// Total number of page appends in progress.
    pub pending_appends: u64,

    /// Total number of page queries in progress.
    pub pending_queries: u64,

    /// Total number of page fsyncs in progress.
    pub pending_fsyncs: u64,
}

impl AddAssign<PageMetadata> for Metadata {
    fn add_assign(&mut self, rhs: PageMetadata) {
        self.log_count += rhs.count;
        self.index_count += rhs.index_count;
        self.disk_size += rhs.file_size;
        self.index_size += rhs.index_size;
        self.pending_resets += rhs.resets;
        self.pending_appends += rhs.appends;
        self.pending_queries += rhs.queries;
        self.pending_fsyncs += rhs.fsyncs;
        self.prev_seq_no = std::cmp::max(self.prev_seq_no, rhs.prev_seq_no);
        self.after_seq_no = std::cmp::min(self.after_seq_no, rhs.after_seq_no);
    }
}

/// Options to customize the behavior of Waldo.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Options {
    /// Maximum number of pages in storage.
    pub ring_size: u32,

    /// Maximum number of concurrent I/O submissions.
    ///
    /// Note that there can 2X this number of pending I/O submissions.
    pub queue_depth: u32,

    /// Maximum number of allocated I/O buffers.
    pub pool_size: u16,

    /// true to allocate I/O buffers with huge page support, false otherwise.
    pub huge_buf: bool,

    /// Size of allocated I/O buffers in the buffer pool.
    ///
    /// Note that if `huge_buf` is true, buffer capacity should be multiples
    /// of system default huge page size, which is usually 2MB.
    pub buf_capacity: usize,

    /// Maximum number of log records in a single page.
    pub page_capacity: u64,

    /// Maximum number of index entries in a page.
    pub index_capacity: usize,

    /// Maximum number of bytes between indexed entries.
    ///
    /// Note that this configuration is a soft limit, i.e, can be violated.
    /// This happens when size of a single log record exceeds this value.
    pub index_sparse_bytes: usize,

    /// Maximum number of logs between indexed entries.
    pub index_sparse_count: usize,

    /// true to enable `O_DSYNC` for log appends, false otherwise.
    ///
    /// If true, append operations only complete when log bytes and associated file
    /// metadata is sync'd to disk, essentially making sure logs are durably stored
    /// on disk.
    pub file_o_dsync: bool,

    /// Maximum size of the file backing a file.
    pub file_capacity: u64,
}

impl From<Options> for PoolOptions {
    fn from(value: Options) -> Self {
        Self {
            pool_size: value.pool_size,
            buf_capacity: value.buf_capacity,
            huge_buf: value.huge_buf,
        }
    }
}

impl From<Options> for PageOptions {
    fn from(value: Options) -> Self {
        Self {
            capacity: value.page_capacity,
            index_capacity: value.index_capacity,
            index_sparse_bytes: value.index_sparse_bytes,
            index_sparse_count: value.index_sparse_count,
            file_o_dsync: value.file_o_dsync,
            file_capacity: value.file_capacity,
        }
    }
}
