//! Storage engine to store sequential log records.

mod action;
mod page;
mod queue;
mod ring;
mod worker;

pub use self::page::{FileOpts, IndexOpts, PageOptions};

use crate::log::{Log, SequencedLogIter};
use crate::runtime::{BufPool, IoBuf, PoolOptions};
use crate::storage::action::{Action, FateError};
use crate::storage::page::{Page, PageMetadata};
use crate::storage::worker::Worker;
use std::ops::AddAssign;
use std::{io, path::Path, sync::Arc};

/// Different types of errors when querying for logs from storage.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// Error when I/O syscalls results in error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Error when unexpected things happen in storage.
    #[error("Fault in storage: {0}")]
    Fault(&'static str),

    /// Error when requested logs are trimmed from storage.
    #[error("Requested range of log after: {0} are trimmed")]
    Trimmed(u64),

    /// Error when logs are appended out of sequence.
    #[error("Log record: {0} has prev: {1}, but expected prev: {2}")]
    Sequence(u64, u64, u64),
}

impl From<FateError> for QueryError {
    fn from(_value: FateError) -> Self {
        QueryError::Fault("Fate sender drooped, worker unexpectedly closed")
    }
}

impl<T> From<flume::SendError<T>> for QueryError {
    fn from(_value: flume::SendError<T>) -> Self {
        QueryError::Fault("Action receiver drooped, worker unexpectedly closed")
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

impl From<FateError> for AppendError {
    fn from(_value: FateError) -> Self {
        AppendError::Fault("Fate sender drooped, worker unexpectedly closed")
    }
}

impl<T> From<flume::SendError<T>> for AppendError {
    fn from(_value: flume::SendError<T>) -> Self {
        AppendError::Fault("Action receiver drooped, worker unexpectedly closed")
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

/// Storage engine that holds sequential log records.
#[derive(Debug, Clone)]
pub struct Storage {
    buf_pool: BufPool,
    tx: flume::Sender<Action>,
    _worker: Arc<Worker>,
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
        let (buf_pool, tx, worker) = Worker::spawn(path.as_ref(), opts).await?;
        Ok(Self {
            tx,
            buf_pool,
            _worker: Arc::new(worker),
        })
    }

    /// Latest metadata for storage, if storage is initialized.
    pub async fn metadata(&self) -> Option<Metadata> {
        let (action, rx) = Action::metadata();
        self.tx.send_async(action).await.ok()?;
        rx.recv_async().await.ok()?
    }

    /// A new session to perform operations against storage.
    pub async fn session(&self) -> Session<'_> {
        let buf = self.buf_pool.take_async().await;
        Session::new(buf, &self.tx)
    }
}

/// A long lived connection to storage.
///
/// A session allows one to append and query log records from storage.
///
/// Note that a session borrows limited resources, you limit creation of new
/// sessions by holding on to a session. So depending on how to you plan to use
/// the session, you might want to release it (by dropping) when not being used.
pub struct Session<'a> {
    buf: Option<IoBuf>,
    action_tx: &'a flume::Sender<Action>,
}

impl Session<'_> {
    /// Create a new session.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer to back this session.
    /// * `action_tx` - Sender to send actions to storage.
    fn new(buf: IoBuf, action_tx: &flume::Sender<Action>) -> Session<'_> {
        Session {
            action_tx,
            buf: Some(buf),
        }
    }

    /// Append logs to storage.
    ///
    /// # Atomicity
    ///
    /// Atomicity is only guaranteed for a single log record. Now (and probably never)
    /// will we provide atomicity guarantees for an entire batch. While this is doable,
    /// it's more code and more importantly, I have no need for it.
    ///
    /// # Durability
    ///
    /// Durability guarantees depend on the options used open storage. Specifically, if
    /// you want guarantee that bytes are flushed to disk when this method successfully
    /// completes, enable `o_dsync` option in `FileOptions`.
    ///
    /// # Isolation
    ///
    /// This operation is executed with sequential consistency, i.e, all appends, even
    /// across threads, always operate against latest state of storage. If storage processes
    /// two concurrent appends, one of them will be rejected with conflict.
    ///
    /// # Arguments
    ///
    /// * `logs` - Log records to append into storage.
    pub async fn append(&mut self, mut logs: &[Log<'_>]) -> Result<(), AppendError> {
        // Return early if there is nothing to append.
        if logs.is_empty() {
            return Ok(());
        }

        // Process the list of provided log records.
        Self::assert_logs(logs)?;
        while !logs.is_empty() {
            // Take ownership of the buffer to share with storage.
            let mut buf = self
                .take_buf()
                .ok_or(AppendError::Fault("Buffer does not exist in a session"))?;

            // Populate buffer with serialized log bytes.
            // Keep track of logs that couldn't be fit into buffer.
            // These logs have to appended into storage in the next batch.
            logs = Self::populate_buf(&mut buf, logs);

            // Wait for a slot in the queue and submit append action.
            let (append, rx) = Action::append(buf);
            self.action_tx.send_async(append).await?;

            // Await and return result from storage.
            let (buf, result) = rx.recv_async().await?;
            self.buf.replace(buf);
            result?;
        }

        Ok(())
    }

    /// Query log records from storage.
    ///
    /// If requested logs exist in storage, they are returned as a contiguous
    /// chunk of sequential log records in ascending order of sequence numbers.
    ///
    /// You cannot specify limits, this method will query for as many log records
    /// as efficiently possible. You can discard the log you don't want, it's free.
    /// Or query for more if you need more logs.
    ///
    /// # Isolation
    ///
    /// Only logs that have been successfully committed against storage are visible
    /// to queries against storage. However note that they might not be durably committed.
    ///
    /// # Arguments
    ///
    /// * `after_seq_no` - Query for logs after this sequence number.
    pub async fn query(&mut self, after_seq_no: u64) -> Result<SequencedLogIter<'_>, QueryError> {
        // Take ownership of the buffer to share with storage.
        let buf = self
            .take_buf()
            .ok_or(QueryError::Fault("Buffer does not exist in a session"))?;

        // Submit action to append bytes into storage.
        let (query, rx) = Action::query(after_seq_no, buf);
        self.action_tx.send_async(query).await?;

        // Await and return result from storage.
        let (buf, result) = rx.recv_async().await?;
        self.buf.replace(buf);
        result?;

        // Return reference to all the log records read from query.
        let buf = unsafe { self.buf.as_ref().unwrap_unchecked() };

        // Figure out offset to the starting seq_no.
        // For performance reasons storage might return a few extra logs
        // at the beginning of the buffer. We have to skip past those.
        let mut offset = 0;
        for log in buf {
            // We've reached the target offset in the underlying buffer.
            if log.seq_no() > after_seq_no {
                break;
            }

            offset += log.size();
        }

        // Range of log bytes to return.
        let bytes = unsafe { buf.split_at_unchecked(offset).1 };

        // Return iterator for the rest of the log records.
        Ok(SequencedLogIter::new(bytes, after_seq_no))
    }

    fn take_buf(&mut self) -> Option<IoBuf> {
        let mut buf = self.buf.take()?;
        buf.clear(); // Clear prior state.
        Some(buf)
    }

    fn assert_logs(logs: &[Log<'_>]) -> Result<(), AppendError> {
        let mut prev_seq_no = None;
        for log in logs {
            // Sequence validation.
            if let Some(prev) = prev_seq_no
                && prev != log.prev_seq_no()
            {
                return Err(AppendError::Sequence(log.seq_no(), log.prev_seq_no(), prev));
            }

            // Limit checks.
            if log.size() >= Log::SIZE_LIMIT {
                return Err(AppendError::ExceedsLimit(log.seq_no(), log.size()));
            }

            prev_seq_no = Some(log.seq_no());
        }

        Ok(())
    }

    fn populate_buf<'a>(buf: &mut IoBuf, logs: &'a [Log<'a>]) -> &'a [Log<'a>] {
        let mut consumed = 0;
        for log in logs {
            if !log.write(buf) {
                break; // Buffer overflow.
            }

            consumed += 1;
        }

        // Return unconsumed log records.
        unsafe { logs.split_at_unchecked(consumed).1 }
    }
}

/// Metadata of a storage instance.
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
}

impl AddAssign<PageMetadata> for Metadata {
    fn add_assign(&mut self, rhs: PageMetadata) {
        self.log_count += rhs.count;
        self.index_count += rhs.index_count;
        self.disk_size += rhs.file_size;
        self.index_size += rhs.index_size;
        self.prev_seq_no = std::cmp::max(self.prev_seq_no, rhs.prev_seq_no);
        self.after_seq_no = std::cmp::min(self.after_seq_no, rhs.after_seq_no);
    }
}
