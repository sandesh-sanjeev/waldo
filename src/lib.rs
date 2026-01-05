//! # Waldo
//!
//! Waldo is an embedded lock-free, on-disk, ring buffer of sequential records. README on the
//! [repository](https://github.com/sandesh-sanjeev/waldo#) provides an overview of the high
//! level design and capabilities.

mod action;
mod log;
mod page;
mod queue;
mod ring;
mod worker;

// I/O runtime is exposed only for benchmarks.
#[cfg(feature = "benchmark")]
pub mod runtime;

#[cfg(not(feature = "benchmark"))]
mod runtime;

// Publicly visible types.
pub use crate::page::{FileOpts, IndexOpts, PageOptions};
pub use crate::runtime::PoolOptions;
pub use log::{Error as LogError, Log, LogIter};
use tokio::sync::watch;

use crate::action::{Action, FateError};
use crate::page::{Page, PageMetadata};
use crate::runtime::{BufPool, IoBuf};
use crate::worker::Worker;
use std::cell::Cell;
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
pub struct Waldo {
    buf_pool: BufPool,
    tx: flume::Sender<Action>,
    watch_rx: watch::Receiver<Option<u64>>,
    _worker: Arc<Worker>,
}

impl Waldo {
    /// Open an instance of storage at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the directory on disk.
    /// * `opts` - Options used to create storage.
    pub async fn open<P: AsRef<Path>>(path: P, opts: Options) -> io::Result<Self> {
        let (buf_pool, tx, watch_rx, worker) = Worker::spawn(path.as_ref(), opts).await?;
        Ok(Self {
            tx,
            buf_pool,
            watch_rx,
            _worker: Arc::new(worker),
        })
    }

    /// Latest metadata for storage, if storage is initialized.
    pub async fn metadata(&self) -> Option<Metadata> {
        let (action, rx) = Action::metadata();
        self.tx.send_async(action).await.ok()?;
        rx.recv_async().await.ok()?
    }

    /// Create a new sink.
    ///
    /// Returns none if one couldn't be created because a free buffer wasn't available.
    pub fn try_sink(&self) -> Option<Sink> {
        Some(Sink {
            tx: self.tx.clone(),
            buf: Some(self.buf_pool.try_take()?),
            prev: *self.watch_rx.borrow(),
        })
    }

    /// Create a new stream
    ///
    /// # Arguments
    ///
    /// * `after_seq_no` - Log records after this sequence number will be delivered in stream.
    pub fn stream(&self, after_seq_no: u64) -> Stream {
        Stream {
            pool: self.buf_pool.clone(),
            prev: Cell::new(after_seq_no),
            tx: self.tx.clone(),
            rx: self.watch_rx.clone(),
        }
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

/// A stream subscription to discover new log records.
///
/// If requested logs exist in storage, they are returned as a contiguous
/// chunk of sequential log records in ascending order of sequence numbers.
///
/// You cannot specify limits, this method will query for as many log records
/// as efficiently possible. You can discard logs you don't want, it's free.
///
/// # Isolation
///
/// Only logs that have been successfully committed against storage are visible
/// to queries against storage. However note that they might not be durably committed.
#[derive(Debug)]
pub struct Stream {
    pool: BufPool,
    prev: Cell<u64>,
    tx: flume::Sender<Action>,
    rx: watch::Receiver<Option<u64>>,
}

impl Stream {
    /// Sequence number of the previous log record delivered to subscriber.
    pub fn prev_seq_no(&self) -> u64 {
        self.prev.get()
    }

    /// Fetch next set of log records from storage.
    pub async fn next(&mut self) -> Result<StreamLogs<'_>, QueryError> {
        loop {
            // See if there are newer log records available.
            if let Some(storage_prev) = *self.rx.borrow_and_update()
                && storage_prev > self.prev.get()
            {
                break;
            };

            // Otherwise wait for new logs to get appended.
            if self.rx.changed().await.is_err() {
                return Err(QueryError::Fault("Storage is closed"));
            }
        }

        // Prepare request to make to storage.
        let after_seq_no = self.prev.get();
        let buf = self.pool.take_async().await;

        // Submit action to append bytes into storage.
        let (query, rx) = Action::query(after_seq_no, buf);
        self.tx.send_async(query).await?;

        // Await and return result from storage.
        let (buf, result) = rx.recv_async().await?;
        result?;

        // Figure out offset to the starting seq_no.
        // For performance reasons storage might return a few extra logs
        // at the beginning of the buffer. We have to skip past those.
        let offset = buf
            .into_iter()
            .take_while(|log| log.seq_no() <= after_seq_no)
            .map(|log| log.size())
            .sum();

        Ok(StreamLogs {
            buf,
            offset,
            prev: &self.prev,
        })
    }
}

/// A chunk of contiguous log records discovered in stream.
#[derive(Debug)]
pub struct StreamLogs<'a> {
    buf: IoBuf,
    offset: usize,
    prev: &'a Cell<u64>,
}

impl<'a> IntoIterator for &'a StreamLogs<'a> {
    type IntoIter = StreamLogIter<'a>;
    type Item = Log<'a>;

    fn into_iter(self) -> Self::IntoIter {
        StreamLogIter {
            prev: self.prev,
            bytes: unsafe { self.buf.get_unchecked(self.offset..) },
        }
    }
}

/// An iterator to iterate through stream log records.
#[derive(Debug)]
pub struct StreamLogIter<'a> {
    bytes: &'a [u8],
    prev: &'a Cell<u64>,
}

impl<'a> Iterator for StreamLogIter<'a> {
    type Item = Log<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // Attempt to deserialize the next set of bytes into log.
        let log = Log::read(self.bytes)?;

        // Safety: We just read enough bytes for the parsed log record.
        // Compiler/LLVM is not smart enough to know this and remove bounds check.
        self.bytes = unsafe { self.bytes.get_unchecked(log.size()..) };

        // Update sequence number in the stream and return.
        self.prev.set(log.seq_no());
        Some(log)
    }
}

/// A sink is buffered log writer.
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
/// # Cancel safety
///
/// If this method is cancelled between an append, any number of logs might be durably
/// appended into storage. Use [`Self::metadata`] to know the latest state of storage.
#[derive(Debug)]
pub struct Sink {
    prev: Option<u64>,
    buf: Option<IoBuf>,
    tx: flume::Sender<Action>,
}

impl Sink {
    /// Sequence number of the last log record pushed to sink.
    pub fn prev_seq_no(&self) -> Option<u64> {
        self.prev
    }

    /// Push a new log record into storage.
    ///
    /// Note that this is a buffered writer (for efficiency reasons). To make sure
    /// accumulated records are sent to storage, invoke [`Sink::flush`].
    pub async fn push(&mut self, log: Log<'_>) -> Result<(), AppendError> {
        // Sequence validation.
        if let Some(prev) = self.prev
            && prev != log.prev_seq_no()
        {
            return Err(AppendError::Sequence(log.seq_no(), log.prev_seq_no(), prev));
        }

        // Limit checks.
        if log.size() >= Log::SIZE_LIMIT {
            return Err(AppendError::ExceedsLimit(log.seq_no(), log.size()));
        }

        // Attempt to simply buffer the log record.
        if !log.write(self.buf_ref()?) {
            // Underlying buffer doesn't have enough space.
            // We flush accumulated logs to disk, reclaiming space.
            self.flush().await?;

            // Now we must have enough space for the log record.
            if !log.write(self.buf_ref()?) {
                return Err(AppendError::Fault("Buffer too small for log"));
            }
        }

        self.prev = Some(log.seq_no());
        Ok(())
    }

    /// Flush accumulated log records to disk.
    pub async fn flush(&mut self) -> Result<(), AppendError> {
        // Return early if there is nothing to flush.
        let buf = self.buf_ref()?;
        if buf.is_empty() {
            return Ok(());
        }

        // Wait for a slot in the queue and submit append action.
        let (append, rx) = Action::append(self.take_buf()?);
        self.tx.send_async(append).await?;

        // Consume results from storage.
        let (mut buf, result) = rx.recv_async().await?;
        buf.clear();
        self.buf.replace(buf);
        result
    }

    fn take_buf(&mut self) -> Result<IoBuf, AppendError> {
        self.buf.take().ok_or(AppendError::Fault("Connection lost"))
    }

    fn buf_ref(&mut self) -> Result<&mut IoBuf, AppendError> {
        self.buf.as_mut().ok_or(AppendError::Fault("Connection lost"))
    }
}
