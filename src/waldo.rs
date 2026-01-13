//! Front end to the storage engine.

use crate::log::LogIter;
use crate::runtime::IoBuf;
use crate::storage::Storage;
use crate::storage::{Error, Metadata, Options};
use crate::{AppendError, Log, QueryError};
use std::ops::Deref;
use std::path::Path;

/// Storage engine for persistent storage of sequential log records.
///
/// ## Clones
///
/// Waldo can be cloned and shared across thread boundaries without any synchronization. However
/// note that concurrent appends is not supported. While appends can be initiated from multiple
/// threads/tasks, if storage processes them in parallel, one will be rejected.
///
/// ## Warning
///
/// Waldo attempts to perform graceful shutdown when dropped. Unfortunately there is no support
/// for drop in async. If [`Waldo::close`] is not explicitly invoked and awaited to completion,
/// Waldo will initiate graceful shutdown during drop which will block the async runtime. This
/// is typically not a big deal, especially in multi-threaded runtimes. However it is recommended
/// to asynchronously invoke close when possible.
#[derive(Debug, Clone)]
pub struct Waldo(Storage);

impl Waldo {
    /// Open an instance of storage at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the directory on disk.
    /// * `opts` - Options used to create storage.
    pub async fn open<P: AsRef<Path>>(path: P, opts: Options) -> Result<Self, Error> {
        Ok(Self(Storage::open(path, opts).await?))
    }

    /// Latest metadata for storage, if storage is initialized.
    ///
    /// Note this gets a rich summary of the current state. It's great for initialization,
    /// periodic scans for telemetry, etc. Use [`Waldo::prev_seq_no`] if all you want is
    /// sequence number of the last appended log.
    pub async fn metadata(&self) -> Option<Metadata> {
        self.0.metadata().run_async().await.ok()?
    }

    /// Sequence number of the last log record in storage.
    ///
    /// Note that this is just an efficient way to know progress made in storage.
    /// Use [`Waldo::metadata`] for more detailed info about current state. Both
    /// methods return None when storage is not initialized.
    pub fn prev_seq_no(&self) -> Option<u64> {
        *self.0.watcher().borrow()
    }

    /// Wait for more log records from storage.
    ///
    /// Repeatedly polling storage in an attempt to discover new log records is not efficient.
    /// In some cases, for example if you were to build a streaming interface, it's better to
    /// just wait for new logs to be available in storage.
    ///
    /// # Arguments
    ///
    /// * `after` - Method completes when storage has logs after this sequence number.
    pub async fn watch_for_after(&mut self, after: u64) -> Result<(), Error> {
        loop {
            // See if there are newer log records available.
            if let Some(storage_prev) = *self.0.watcher_mut().borrow_and_update()
                && storage_prev > after
            {
                return Ok(());
            };

            // Otherwise wait for new logs to get appended.
            self.0.watcher_mut().changed().await?;
        }
    }

    /// Append an ordered sequence of log records into storage.
    ///
    /// ## Atomicity
    ///
    /// Atomicity is only guaranteed for a single log record. Now (and probably never)
    /// will we provide atomicity guarantees for an entire batch. While this is doable,
    /// it's more code and more importantly, I have no need for it.
    ///
    /// ## Durability
    ///
    /// Durability guarantees depend on the options used open storage. Specifically, if
    /// you want guarantee that bytes are durably flushed to disk when append successfully
    /// completes, enable [`Options::file_o_dsync`]. It's a great default to start off with.
    ///
    /// ## Isolation
    ///
    /// append is executed with sequential consistency, i.e, all appends, even across threads,
    /// always operate against latest state of storage. If storage processes two concurrent
    /// appends, one of them will be rejected with conflict.
    ///
    /// ## Cancel safety
    ///
    /// If append is cancelled between an append, any number of logs might be durably appended
    /// into storage. However, they are guaranteed to be appended in order and without gaps.
    pub async fn append(&self, logs: &[Log<'_>]) -> Result<(), AppendError> {
        // Return early if there is nothing else to do.
        if logs.is_empty() {
            return Ok(());
        }

        // Perform all validations upfront.
        let mut prev = *self.0.watcher().borrow();
        for log in logs {
            // Sequence validation.
            if let Some(prev) = prev
                && prev != log.prev_seq_no()
            {
                return Err(AppendError::Sequence(log.seq_no(), log.prev_seq_no(), prev));
            }

            // Limit checks.
            if log.size() >= Log::SIZE_LIMIT {
                return Err(AppendError::ExceedsLimit(log.seq_no(), log.size()));
            }

            // For next iteration.
            prev = Some(log.seq_no());
        }

        // Push the logs in batches.
        let mut logs = logs;
        let mut buf = self.0.buf_pool().take_async().await;
        while !logs.is_empty() {
            // To clear state from previous iteration.
            buf.clear();

            // Fully saturate the borrowed buffer.
            let mut end = 0;
            for log in logs.iter() {
                // No space in the buffer for the log record.
                // Flush accumulated logs and then process remaining.
                if !log.write(&mut buf) {
                    break;
                }

                end += 1;
            }

            // Push accumulated logs to storage.
            let result = self.0.append(buf).run_async().await?;

            // Process results from storage.
            buf = result.0;
            result.1?;

            // Update state for next iteration.
            logs = logs.split_at(end).1;
        }
        Ok(())
    }

    /// Query log records from storage.
    ///
    /// If requested logs exist in storage, they are returned as a contiguous
    /// chunk of sequential log records in ascending order of sequence numbers.
    ///
    /// You cannot specify limits, this method will query for as many log records
    /// as efficiently possible. You can discard logs you don't want, it's free.
    ///
    /// ## Isolation
    ///
    /// Only logs that have been successfully committed against storage are visible
    /// to queries against storage. However note that they might not be durably stored
    /// on disk.
    pub async fn query(&mut self, cursor: Cursor) -> Result<QueryLogs, QueryError> {
        // Return early if storage is not initialized.
        let Some(storage_prev) = *self.0.watcher().borrow() else {
            return Ok(QueryLogs(QueryBuf::Empty));
        };

        // Return early if storage doesn't have next set of logs yet.
        let after_seq_no = self.cursor_seq_no(cursor);
        if storage_prev <= after_seq_no {
            return Ok(QueryLogs(QueryBuf::Empty));
        }

        // Query requested range of log records from storage.
        let buf = self.0.buf_pool().take_async().await;
        let (buf, result) = self.0.query(after_seq_no, buf).run_async().await?;
        result?;

        // Figure out offset to the starting seq_no.
        // For performance reasons storage might return a few extra logs
        // at the beginning of the buffer. We have to skip past those.
        let offset = buf
            .into_iter()
            .take_while(|log| log.seq_no() <= after_seq_no)
            .map(|log| log.size())
            .sum();

        Ok(QueryLogs(QueryBuf::Io(buf, offset)))
    }

    /// Initiate graceful shutdown of storage.
    ///
    /// If there are no other references to Waldo and this method completes
    /// successfully, you can be guaranteed that all logs appended to storage
    /// have been durably stored on disk (unless they were trimmed away to reclaim
    /// space for new logs).
    pub async fn close(self) {
        self.0.close().await
    }

    fn cursor_seq_no(&self, cursor: Cursor) -> u64 {
        match cursor {
            Cursor::Trim => 0,
            Cursor::After(seq_no) => seq_no,
            Cursor::From(seq_no) => seq_no.saturating_sub(1),
            Cursor::Tip => self.prev_seq_no().unwrap_or(0),
        }
    }
}

/// Different logical positions for stream subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cursor {
    /// Start streaming from the oldest log record.
    Tip,

    /// Start streaming from after newest log record.
    Trim,

    /// Start streaming after (exclusive) the provided sequence number.
    After(u64),

    /// Start streaming from (inclusive) the provided sequence number.
    From(u64),
}

/// A chunk of contiguous log records queried from storage.
#[derive(Debug)]
pub struct QueryLogs(QueryBuf);

impl<'a> IntoIterator for &'a QueryLogs {
    type IntoIter = LogIter<'a>;
    type Item = Log<'a>;

    fn into_iter(self) -> Self::IntoIter {
        LogIter(&self.0)
    }
}

/// Different types of buffers returned from a query.
#[derive(Debug)]
enum QueryBuf {
    Empty,
    Io(IoBuf, usize),
}

impl Deref for QueryBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Empty => &[],
            Self::Io(buf, offset) => unsafe { buf.get_unchecked(*offset..) },
        }
    }
}
