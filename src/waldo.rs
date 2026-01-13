//! Front end to the storage engine.

use crate::sink::Sink;
use crate::storage::Storage;
use crate::storage::{Error, Metadata, Options};
use crate::stream::Stream;
use std::path::Path;

/// Storage engine for persistent storage of sequential log records.
///
/// Waldo exposes a `stream` interface with two parts, a sink and a stream.
///
/// ## [`Sink`]
///
/// A sink is an interface to push new logs to storage. Under the hood it is a
/// buffered writer that flushes buffered logs to storage when buffer is full.
/// Optionally one can flush without waiting for the buffer to fill up.
///
/// ## [`Stream`]
///
/// A stream is an interface to discovered new logs appended to storage. One creates
/// a stream subscription with a starting sequence number. This stream can be used
/// indefinitely to repeatedly discover new logs.
///
/// ## Warning
///
/// Waldo attempts to perform graceful shutdown when dropped. Unfortunately there
/// is no support for drop in async. If [`Waldo::close`] is not explicitly invoked
/// and awaited to completion, Waldo will initiate graceful shutdown during drop
/// which will block the async runtime. This is typically not a big deal, especially
/// in multi-threaded runtimes. However it is recommended to asynchronously invoke close
/// when possible.
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

    /// Create a new sink to publish log records to storage.
    ///
    /// Use [`Sink::push`] to append new log records into the Sink. When internal
    /// buffers fill up, accumulated logs are flushed to disk. Use [`Sink::flush`]
    /// to ensure any accumulated logs are flushed to storage.
    ///
    /// Note that there might be data loss if a sink is dropped without a flush.
    /// Even though we can easily support blocking flush during drop, there is no
    /// way to return results.
    pub fn sink(&self) -> Sink {
        Sink::new(self.0.clone())
    }

    /// Create a new log stream subscription.
    ///
    /// Note that this is a cold stream, i.e, it is a passive future that makes
    /// progress when you await for more logs similar to regular futures. Use
    /// [`Stream::next`] or [`Stream::try_next`] to discover new logs.
    ///
    /// # Arguments
    ///
    /// * `cursor` - Cursor to start streaming log records.
    pub fn stream(&self, cursor: Cursor) -> Stream {
        Stream::new(self.cursor_seq_no(cursor), self.0.clone())
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
