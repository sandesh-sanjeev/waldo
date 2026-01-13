//! A stream of log records from storage.

use crate::log::Log;
use crate::runtime::IoBuf;
use crate::storage::{QueryError, Storage};
use std::cell::Cell;
use std::ops::Deref;

/// A stream subscription to discover new log records.
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
#[derive(Debug)]
pub struct Stream {
    prev: Cell<u64>,
    storage: Storage,
}

impl Stream {
    /// Create a new Stream.
    ///
    /// # Arguments
    ///
    /// * `prev` - Sequence number of the previous log consumed.
    /// * `storage` - Storage instance backing this sink.
    pub(crate) fn new(prev: u64, storage: Storage) -> Self {
        Self {
            storage,
            prev: Cell::new(prev),
        }
    }

    /// Sequence number of the previous log record observed in stream.
    pub fn prev_seq_no(&self) -> u64 {
        self.prev.get()
    }

    /// Fetch next set of log records from storage.
    ///
    /// Note that if the stream is caught up to the tip of storage, this method
    /// waits for more logs to be available in storage before completing. This
    /// makes interaction with storage very efficient, repeated polling is not
    /// necessary. Use [`Stream::try_next`] if you want results without waiting
    /// for more logs.
    pub async fn next(&mut self) -> Result<StreamLogs<'_>, QueryError> {
        loop {
            // See if there are newer log records available.
            if let Some(storage_prev) = *self.storage.watcher_mut().borrow_and_update()
                && storage_prev > self.prev.get()
            {
                break;
            };

            // Otherwise wait for new logs to get appended.
            self.storage.watcher_mut().changed().await?;
        }

        // Attempt to fetch the next set of logs.
        self.try_next().await
    }

    /// Fetch next set of log records from storage.
    ///
    /// Note that this method returns empty logs when there are no new logs available
    /// in storage. Use [`Stream::next`] if you want to wait for more logs to be available.
    pub async fn try_next(&mut self) -> Result<StreamLogs<'_>, QueryError> {
        // Return early if storage is not initialized.
        let Some(storage_prev) = *self.storage.watcher_mut().borrow_and_update() else {
            return Ok(StreamLogs {
                prev: &self.prev,
                buf: StreamBuf::Empty,
            });
        };

        // Return early if storage doesn't have next set of logs yet.
        let after_seq_no = self.prev.get();
        if storage_prev <= after_seq_no {
            return Ok(StreamLogs {
                prev: &self.prev,
                buf: StreamBuf::Empty,
            });
        }

        // Borrow buffer from pool for this query.
        // We'll return it back to pool once logs are consumed.
        let buf = self.storage.buf_pool().take_async().await;

        // Await and return result from storage.
        let (buf, result) = self.storage.query(after_seq_no, buf).run_async().await?;
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
            buf: StreamBuf::Io(buf, offset),
            prev: &self.prev,
        })
    }
}

/// Buffer of bytes from a log stream.
#[derive(Debug)]
enum StreamBuf {
    Empty,
    Io(IoBuf, usize),
}

impl Deref for StreamBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Empty => &[],
            Self::Io(buf, offset) => unsafe { buf.get_unchecked(*offset..) },
        }
    }
}

/// A chunk of contiguous log records discovered in stream.
#[derive(Debug)]
pub struct StreamLogs<'a> {
    buf: StreamBuf,
    prev: &'a Cell<u64>,
}

impl<'a> IntoIterator for &'a StreamLogs<'a> {
    type IntoIter = StreamLogIter<'a>;
    type Item = Log<'a>;

    fn into_iter(self) -> Self::IntoIter {
        StreamLogIter {
            prev: self.prev,
            bytes: &self.buf,
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
