//! A buffered writer to push logs into storage.

use crate::log::Log;
use crate::runtime::IoBuf;
use crate::storage::{AppendError, Storage};

/// A sink is buffered log writer.
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
/// you want guarantee that bytes are durably flushed to disk when [`Sink::flush`]
/// successfully completes, enable [`Options::file_o_dsync`]. It's a great default to
/// start off with.
///
/// ## Isolation
///
/// [`Sink::push`] is executed with sequential consistency, i.e, all appends, even
/// across threads, always operate against latest state of storage. If storage processes
/// two concurrent appends, one of them will be rejected with conflict.
///
/// ## Flush
///
/// A sink writes to storage only when backing buffer is full, which could take arbitrary
/// time or never. Use [`Sink::flush`] to make sure all accumulated logs have been dispatched
/// to storage. Importantly, if the sink is dropped without a flush, any accumulated logs
/// will be lost.
///
/// ## Cancel safety
///
/// If [`Sink::push`] is cancelled between an append, any number of logs might be durably
/// appended into storage. Use [`Waldo::metadata`] for detailed info about current state
/// of storage, or [`Waldo::prev_seq_no`] to just know sequence number of the last append
/// log record.
#[derive(Debug)]
pub struct Sink {
    prev: Option<u64>,
    buf: Option<IoBuf>,
    storage: Storage,
}

impl Sink {
    /// Create a new Sink.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage instance backing this sink.
    pub(crate) fn new(storage: Storage) -> Self {
        let prev = *storage.watcher().borrow();
        Self {
            prev,
            storage,
            buf: None,
        }
    }

    /// Sequence number of the last log record pushed to sink.
    pub fn prev_seq_no(&self) -> Option<u64> {
        self.prev
    }

    /// Push a new log record into storage.
    ///
    /// Note that this is a buffered writer (for efficiency reasons). To make sure
    /// accumulated records are sent to storage, invoke [`Sink::flush`].
    ///
    /// # Arguments
    ///
    /// * `log` - Log record to append into storage.
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
        if !log.write(self.buf_ref().await) {
            // Underlying buffer doesn't have enough space.
            // We flush accumulated logs to disk, reclaiming space.
            self.flush().await?;

            // Now we must have enough space for the log record.
            if !log.write(self.buf_ref().await) {
                return Err(AppendError::Fault("Buffer too small for log"));
            }
        }

        self.prev = Some(log.seq_no());
        Ok(())
    }

    /// Flush accumulated log records to storage.
    ///
    /// Note that this method is no-op if there were no logs to flush to storage.
    /// Also, this is NOT equivalent to `fsync`, there is no guarantee that logs
    /// are durably stored on disk.
    ///
    /// Also, a flush effectively releases borrowed resources. Be sure to flush when
    /// you are done with your logical unit of work, such as when you're done appending
    /// an batch of log records.
    pub async fn flush(&mut self) -> Result<(), AppendError> {
        // Return early if there is nothing to flush.
        let Some(buf) = self.buf.take() else {
            return Ok(());
        };

        // Return early if there is nothing to append into storage.
        if buf.is_empty() {
            return Ok(());
        }

        // Create and run a storage action to append log records.
        let (_, result) = self.storage.append(buf).run_async().await?;
        result
    }

    async fn buf_ref(&mut self) -> &mut IoBuf {
        // We don't always hold on to buffer, it's wasteful.
        // So, we don't have buffer, we borrow one from pool.
        if self.buf.is_none() {
            self.buf = Some(self.storage.buf_pool().take_async().await);
        }

        self.buf.as_mut().expect("Buffer should be set")
    }
}
