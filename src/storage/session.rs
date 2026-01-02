//! A long lived connection to storage.

use crate::{
    log::{Log, SequencedLogIter},
    runtime::IoBuf,
    storage::{Action, FateError},
};

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
    pub(super) fn new(buf: IoBuf, action_tx: &flume::Sender<Action>) -> Session<'_> {
        Session {
            action_tx,
            buf: Some(buf),
        }
    }

    /// Append logs to storage.
    ///
    /// Note that this does not provide atomicity or durability guarantees as of today,
    /// maybe forever, in the name of performance, cancel safety and ultimately simplicity.
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
            // Make sure a contiguous sequence of logs were provided.
            if let Some(prev) = prev_seq_no
                && prev != log.prev_seq_no()
            {
                return Err(AppendError::Sequence(log.seq_no(), log.prev_seq_no(), prev));
            }

            // Make sure logs stay within their size limits.
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
