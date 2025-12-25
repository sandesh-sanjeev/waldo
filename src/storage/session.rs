use crate::{
    log::Log,
    runtime::IoBuf,
    storage::{Action, Append, AsyncFate, FateError, Query},
};

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Fate of an append operation was lost")]
    Fault,

    #[error("Attempted to read {0} bytes, but only {1} was read")]
    IncompleteRead(usize, u32),

    #[error("Requested range of log after: {0} are trimmed")]
    Trimmed(u64),
}

impl From<FateError> for QueryError {
    fn from(_value: FateError) -> Self {
        QueryError::Fault
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Fate of an action was lost for unknown reasons")]
    Fault,

    #[error("A conflicting append is already in progress")]
    Conflict,

    #[error("Log record: {0} has prev: {1}, but expected prev: {2}")]
    Sequence(u64, u64, u64),

    #[error("Log record: {0} has size: {1} that exceeds limit: {2}")]
    ExceedsLimit(u64, usize, usize),

    #[error("Attempted to write {0} bytes, but only {1} was written")]
    IncompleteWrite(usize, u32),
}

impl From<FateError> for AppendError {
    fn from(_value: FateError) -> Self {
        AppendError::Fault
    }
}

/// An open session to read and write from storage.
pub struct Session<'a> {
    buf: Option<IoBuf>,
    action_tx: &'a flume::Sender<Action>,
}

impl Session<'_> {
    const LOG_SIZE_LIMIT: usize = 1024 * 1024;

    pub(super) fn new(buf: IoBuf, action_tx: &flume::Sender<Action>) -> Session<'_> {
        Session {
            action_tx,
            buf: Some(buf),
        }
    }

    pub async fn append(&mut self, logs: &[Log<'_>]) -> Result<(), AppendError> {
        // Return early if there is nothing to append.
        if logs.is_empty() {
            return Ok(());
        }

        // Perform all validations we can on the list of logs.
        Self::assert_logs(logs)?;

        // Split logs into chunks that makes most efficient use
        // of the buffer for appending logs into storage. Then
        // write all those chunks into storage.
        let mut logs = logs;
        while !logs.is_empty() {
            // Write log bytes to buffer.
            let mut buf = self.buf.take().ok_or(AppendError::Fault)?;
            buf.clear(); // Clear previous state.
            logs = Self::populate_buf(&mut buf, logs);

            // Append buffer bytes to storage.
            // Submit action to append bytes into storage.
            let (tx, rx) = AsyncFate::channel();
            let append = Action::Append(Append { buf, tx });
            self.action_tx
                .send_async(append)
                .await
                .map_err(|_| AppendError::Fault)?;

            // Await and return result from storage.
            let (buf, result) = rx.recv_async().await?;
            self.buf.replace(buf);
            result?;
        }

        // All the log records were successfully appended.
        Ok(())
    }

    pub async fn query(&mut self, after_seq_no: u64) -> Result<impl Iterator<Item = Log<'_>>, QueryError> {
        // Fetch buffer to bytes to submit to storage.
        let mut buf = self.buf.take().ok_or(QueryError::Fault)?;
        buf.clear(); // Clear any state from previous action.

        // Submit action to append bytes into storage.
        let (tx, rx) = AsyncFate::channel();
        let query = Action::Query(Query { buf, tx, after_seq_no });
        self.action_tx.send_async(query).await.map_err(|_| QueryError::Fault)?;

        // Await and return result from storage.
        let (buf, result) = rx.recv_async().await?;
        self.buf.replace(buf);
        result?; // First return error, if any, from storage.

        // Return reference to all the log records read from query.
        // A filter is necessary here because storage can read slightly more logs are
        // the beginning. It's more efficient to filter it out here
        let buf = self.buf.as_ref().expect("Associated buffer must exist");
        Ok(buf.into_iter().filter(move |log| log.seq_no() > after_seq_no))
    }

    fn assert_logs(logs: &[Log<'_>]) -> Result<(), AppendError> {
        let mut prev_seq_no = None;
        for log in logs {
            if let Some(prev) = prev_seq_no
                && prev != log.prev_seq_no()
            {
                return Err(AppendError::Sequence(log.seq_no(), log.prev_seq_no(), prev));
            }

            if log.size() >= Self::LOG_SIZE_LIMIT {
                return Err(AppendError::ExceedsLimit(
                    log.seq_no(),
                    log.size(),
                    Self::LOG_SIZE_LIMIT,
                ));
            }

            prev_seq_no = Some(log.seq_no());
        }

        Ok(())
    }

    fn populate_buf<'a>(buf: &mut IoBuf, logs: &'a [Log<'a>]) -> &'a [Log<'a>] {
        let mut consumed = 0;
        for log in logs {
            if !log.write(buf) {
                break;
            }

            consumed += 1;
        }

        logs.split_at(consumed).1
    }
}
