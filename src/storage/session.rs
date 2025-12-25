use crate::{
    log::Log,
    runtime::IoBuf,
    storage::{Action, Append, AppendError, AsyncFate, Error, Query, QueryError},
};

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
            // Fetch buffer to bytes to submit to storage.
            let mut buf = self.buf.take().ok_or(Error::UnexpectedClose)?;
            buf.clear(); // Clear any state from previous action.

            // Populate the buffer, with as many logs as possible.
            let mut consumed = 0;
            for log in logs {
                // We've consumed enough if buffer doesn't have space for the log.
                if !log.write(&mut buf) {
                    break;
                }

                // Track the last log written into the buffer.
                consumed += 1;
            }

            // Append buffer bytes to storage.
            // Submit action to append bytes into storage.
            let (tx, rx) = AsyncFate::channel();
            self.action_tx
                .send_async(Action::Append(Append { buf, tx }))
                .await
                .map_err(|_| Error::UnexpectedClose)?;

            // Await and return result from storage.
            let (buf, result) = rx.recv_async().await.map_err(Error::from)?;
            self.buf.replace(buf);

            // Process results from append operation.
            result?;
            logs = logs.split_at(consumed).1;
        }

        // All the log records were successfully appended.
        Ok(())
    }

    pub async fn query(&mut self, after_seq_no: u64) -> Result<impl Iterator<Item = Log<'_>>, QueryError> {
        // Fetch buffer to bytes to submit to storage.
        let mut buf = self.buf.take().ok_or(Error::UnexpectedClose)?;
        buf.clear(); // Clear any state from previous action.

        // Submit action to append bytes into storage.
        let (tx, rx) = AsyncFate::channel();
        self.action_tx
            .send_async(Action::Query(Query { buf, tx, after_seq_no }))
            .await
            .map_err(|_| Error::UnexpectedClose)?;

        // Await and return result from storage.
        let (buf, result) = rx.recv_async().await.map_err(Error::from)?;
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
}
