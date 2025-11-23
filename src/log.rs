//! Sequenced, user supplied log record and associated buffers.

use std::borrow::Cow;
use thiserror::Error;

/// Results from log interactions.
pub type Result<T> = std::result::Result<T, Error>;

/// Different types errors when working with log records.
#[derive(Debug, Error)]
pub enum Error {
    /// Error when log record is constructed with invalid sequence number.
    #[error("seq_no {0} <= prev_seq_no {1}")]
    InvalidSequence(u64, u64),

    /// Error when log records are encountered out of order.
    #[error("seq_no {0}, expected prev {1} but got {2}")]
    OutOfSequence(u64, u64, u64),

    /// Error when size of log data exceeds limits.-
    #[error("seq_no {0} data size {1} exceeds limit {limit}", limit = Log::DATA_SIZE_LIMIT)]
    DataExceedsLimit(u64, usize),

    /// Error when corruption is detected.
    #[error("seq_no: {0}, expected hash {1}, but got {2}")]
    MalformedBytes(u64, u64, u64),
}

/// A sequenced log record that can be appended into Journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log<'a>
where
    [u8]: ToOwned<Owned = Vec<u8>>,
{
    pub(crate) seq_no: u64,
    pub(crate) prev_seq_no: u64,
    pub(crate) data: Cow<'a, [u8]>,
}

impl Log<'_> {
    /// Maximum allowed size of log data.
    #[cfg(not(test))]
    pub const DATA_SIZE_LIMIT: usize = u32::MAX as usize;

    /// Maximum allowed size of log data.
    #[cfg(test)]
    pub const DATA_SIZE_LIMIT: usize = u16::MAX as usize;

    /// Create new log from borrowed data.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Sequence number of the log.
    /// * `prev_seq_no` - Sequence number of previous log.
    /// * `data` - Payload of the log.
    pub fn new_borrowed(seq_no: u64, prev_seq_no: u64, data: &[u8]) -> Result<Log<'_>> {
        Log::new(seq_no, prev_seq_no, Cow::Borrowed(data))
    }

    /// Create new log from owned data.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Sequence number of the log.
    /// * `prev_seq_no` - Sequence number of previous log.
    /// * `data` - Payload of the log.
    pub fn new_owned(seq_no: u64, prev_seq_no: u64, data: Vec<u8>) -> Result<Log<'static>> {
        Log::new(seq_no, prev_seq_no, Cow::Owned(data))
    }

    /// Sequence number of the log record.
    pub fn seq_no(&self) -> u64 {
        self.seq_no
    }

    /// Sequence number of the previous log record.
    pub fn prev_seq_no(&self) -> u64 {
        self.prev_seq_no
    }

    /// Reference to data held in log.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn new(seq_no: u64, prev_seq_no: u64, data: Cow<'_, [u8]>) -> Result<Log<'_>> {
        // Sequence numbers should be monotonically increasing.
        if seq_no <= prev_seq_no {
            return Err(Error::InvalidSequence(seq_no, prev_seq_no));
        }

        // Size of log payload has practical limits applied to them.
        if data.len() > Self::DATA_SIZE_LIMIT {
            return Err(Error::DataExceedsLimit(seq_no, data.len()));
        }

        Ok(Log {
            seq_no,
            prev_seq_no,
            data,
        })
    }
}
