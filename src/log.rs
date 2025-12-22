//! User supplied sequenced log record.

use crate::runtime::IoBuf;
use std::borrow::Cow;

/// A sequenced log record that can be appended into Journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log<'a>
where
    [u8]: ToOwned<Owned = Vec<u8>>,
{
    pub seq_no: u64,
    pub prev_seq_no: u64,
    pub data: Cow<'a, [u8]>,
}

impl Log<'_> {
    /// Create new log from borrowed data.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Sequence number of the log.
    /// * `prev_seq_no` - Sequence number of previous log.
    /// * `data` - Payload of the log.
    pub fn new_borrowed(seq_no: u64, prev_seq_no: u64, data: &[u8]) -> Log<'_> {
        Log {
            seq_no,
            prev_seq_no,
            data: Cow::Borrowed(data),
        }
    }

    /// Create new log from owned data.
    ///
    /// # Arguments
    ///
    /// * `seq_no` - Sequence number of the log.
    /// * `prev_seq_no` - Sequence number of previous log.
    /// * `data` - Payload of the log.
    pub fn new_owned(seq_no: u64, prev_seq_no: u64, data: Vec<u8>) -> Log<'static> {
        Log {
            seq_no,
            prev_seq_no,
            data: Cow::Owned(data),
        }
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

    pub(crate) const fn header_size() -> usize {
        size_of::<u64>() // seq_no
        + size_of::<u64>() // prev_seq_no
        + size_of::<u32>() // data_size
    }

    pub(crate) fn size(&self) -> usize {
        Self::header_size() + self.data.len()
    }

    pub(crate) fn write(&self, buf: &mut IoBuf) -> bool {
        // Make sure buffer has enough remaining bytes.
        if buf.remaining() < self.size() {
            return false; // Rejected due to overflow.
        }

        // Serialize all the different parts of Log.
        let seq_no_bytes = self.seq_no.to_be_bytes();
        let prev_seq_no_bytes = self.prev_seq_no.to_be_bytes();
        let log_size_bytes = (self.data.len() as u32).to_be_bytes();

        // Write serialized bytes into buffer.
        buf.extend_from_slice(&seq_no_bytes);
        buf.extend_from_slice(&prev_seq_no_bytes);
        buf.extend_from_slice(&log_size_bytes);
        buf.extend_from_slice(&self.data);

        // Successfully written to buffer.
        true
    }

    pub(crate) fn read(bytes: &[u8]) -> Option<Log<'_>> {
        // Split next N bytes into interesting sections.
        let (header, rest) = bytes.split_at_checked(Self::header_size())?;
        let (seq_no, header_rest) = header.split_at(size_of::<u64>());
        let (prev_seq_no, data_size) = header_rest.split_at(size_of::<u64>());

        // Deserialize all the different sections of bytes.
        let seq_no = u64::from_be_bytes(seq_no.try_into().ok()?);
        let prev_seq_no = u64::from_be_bytes(prev_seq_no.try_into().ok()?);
        let data_size = u32::from_be_bytes(data_size.try_into().ok()?);
        let (data, _) = rest.split_at_checked(data_size as _)?;

        // Return fully deserialized log record.
        Some(Log::new_borrowed(seq_no, prev_seq_no, data))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogIter<'a> {
    bytes: &'a [u8],
    after: Option<u64>,
}

impl LogIter<'_> {
    pub fn iter(bytes: &[u8]) -> LogIter<'_> {
        LogIter { after: None, bytes }
    }

    pub fn iter_after(after: Option<u64>, bytes: &[u8]) -> LogIter<'_> {
        LogIter { after, bytes }
    }
}

impl<'a> IntoIterator for &'a IoBuf {
    type Item = Log<'a>;
    type IntoIter = LogIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        LogIter::iter(self)
    }
}

impl<'a> Iterator for LogIter<'a> {
    type Item = Log<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Deserialize the next log record.
            let log = Log::read(self.bytes)?;
            self.bytes = self.bytes.split_at(log.size()).1;

            // Skip the log record if it is lesser than starting seq_no.
            // Listing will have an unbounded start if starting seq_no is not provided.
            if let Some(after) = self.after
                && log.seq_no() <= after
            {
                continue;
            }

            // Return the deserialized log record.
            return Some(log);
        }
    }
}
