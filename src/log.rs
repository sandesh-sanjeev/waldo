//! User supplied sequenced log record.

use crate::runtime::IoBuf;
use bytemuck::{Pod, Zeroable};
use std::borrow::Cow;

/// A sequenced log record that can be appended into Journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log<'a>
where
    [u8]: ToOwned<Owned = Vec<u8>>,
{
    seq_no: u64,
    prev_seq_no: u64,
    data: Cow<'a, [u8]>,
}

impl Log<'_> {
    /// Padding bytes adding at the end of serialized log.
    /// This allows log header to stay at 8 byte aligned addresses.
    const PADDING: [u8; 7] = [0, 0, 0, 0, 0, 0, 0];

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

    /// Serialized size of the log record.
    pub fn size(&self) -> usize {
        let size = self.true_size();
        let padding = size % 8;
        size + padding
    }

    /// True size of the log record, excluding padding.
    pub(crate) fn true_size(&self) -> usize {
        Header::SIZE + self.data.len()
    }

    /// Write serialized log bytes into a buffer.
    ///
    /// Returns true when bytes were successfully written, false otherwise.
    /// This happens when buffer runs out of space for log bytes.
    ///
    /// # Arguments
    ///
    /// * `buf` - Destination buffer to write bytes.
    pub(crate) fn write(&self, buf: &mut IoBuf) -> bool {
        // Make sure buffer has enough remaining bytes.
        let size = self.size();
        if buf.remaining() < size {
            return false; // Rejected due to overflow.
        }

        // Header associated with the log.
        let header = Header::from(self);
        let padding = size - self.true_size();

        // Write serialized bytes into buffer.
        buf.extend_from_slice(header.bytes_of());
        buf.extend_from_slice(&self.data);
        buf.extend_from_slice(&Self::PADDING[..padding]);

        // Successfully written to buffer.
        true
    }

    /// Read next N bytes into a Log record, if there is one.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Source byte slice to fetch bytes.
    pub(crate) fn read(bytes: &[u8]) -> Option<Log<'_>> {
        // Not enough bytes for next log header.
        if bytes.len() < Header::SIZE {
            return None;
        }

        // Cast next N bytes into log header.
        let (header, rest) = bytes.split_at(Header::SIZE);
        let header = Header::from_bytes(header);

        // Check if there is enough space associated log bytes.
        let data_size = header.data_size as usize;
        if rest.len() < data_size {
            return None;
        }

        // Cast next N bytes as log payload.
        let (data, _) = rest.split_at(data_size);

        // Return fully deserialized log record.
        Some(Log::new_borrowed(header.seq_no, header.prev_seq_no, data))
    }
}

/// An iterator to iterate through a buffer of log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogIter<'a>(&'a [u8]);

impl<'a> IntoIterator for &'a IoBuf {
    type Item = Log<'a>;
    type IntoIter = LogIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        LogIter(self)
    }
}

impl<'a> Iterator for LogIter<'a> {
    type Item = Log<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let log = Log::read(self.0)?;
        self.0 = self.0.split_at(log.size()).1;
        Some(log)
    }
}

/// Header of a log record.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
struct Header {
    seq_no: u64,
    prev_seq_no: u64,
    data_size: u32,
    padding: u32,
}

impl From<&Log<'_>> for Header {
    fn from(value: &Log<'_>) -> Self {
        Self {
            seq_no: value.seq_no(),
            prev_seq_no: value.prev_seq_no(),
            data_size: value.data().len() as u32,
            padding: 0, // TODO: Add checksum here.
        }
    }
}

impl Header {
    /// A header has fixed set of attributes with fixed sizes.
    const SIZE: usize = size_of::<Self>();

    fn bytes_of(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }

    fn from_bytes(bytes: &[u8]) -> &Header {
        bytemuck::from_bytes(bytes)
    }
}
