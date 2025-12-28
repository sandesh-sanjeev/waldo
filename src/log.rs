//! User supplied sequenced log record.

use crate::runtime::IoBuf;
use bytemuck::{Pod, Zeroable};
use std::borrow::Cow;
use xxhash_rust::xxh3;

/// A sequenced log record that can be appended into Journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log<'a>
where
    [u8]: ToOwned<Owned = Vec<u8>>,
{
    seq_no: u64,
    prev_seq_no: u64,
    data: Cow<'a, [u8]>,
    data_hash: u32,
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
            data_hash: Self::gen_hash(data),
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
            data_hash: Self::gen_hash(&data),
            data: Cow::Owned(data),
        }
    }

    /// Registered hash of the log record.
    pub fn data_hash(&self) -> u32 {
        self.data_hash
    }

    /// Validate integrity of log payload.
    pub fn validate_data(&self) -> Result<(), Error> {
        let hash = Log::gen_hash(self.data());
        if hash != self.data_hash() {
            return Err(Error::Corruption(self.seq_no, self.data_hash, hash));
        }

        Ok(())
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

        // Additional padding we need to add to a log record
        // so that address of the starting offset of a log record
        // to be aligned to 8 byte boundaries. This makes deserialization
        // of the header a pure pointer cast, i.e, fast.
        let extra = size & 7;
        let padding = if extra > 0 { 8 - extra } else { 0 };

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
        let (header, rest) = unsafe { bytes.split_at_unchecked(Header::SIZE) };
        let header = Header::from_bytes(header);

        // Check if there is enough space associated log bytes.
        let data_size = header.data_size as usize;
        if rest.len() < data_size {
            return None;
        }

        // Cast next N bytes as log payload.
        let (data, _) = unsafe { rest.split_at_unchecked(data_size) };

        // Return fully deserialized log record.
        Some(Log {
            seq_no: header.seq_no,
            prev_seq_no: header.prev_seq_no,
            data_hash: header.data_hash,
            data: Cow::Borrowed(data),
        })
    }

    /// Generate hash for the log record.
    fn gen_hash(data: &[u8]) -> u32 {
        // For an ideal hash function, any N bits should make
        // collision detection 1 / (2 ^ N - 1). We expect logs
        // to be < 1 KB most of the time, 32 bits should provide
        // plenty of bites for collision resistance.
        xxh3::xxh3_64(data) as u32
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
        self.0 = unsafe { self.0.split_at_unchecked(log.size()).1 };
        Some(log)
    }
}

/// Different types of errors when validating a log record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// Error when an out of sequence log is detected.
    #[error("Log record: {0} has prev: {1}, but expected prev: {2}")]
    Sequence(u64, u64, u64),

    /// Error when a corrupted log record is detected.
    #[error("Log record: {0} has hash: {1}, but expected hash: {2}")]
    Corruption(u64, u32, u32),
}

/// An iterator that validates log sequence before handing them out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequencedLogIter<'a> {
    bytes: &'a [u8],
    prev_seq_no: u64,
    is_error: bool,
}

impl SequencedLogIter<'_> {
    /// Create a new checked iterator.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Bytes to iterate through.
    /// * `prev_seq_no` - Sequence number of the immediately previous log.
    pub(crate) fn new(bytes: &[u8], prev_seq_no: u64) -> SequencedLogIter<'_> {
        SequencedLogIter {
            bytes,
            prev_seq_no,
            is_error: false,
        }
    }
}

impl<'a> Iterator for SequencedLogIter<'a> {
    type Item = Result<Log<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // Return early if this iterator already completed with an error.
        if self.is_error {
            return None;
        }

        // Attempt to deserialize the next set of bytes into log.
        let log = Log::read(self.bytes)?;
        self.bytes = unsafe { self.bytes.split_at_unchecked(log.size()).1 };

        // Make sure unbroken sequence of logs.
        if self.prev_seq_no != log.prev_seq_no() {
            self.is_error = true;
            return Some(Err(Error::Sequence(log.seq_no(), log.prev_seq_no(), self.prev_seq_no)));
        }

        // Alright, everything looks good!
        self.prev_seq_no = log.seq_no();
        Some(Ok(log))
    }
}

/// Header of a log record.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
struct Header {
    seq_no: u64,
    prev_seq_no: u64,
    data_hash: u32,
    data_size: u32,
}

impl From<&Log<'_>> for Header {
    fn from(value: &Log<'_>) -> Self {
        Self {
            seq_no: value.seq_no(),
            prev_seq_no: value.prev_seq_no(),
            data_size: value.data().len() as u32,
            data_hash: value.data_hash(),
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
