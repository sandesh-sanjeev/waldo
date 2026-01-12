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
    /// Maximum size of a log record of 1 MB.
    /// TODO: Add checks for buffer size and bytes between sparse index.
    pub const SIZE_LIMIT: usize = 1024 * 1024;

    /// Padding bytes added at the end of serialized log.
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

    /// Borrow an immutable copy of this log.
    pub fn borrow(&self) -> Log<'_> {
        Log::new_borrowed(self.seq_no, self.prev_seq_no, &self.data)
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
        // This rounds up the size so that it is aligned to 8 byte boundaries.
        // This makes deserialization of the header a pure pointer cast, i.e, fast.
        (self.true_size() + 7) & !7
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
            return false;
        }

        // Header associated with the log.
        let header = Header::from(self);
        let padding = size - self.true_size();

        // Write serialized bytes into buffer.
        buf.extend_from_slice(header.bytes_of());
        buf.extend_from_slice(&self.data);
        buf.extend_from_slice(&Self::PADDING[..padding]);
        true
    }

    /// Read next N bytes into a Log record, if there is one.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Source byte slice to fetch bytes.
    pub(crate) fn read(bytes: &[u8]) -> Option<Log<'_>> {
        // Cast next N bytes into log header.
        let (header, rest) = bytes.split_at_checked(Header::SIZE)?;
        let header = Header::from_bytes(header);

        // Fetch payload associated with the log record.
        let data_size = header.data_size as usize;
        let data = rest.get(..data_size)?;

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
        // plenty of bits for reasonable probably of collision
        // resistance.
        //
        // TODO: Mix seq_no and prev_seq_no into the hash.
        xxh3::xxh3_64(data) as u32
    }
}

/// An iterator to iterate through a buffer of log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogIter<'a>(pub(crate) &'a [u8]);

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
        // Attempt to deserialize the next set of bytes into log.
        let log = Log::read(self.0)?;

        // Safety: We just read enough bytes for the parsed log record.
        // Compiler/LLVM is not smart enough to know this and remove bounds check.
        self.0 = unsafe { self.0.get_unchecked(log.size()..) };

        // Return fully parsed record.
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

/// Strategy to generate arbitrary log records for prop tests.
#[cfg(test)]
pub(crate) mod strategy {
    use crate::Log;
    use proptest::collection::hash_set;
    use proptest::collection::vec;
    use proptest::prelude::*;

    // Limit the size of an individual log record, so no unbounded test inputs.
    pub(crate) const DATA_LIMIT: usize = 64 * 1024; // 64 KB

    /// Strategy to generate an individual log record.
    pub(crate) fn arbitrary_log() -> impl Strategy<Value = Log<'static>> {
        (1..u64::MAX)
            .prop_flat_map(|seq_no| (Just(seq_no), 0..seq_no, vec(any::<u8>(), 0..DATA_LIMIT)))
            .prop_map(|(seq_no, prev_seq_no, data)| Log::new_owned(seq_no, prev_seq_no, data))
    }

    /// Strategy to generate an arbitrary list of log records.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of log record to generate.
    pub(crate) fn arbitrary_logs(limit: usize) -> impl Strategy<Value = Vec<Log<'static>>> {
        (0..limit)
            .prop_map(|count| {
                (
                    hash_set(any::<u64>(), count + 1),
                    vec(vec(any::<u8>(), 0..DATA_LIMIT), count),
                )
            })
            .prop_flat_map(|(seq_no_strategy, data_strategy)| {
                (seq_no_strategy, data_strategy).prop_map(|(seq_nos, data)| {
                    let mut seq_nos: Vec<_> = seq_nos.into_iter().collect();

                    seq_nos.sort();
                    let mut logs = Vec::with_capacity(data.len());
                    for (seq_no_pair, data) in seq_nos.windows(2).zip(data.into_iter()) {
                        let seq_no = seq_no_pair[0];
                        let prev_seq_no = seq_no_pair[1];
                        logs.push(Log::new_owned(seq_no, prev_seq_no, data));
                    }

                    logs
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::strategy::*;
    use super::*;
    use crate::runtime::{BufPool, PoolOptions};
    use anyhow::Result;
    use assert2::let_assert;
    use proptest::prelude::*;
    use proptest::prop_assert_eq;

    // We'll have enough capacity for at least 10 full log records.
    const BUF_CAPACITY: usize = (Header::SIZE + DATA_LIMIT) * 10;

    proptest::proptest! {
        #[test]
        fn header_round_trip(header in arbitrary_header()) {
            let_assert!(Ok(pool) = buffer_pool(1));

            // Copy serialized bytes to a buffer.
            let mut buf = pool.take();
            buf.extend_from_slice(header.bytes_of());

            // Deserialize bytes into a reference to header.
            let deserialized = Header::from_bytes(&buf);
            prop_assert_eq!(deserialized, &header);
        }

        #[test]
        fn log_round_trip(log in arbitrary_log()) {
            let_assert!(Ok(pool) = buffer_pool(1));

            // Copy serialized bytes to a buffer.
            let mut buf = pool.take();
            prop_assert!(log.write(&mut buf));

            // Make sure size is accurate.
            prop_assert_eq!(buf.len(), log.size());

            // Deserialize bytes into a reference to header.
            let deserialized = Log::read(&buf);
            prop_assert_eq!(deserialized, Some(log));
        }

        #[test]
        fn hash_mismatch_validation_error(mut log in arbitrary_log()) {
            prop_assert!(log.validate_data().is_ok());

            // Mess around with data hash.
            // This simulates corruption, should result in error.
            log.data_hash = log.data_hash.wrapping_sub(1);
            let_assert!(Err(Error::Corruption(_, _, _)) = log.validate_data());
        }

        #[test]
        fn log_borrowed_owned_equivalence(log in arbitrary_log()) {
            let_assert!(Ok(pool) = buffer_pool(2));

            // Create a borrowed copy of the log record.
            // It should be identical to owned copy in every way.
            let borrow = log.borrow();
            prop_assert_eq!(&borrow, &log);

            // Make sure serialized bytes match too.
            let mut buf = pool.take();
            let mut buf_borrow = pool.take();

            prop_assert!(log.write(&mut buf));
            prop_assert!(borrow.write(&mut buf_borrow));
            prop_assert_eq!(&buf[..], &buf_borrow[..]);
        }
    }

    proptest::proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn buf_iter_returns_parsed_logs(logs in arbitrary_logs(25)) {
            let_assert!(Ok(pool) = buffer_pool(1));

            // Append as many logs as possible.
            let mut count = 0;
            let mut buf = pool.take();
            for log in &logs {
                if !log.write(&mut buf) {
                    break;
                }

                count += 1;
            }

            // Make sure those logs can be iterated.
            let appended = logs.split_at(count).0;
            for (expected, log) in appended.iter().zip(buf.into_iter()) {
                prop_assert_eq!(expected, &log);
            }
        }
    }

    fn arbitrary_header() -> impl Strategy<Value = Header> {
        arbitrary_log().prop_map(|log| Header::from(&log))
    }

    fn buffer_pool(pool_size: u16) -> Result<BufPool> {
        Ok(BufPool::unregistered(PoolOptions {
            pool_size,
            buf_capacity: BUF_CAPACITY,
            huge_buf: false,
        })?)
    }
}
