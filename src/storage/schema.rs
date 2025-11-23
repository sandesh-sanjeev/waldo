//! Schema of data stored in persistent storage.

use crate::{
    log::{Error as LogError, Log, Result as LogResult},
    runtime::IoBuf,
};
use std::borrow::Cow;
use xxhash_rust::{const_xxh3::const_custom_default_secret, xxh3::xxh3_64_with_secret};

/// A marker trait for version of schema.
pub(crate) trait Version {
    /// An unsigned 64 bit unique version number.
    const VERSION: u64;
}

/// Version 1 of schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V1;

impl Version for V1 {
    const VERSION: u64 = 1;
}

/// A hasher to generate hashes of byte blobs.
pub(crate) trait Hasher
where
    Self: Version,
{
    /// Hash of the contents of an item.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Blob of bytes to hash.
    fn hash(bytes: &[u8]) -> u64;
}

impl Hasher for V1 {
    fn hash(bytes: &[u8]) -> u64 {
        // A secret derived from version number.
        // Efficient to generate secret during compile time.
        // TODO: Maybe we should really be using a magic (prime?) number.
        const SECRET: [u8; 192] = const_custom_default_secret(V1::VERSION);

        // Generate hash for the source bytes.
        xxh3_64_with_secret(bytes, &SECRET)
    }
}

/// Serializer used to perform versioned (de)serialize of logs.
pub(crate) trait LogSerializer<'a, V>
where
    V: Version,
    Self: Sized + 'a,
{
    /// Size of serialized item.
    ///
    /// # Arguments
    ///
    /// * `version` - Version of serializer.
    fn size(&self) -> usize;

    /// Serialize log bytes and append into buffer.
    ///
    /// Returns the number of bytes written into buffer.
    /// None if the buffer does not have enough bytes for the log.
    ///
    /// # Arguments
    ///
    /// * `dst` - Destination buffer to append item bytes.
    fn write(&self, dst: &mut IoBuf) -> Option<usize>;

    /// Parse next log in buffer.
    ///
    /// Returns an error bytes are corrupted, malformed or fail invariants.
    ///
    /// # Arguments
    ///
    /// * `src` - Source buffer to parse item from.
    fn read(src: &'a [u8]) -> Option<LogResult<Self>>;

    /// Parse next log in buffer without any integrity validations.
    ///
    /// Allows for better perf when parsing an already validated blob of bytes.
    ///
    /// # Arguments
    ///
    /// * `src` - Source buffer to parse item from.
    fn read_unchecked(src: &'a [u8]) -> Option<(Log<'a>, u64)>;
}

impl<'a> LogSerializer<'a, V1> for Log<'a> {
    fn size(&self) -> usize {
        size_of::<u64>() // seq_no
        + size_of::<u64>() // prev_seq_no
        + size_of::<u32>() // data_size
        + self.data().len() // actual data bytes
        + size_of::<u64>() // hash
    }

    fn write(&self, dst: &mut IoBuf) -> Option<usize> {
        // Fetch size of the log record.
        // Make sure buffer have enough space to hold the buffer.
        let log_size = u32::try_from(self.size()).ok()?;
        if dst.remaining() < log_size {
            return None;
        }

        // Starting length of the source buffer.
        let mut write_len = 0;

        // Append log header to buffer.
        write_len += dst.extend_from_slice(&self.seq_no.to_be_bytes())?;
        write_len += dst.extend_from_slice(&self.prev_seq_no.to_be_bytes())?;
        write_len += dst.extend_from_slice(&(self.data.len() as u32).to_be_bytes())?;

        // Actual log payload.
        write_len += dst.extend_from_slice(self.data())?;

        // Append hash for integrity checks.
        // That's the only footer for now.
        let hash = V1::hash(&dst[write_len..]);
        write_len += dst.extend_from_slice(&hash.to_be_bytes())?;

        // Return total number of bytes written into buffer.
        Some(write_len)
    }

    fn read(src: &'a [u8]) -> Option<LogResult<Self>> {
        // Safety: We'll perform the safety checks next :)
        let (log, hash) = Self::read_unchecked(src)?;

        // We have all the bytes, perform integrity check.
        let end = log.size() - size_of::<u64>();
        let bytes_hash = V1::hash(&src[..end]);
        if hash != bytes_hash {
            return Some(Err(LogError::MalformedBytes(log.seq_no(), hash, bytes_hash)));
        }

        // Return fully parsed and validated log record.
        Some(Log::new(log.seq_no, log.prev_seq_no, log.data))
    }

    fn read_unchecked(src: &'a [u8]) -> Option<(Log<'a>, u64)> {
        // Parse seq_no if enough bytes exist.
        let (seq_no_bytes, rest) = src.split_at_checked(size_of::<u64>())?;
        let seq_no = u64::from_be_bytes(seq_no_bytes.try_into().ok()?);

        // Parse prev_seq_no if enough bytes exist.
        let (prev_seq_no_bytes, rest) = rest.split_at_checked(size_of::<u64>())?;
        let prev_seq_no = u64::from_be_bytes(prev_seq_no_bytes.try_into().ok()?);

        // Parse data size if enough bytes exist.
        let (data_size_bytes, rest) = rest.split_at_checked(size_of::<u32>())?;
        let data_size = u32::from_be_bytes(data_size_bytes.try_into().ok()?);

        // Parse data if enough bytes exist.
        let (data, rest) = rest.split_at_checked(data_size as usize)?;

        // Parse hash if enough bytes exist.
        let (hash_bytes, _) = rest.split_at_checked(size_of::<u64>())?;
        let hash = u64::from_be_bytes(hash_bytes.try_into().ok()?);

        // Parse the entire un-validated logs.
        let log = Log {
            seq_no,
            prev_seq_no,
            data: Cow::Borrowed(data),
        };

        Some((log, hash))
    }
}
