//! A sparse index to offsets of log records in a page.

use crate::log::Log;

/// Options for sparse index backing a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct IndexOptions {
    /// Maximum number of entries to index.
    pub(super) capacity: usize,

    /// Maximum amount of logs between index entries.
    pub(super) sparse_count: usize,

    /// Maximum amount of bytes between index entries.
    pub(super) sparse_bytes: usize,
}

/// A sparse index to log record offsets on a page file.
///
/// Once constructed the sparse index performs no allocations.
/// Given the index is append only, once it runs out of space,
/// must be rotated away and cleared to reclaim memory.
#[derive(Debug)]
pub(super) struct PageIndex {
    state: IndexState,
    entries: Vec<IndexEntry>,
}

impl PageIndex {
    /// Size of a single entry stored in a page index.
    pub(super) const ENTRY_SIZE: usize = size_of::<IndexEntry>();

    /// Create a new sparse index.
    ///
    /// # Arguments
    ///
    /// * `opts` - Options to customize the index.
    pub(super) fn new(opts: IndexOptions) -> Self {
        Self {
            state: IndexState::new(opts),
            entries: Vec::with_capacity(opts.capacity),
        }
    }

    /// Current state of the index.
    pub(super) fn state(&self) -> IndexState {
        self.state
    }

    /// Best offset to begin query.
    ///
    /// Note that even though this method always returns an offset,
    /// associated range of sequence numbers might or might not exist.xw
    ///
    /// # Arguments
    ///
    /// * `after_seq_no` - Sequence number of the a log record.
    pub(super) fn offset(&self, after_seq_no: u64) -> u64 {
        match self
            .entries
            .binary_search_by_key(&after_seq_no, IndexEntry::after_seq_no)
        {
            // Index doesn't exist, if it did, would exist at beginning.
            // Query from beginning of the page.
            Err(0) => 0,

            // Found exact match start at.
            Ok(i) => self.entries[i].offset(),

            // Exact match would have been at this index, but there is no exact match.
            // Everything before has seq_no < after and everything after has > seq_no.
            // So we pick the previous one and skip tasks that are not part of this query.
            Err(i) => self.entries[i - 1].offset(),
        }
    }

    /// Apply a log record into the index.
    ///
    /// # Arguments
    ///
    /// * `log` - Log record to apply into the index.
    /// * `offset` - Offset to end of log record on file.
    pub(super) fn apply(&mut self, log: &Log<'_>, offset: u64) {
        if self.state.apply(log) {
            self.entries.push(IndexEntry::new(offset, log.seq_no()));
        }
    }

    /// Clear all indexed entries.
    pub(super) fn clear(&mut self) {
        self.state.clear();
        self.entries.clear();
    }
}

/// Current state of a sparse index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct IndexState {
    pub(super) len: usize,
    pub(super) opts: IndexOptions,
    pub(super) count_since: usize,
    pub(super) bytes_since: usize,
}

impl IndexState {
    fn new(opts: IndexOptions) -> Self {
        Self {
            opts,
            len: 0,
            count_since: 0,
            bytes_since: 0,
        }
    }

    /// Check if applying the log will overflow the index.
    ///
    /// # Arguments
    ///
    /// * `log` - Log record to apply to index.
    pub(super) fn is_overflow(&self, log: &Log<'_>) -> bool {
        let count_since = self.count_since + 1;
        let bytes_since = self.bytes_since + log.size();

        let within_count = count_since <= self.opts.sparse_count;
        let within_bytes = bytes_since <= self.opts.sparse_bytes;

        // If no index entry needs to be created, no overflow can occur.
        if within_count && within_bytes {
            return false;
        }

        self.len >= self.opts.capacity
    }

    /// Apply log record to index state.
    ///
    /// Returns true if the log record must be indexed, false otherwise.
    ///
    /// # Arguments
    ///
    /// * `log` - Log record to apply to index state.
    pub(super) fn apply(&mut self, log: &Log<'_>) -> bool {
        self.count_since += 1;
        self.bytes_since += log.size();

        // Check if log must be indexed.
        let within_count = self.count_since <= self.opts.sparse_count;
        let within_bytes = self.bytes_since <= self.opts.sparse_bytes;

        // Nothing to do if the log doesn't have to be indexed.
        if within_count && within_bytes {
            return false;
        }

        // If the previous record was indexed, reset state for next.
        self.len += 1;
        self.count_since = 0;
        self.bytes_since = 0;
        true
    }

    /// Clear the accumulated state.
    pub(super) fn clear(&mut self) {
        self.len = 0;
        self.count_since = 0;
        self.bytes_since = 0;
    }
}

/// An indexed log record with associated offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexEntry {
    after_seq_no: u64,
    offset: u64,
}

impl IndexEntry {
    fn new(offset: u64, after_seq_no: u64) -> Self {
        Self { offset, after_seq_no }
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn after_seq_no(&self) -> u64 {
        self.after_seq_no
    }
}
