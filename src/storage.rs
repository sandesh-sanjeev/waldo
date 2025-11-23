//! Asynchronous storage engine for sequenced log records.

mod schema;

use crate::runtime::IoFile;

pub struct Storage {
    index: usize,
    ring: Vec<Segment>,
}

struct Segment {
    // A file registered to the I/O runtime.
    file: IoFile,
    file_len: u64,

    // Index associated with the segment.
    // Allows for efficient operations against a segment.
    count: u64,
    prev_seq_no: u64,
    index: Vec<Entry>,
}

/// Index to a log record in a segment.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct Entry {
    seq_no: u64,
    offset: u64,
}
