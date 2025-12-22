use crate::{
    log::Log,
    runtime::{IoAction, IoFixedFd},
    storage::{
        AppendError, Error, IndexOptions,
        action::{Action, ActionCtx, Append, Query},
    },
};
use std::{collections::VecDeque, io};

#[derive(Debug)]
pub(super) struct Page {
    // Index of the page in the ring buffer.
    id: u32,

    // File handle to the underlying file on disk.
    file: IoFixedFd,

    // Current state of the page.
    state: Option<PageState>,

    // A sparse index to log records stored in page.
    index: Vec<IndexEntry>,

    // Configuration for sparse index.
    index_opts: IndexOptions,
}

impl Page {
    const LOG_SIZE_LIMIT: usize = 1024 * 1024; // 1 MB.

    pub(super) fn new(id: u32, file: IoFixedFd, index_opts: IndexOptions) -> Self {
        Self {
            id,
            file,
            index_opts,
            state: None,
            index: Vec::with_capacity(index_opts.capacity),
        }
    }

    pub(super) fn initialize(&mut self, after_seq_no: u64) {
        let state = PageState::new(after_seq_no, self.index_opts);
        self.state.replace(state); // TODO: Make sure not already initialized.
    }

    /// Process an action.
    pub(super) fn action(&self, action: Action, queue: &mut VecDeque<PageIo>) {
        match action {
            Action::Query(query) => self.query(query, queue),
            Action::Append(append) => self.append(append, queue),
        };
    }

    /// Load current state of the page.
    pub(super) fn load_state(&self) -> PageState {
        *self
            .state
            .as_ref()
            .expect("State should not be loaded in uninitialized page")
    }

    /// Initialize a query action against the page.
    pub(super) fn query(&self, query: Query, queue: &mut VecDeque<PageIo>) {
        let state = self.load_state();
        let Query {
            after_seq_no: after,
            mut buf,
            tx,
        } = query;

        // Return early if the page doesn't contain the requested range of logs.
        if after >= state.prev_seq_no || after < state.after_seq_no {
            buf.clear(); // Make sure to reflect no read.
            tx.send(buf, Ok(()));
            return;
        }

        // Find index to the closet log record that has seq_no <= after.
        let offset = match self.index.binary_search_by_key(&after, IndexEntry::seq_no) {
            // Query from beginning of the page.
            Err(0) => 0,

            // Found exact match start at.
            Ok(i) => self.index[i].offset(),

            // Exact match would have been at this index, but there is no exact match.
            // Everything before has seq_no < after and everything after has > seq_no.
            // So we pick the previous one and skip tasks that are not part of this query.
            Err(i) => self.index[i - 1].offset(),
        };

        // Do not attempt to read beyond known end of file.
        let remaining = state.offset.saturating_sub(offset);
        let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);

        // Return early if there are no bytes to read.
        if remaining == 0 || buf.capacity() == 0 {
            buf.clear(); // Make sure to reflect no read.
            tx.send(buf, Ok(()));
            return;
        }

        // Make enough space in buffer for new bytes.
        let len = std::cmp::min(remaining, buf.capacity());
        buf.set_len(len);

        // Enqueue I/O action for execution asynchronously.
        queue.push_back(PageIo {
            id: self.id,
            ctx: ActionCtx::query(tx),
            action: IoAction::read_at(self.file, offset, buf),
        });
    }

    /// Initialize an append action against the page.
    pub(super) fn append(&self, append: Append, queue: &mut VecDeque<PageIo>) {
        let state = self.load_state();
        let Append { buf, tx } = append;

        // TODO: Handle the case where a page is full.

        // Return early if there is nothing to append.
        if buf.is_empty() {
            tx.send(buf, Ok(()));
            return;
        }

        // Perform validations on log records being appended.
        let mut error = None;
        let mut prev_seq_no = state.prev_seq_no;
        for log in &buf {
            // Make sure sequence of appended logs is not broken.
            if log.prev_seq_no() != prev_seq_no {
                error = Some(AppendError::Sequence(log.seq_no(), prev_seq_no, log.prev_seq_no()));
                break;
            }

            // Make sure log record does not exceed limits.
            if log.size() > Self::LOG_SIZE_LIMIT {
                error = Some(AppendError::ExceedsLimit(
                    log.seq_no(),
                    log.size(),
                    Self::LOG_SIZE_LIMIT,
                ));
                break;
            }

            // Update for next iteration.
            prev_seq_no = log.seq_no();
        }

        // Return early if sequence validation failed.
        if let Some(error) = error {
            tx.send(buf, Err(error));
            return;
        }

        // Return early if there is no complete log in the buffer.
        if prev_seq_no == state.prev_seq_no {
            tx.send(buf, Ok(()));
            return;
        }

        // Enqueue I/O action for execution asynchronously.
        queue.push_back(PageIo {
            id: self.id,
            ctx: ActionCtx::append(tx),
            action: IoAction::write_at(self.file, state.offset, buf),
        });
    }

    pub(super) fn commit(&mut self, result: u32, action: IoAction, ctx: ActionCtx) {
        match ctx {
            ActionCtx::Query { tx, .. } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");

                // Make sure all the bytes were read.
                if result != buf.len() as u32 {
                    let error = io::Error::new(io::ErrorKind::UnexpectedEof, "Incomplete read");
                    tx.send(buf, Err(Error::from(error).into()));
                    return;
                }

                // Return the buffer back.
                tx.send(buf, Ok(()));
            }

            ActionCtx::Append { tx } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");

                // Make sure all bytes written.
                if result != buf.len() as u32 {
                    let error = io::Error::new(io::ErrorKind::UnexpectedEof, "Incomplete write");
                    tx.send(buf, Err(Error::from(error).into()));
                    return;
                }

                // Update in-memory state with things we wrote into file.
                let mut state = self.load_state();
                for log in &buf {
                    // Update local state with contents of the log record.
                    state.apply(&log);

                    // Check if this log record must be indexed.
                    if state.index.index_prev(self.index_opts) {
                        self.index.push(IndexEntry::new(state.offset, log.seq_no()));
                        state.index.reset();
                    }
                }

                // Update state and return success.
                self.state = Some(state);
                tx.send(buf, Ok(()));
            }
        }
    }

    pub(super) fn abort(&self, error: io::Error, action: IoAction, ctx: ActionCtx) {
        match ctx {
            ActionCtx::Query { tx, .. } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");

                // Return error from the action.
                tx.send(buf, Err(Error::from(error).into()));
            }

            ActionCtx::Append { tx, .. } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");

                // Return error from the action.
                tx.send(buf, Err(Error::from(error).into()));
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct PageIo {
    pub(super) id: u32,
    pub(super) ctx: ActionCtx,
    pub(super) action: IoAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PageState {
    pub(super) count: u64,
    pub(super) offset: u64,
    pub(super) prev_seq_no: u64,
    pub(super) after_seq_no: u64,
    pub(super) index: IndexState,
}

impl PageState {
    fn new(after_seq_no: u64, opts: IndexOptions) -> Self {
        Self {
            count: 0,
            offset: 0,
            after_seq_no,
            prev_seq_no: after_seq_no,
            index: IndexState::new(opts),
        }
    }

    fn apply(&mut self, log: &Log<'_>) {
        // Another sanity check to be super extra sure.
        assert_eq!(log.prev_seq_no(), self.prev_seq_no);

        // Update state of the index.
        self.index.apply(log);

        // Update state of the page.
        self.count += 1;
        self.prev_seq_no = log.seq_no();
        self.offset += log.size() as u64;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct IndexState {
    pub(super) count_since: usize,
    pub(super) bytes_since: usize,
}

impl IndexState {
    fn new(opts: IndexOptions) -> Self {
        Self {
            count_since: opts.sparse_count,
            bytes_since: opts.sparse_bytes,
        }
    }

    fn apply(&mut self, log: &Log<'_>) {
        self.count_since += 1;
        self.bytes_since += log.size();
    }

    fn index_prev(&self, opts: IndexOptions) -> bool {
        self.count_since > opts.sparse_count || self.bytes_since > opts.sparse_bytes
    }

    fn reset(&mut self) {
        self.count_since = 0;
        self.bytes_since = 0;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexEntry {
    offset: u64,
    seq_no: u64,
}

impl IndexEntry {
    fn new(offset: u64, seq_no: u64) -> Self {
        Self { offset, seq_no }
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn seq_no(&self) -> u64 {
        self.seq_no
    }
}
