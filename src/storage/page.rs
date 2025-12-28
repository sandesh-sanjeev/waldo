//! Contiguous sequence of logs in storage.

use crate::{
    log::Log,
    runtime::{IoAction, IoFixedFd},
    storage::{
        PageOptions,
        action::{Action, ActionCtx, Append, IoQueue, PageIo, Query},
        session::{AppendError, QueryError},
    },
};
use std::io;

/// A chunk of contiguous sequence of logs within storage.
///
/// Each page is backed by a file on disk, some pre-allocated memory for
/// log index entries along with some other bits of in-memory state.
///
/// A page can be uninitialized (just empty space), empty (no logs) or
/// filled with logs. An uninitialized page can be initialized at runtime.
///
/// A page has a pre-determined maximum size. Then the page runs out of capacity,
/// it must be reset or rotated away to make space for new log records.
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

    // Configuration for the page.
    opts: PageOptions,
}

impl Page {
    /// Create a new empty page.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identity of the page.
    /// * `file` - Handle to the file backing this page.
    /// * `opts` - Options for the page.
    pub(super) fn new_empty(id: u32, file: IoFixedFd, opts: PageOptions) -> Self {
        Self {
            id,
            file,
            opts,
            state: None,
            index: Vec::with_capacity(opts.index_capacity),
        }
    }

    /// Returns true if the page is already initialized, false otherwise.
    pub(super) fn is_initialized(&self) -> bool {
        self.state.is_some()
    }

    /// Returns current state of page if page is initialized, empty otherwise.
    pub(super) fn state(&self) -> Option<PageState> {
        self.state.as_ref().copied()
    }

    /// (Re)Initialize a page.
    ///
    /// # Arguments
    ///
    /// * `after_seq_no` - Sequence number of the immediately previous log.
    pub(super) fn initialize(&mut self, after_seq_no: u64) -> bool {
        // Make sure page is not already initialized.
        if self.state.is_some() {
            return false;
        }

        // Initialize the page with the new starting state.
        self.state.replace(PageState::new(after_seq_no));
        true
    }

    /// Append a list of logs records into the page.
    ///
    /// If the page is initialized, logs are appended in the right sequence and there is
    /// no pending append or resets in page, then this initiates an async action to write
    /// a range of bytes into the underlying file.
    ///
    /// Note that this method returns [`ActionIo::Overflow`] if the action could not be
    /// issued because the page ran out of space. When this happens space must be reclaimed
    /// from the oldest page to make room for the new append.
    ///
    /// # Arguments
    ///
    /// * `append` - Append action against the page.
    /// * `queue` - I/O queue to append I/O actions.
    pub(super) fn append(&mut self, append: Append, queue: &mut IoQueue) -> ActionIo {
        let mut state = self.assert_load_state();
        let Append { buf, tx } = append;

        // Return early if there is nothing to append.
        if buf.is_empty() {
            tx.send(buf, Ok(()));
            return ActionIo::Completed;
        }

        // Return early if there is already an append in progress.
        // In theory we can support pipelining of writes, but it ain't simple.
        if state.pending.append {
            tx.send(buf, Err(AppendError::Conflict));
            return ActionIo::Completed;
        }

        // Re-queue the action if the page is currently being reset.
        // Once reset and initialization is complete, this page can accept logs.
        if state.pending.reset || state.resetting {
            queue.reprocess(Action::Append(Append { buf, tx }));
            return ActionIo::Pending;
        }

        // Perform validations on log records being appended.
        let mut error = None;
        let mut index_count = self.index.len();
        let mut scratch = state;
        for log in &buf {
            // Make sure sequence of appended logs is not broken.
            if log.prev_seq_no() != scratch.prev_seq_no {
                error = Some(AppendError::Sequence(
                    log.seq_no(),
                    log.prev_seq_no(),
                    scratch.prev_seq_no,
                ));
                break;
            }

            // For capacity calculations.
            if scratch.apply(&log, self.opts).is_some() {
                index_count += 1;
                scratch.index.reset();
            }
        }

        // Return early if sequence validation failed.
        if let Some(error) = error {
            tx.send(buf, Err(error));
            return ActionIo::Completed;
        }

        // Return early if there is no complete log in the buffer.
        if scratch.prev_seq_no == state.prev_seq_no {
            tx.send(buf, Ok(()));
            return ActionIo::Completed;
        }

        // Re-queue the action and indicate that page does not have enough capacity.
        if scratch.count > self.opts.page_capacity
            || scratch.offset > self.opts.file_capacity
            || index_count > self.opts.index_capacity
        {
            queue.reprocess(Action::Append(Append { buf, tx }));
            return ActionIo::Overflow;
        }

        // Track the in-progress query.
        state.pending.append = true;
        self.state = Some(state);

        // Enqueue I/O action for execution asynchronously.
        queue.issue(PageIo {
            id: self.id,
            ctx: ActionCtx::append(tx),
            action: IoAction::write_at(self.file, state.offset, buf),
        });
        ActionIo::Issued
    }

    /// Query for a range of log records from page.
    ///
    /// If the page is initialized, contains requested range of logs and is not
    /// currently being reset, then this initiates an async action to read a range
    /// of bytes from underlying file.
    ///
    /// # Arguments
    ///
    /// * `query` - Query action against the page.
    /// * `queue` - I/O queue to append I/O actions.
    pub(super) fn query(&mut self, query: Query, queue: &mut IoQueue) -> ActionIo {
        let mut state = self.assert_load_state();
        let Query {
            after_seq_no: after,
            mut buf,
            tx,
        } = query;

        // Return early if the query was incorrectly routed to this page.
        if after < state.after_seq_no {
            buf.clear(); // Make sure to reflect no read.
            tx.send(buf, Err(QueryError::Fault("Query routed to wrong page")));
            return ActionIo::Completed;
        }

        // Return early if the page doesn't contain the requested range of logs.
        if after >= state.prev_seq_no {
            buf.clear(); // Make sure to reflect no read.
            tx.send(buf, Ok(()));
            return ActionIo::Completed;
        }

        // We can fail the request saying requested log range is trimmed,
        // because once reset completes, this page won't contain requested logs.
        // There is an assumption here however that it is illegal for a reset to fail.
        if state.pending.reset || state.resetting {
            buf.clear(); // Make sure to reflect no read.
            tx.send(buf, Err(QueryError::Trimmed(after)));
            return ActionIo::Completed;
        }

        // Find index to the closet log record that has seq_no <= after.
        let offset = match self.index.binary_search_by_key(&after, IndexEntry::after_seq_no) {
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
            return ActionIo::Completed;
        }

        // Make enough space in buffer for new bytes.
        let len = std::cmp::min(remaining, buf.capacity());
        buf.set_len(len);

        // Track the in-progress query.
        state.pending.query += 1;
        self.state = Some(state);

        // Enqueue I/O action for execution asynchronously.
        queue.issue(PageIo {
            id: self.id,
            ctx: ActionCtx::query(tx),
            action: IoAction::read_at(self.file, offset, buf),
        });
        ActionIo::Issued
    }

    /// Reset page to empty.
    ///
    /// If the page is initialized and there are no pending I/O operations
    /// on the page, then this initiates an async action to truncate the
    /// underlying page to size zero.
    ///
    /// # Arguments
    ///
    /// * `queue` - Queue to append I/O actions.
    pub(super) fn reset(&mut self, queue: &mut IoQueue) -> ActionIo {
        // No need to reset a page that is already empty.
        let Some(mut state) = self.state.as_ref().copied() else {
            return ActionIo::Completed;
        };

        // Return early if there is already a pending reset.
        if state.pending.reset {
            return ActionIo::Completed;
        }

        // Store in-memory state that says page reset has begun.
        // This allows us to reject new actions while gracefully,
        // completing pending actions.
        state.resetting = true;

        // If there is any pending I/O in the page, it cannot be reset.
        // The worker will just have to retry the operation after all the
        // pending I/O on this page have been drained.
        if state.pending.has_pending() {
            return ActionIo::Pending;
        }

        // Track the in-progress reset.
        state.pending.reset = true;
        self.state = Some(state);

        // Enqueue I/O action for execution asynchronously.
        queue.issue(PageIo {
            id: self.id,
            ctx: ActionCtx::Reset,
            action: IoAction::resize(self.file, 0),
        });
        ActionIo::Issued
    }

    /// Process successfully completed async actions.
    ///
    /// # Arguments
    ///
    /// * `result` - Result of the I/O operation.
    /// * `action` - I/O action completed.
    /// * `ctx` - Context associated with the action.
    pub(super) fn commit(&mut self, result: u32, action: IoAction, ctx: ActionCtx) {
        let mut state = self.assert_load_state();
        match ctx {
            ActionCtx::Reset => {
                // Underlying file is already truncated.
                // We just need to reset all internal state.
                self.index.clear();
                self.state = None;
            }

            ActionCtx::Query { tx, .. } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");
                if result != buf.len() as u32 {
                    let error = QueryError::Fault("Incomplete read in a query action");
                    tx.send(buf, Err(error));
                    return;
                }

                // Stop tracking the completed query.
                state.pending.query -= 1;
                self.state = Some(state);

                // Return result of the action.
                tx.send(buf, Ok(()));
            }

            ActionCtx::Append { tx } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");
                if result != buf.len() as u32 {
                    let error = AppendError::Fault("Incomplete write in an append action");
                    tx.send(buf, Err(error));
                    return;
                }

                // Update in-memory state with things we wrote into file.
                for log in &buf {
                    if let Some(entry) = state.apply(&log, self.opts) {
                        self.index.push(entry);
                        state.index.reset();
                    }
                }

                // Stop tracking the completed action.
                state.pending.append = false;
                self.state = Some(state);

                // Return result of the action.
                tx.send(buf, Ok(()));
            }
        }
    }

    /// Process async actions that ended with an error.
    ///
    /// # Arguments
    ///
    /// * `error` - I/O error performing the operation.
    /// * `action` - I/O action that errored out.
    /// * `ctx` - Context associated with the action.
    pub(super) fn abort(&mut self, error: io::Error, action: IoAction, ctx: ActionCtx) {
        let mut state = self.assert_load_state();
        match ctx {
            ActionCtx::Reset => {
                state.resetting = false;
                state.pending.reset = false;
                self.state = Some(state);

                // FIXME: Should we abort all pending append actions?
                eprintln!("Page reset aborted with error: {error:?}");
            }

            ActionCtx::Query { tx, .. } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");

                // Stop tracking the aborted query.
                state.pending.query -= 1;
                self.state = Some(state);

                // Return error from the action.
                tx.send(buf, Err(error.into()));
            }

            ActionCtx::Append { tx, .. } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");

                // Stop tracking the aborted append.
                state.pending.append = false;
                self.state = Some(state);

                // Return error from the action.
                tx.send(buf, Err(error.into()));
            }
        }
    }

    fn assert_load_state(&self) -> PageState {
        self.state
            .as_ref()
            .copied()
            .expect("Loading state from an uninitialized page")
    }
}

/// Result from processing an action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActionIo {
    Issued,
    Pending,
    Overflow,
    Completed,
}

/// State of a storage page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PageState {
    pub(super) count: u64,
    pub(super) offset: u64,
    pub(super) prev_seq_no: u64,
    pub(super) after_seq_no: u64,
    pub(super) index: IndexState,
    pub(super) pending: PendingIo,
    pub(super) resetting: bool,
}

impl PageState {
    fn new(after_seq_no: u64) -> Self {
        Self {
            count: 0,
            offset: 0,
            after_seq_no,
            resetting: false,
            prev_seq_no: after_seq_no,
            index: IndexState::new(),
            pending: PendingIo::default(),
        }
    }

    fn apply(&mut self, log: &Log<'_>, opts: PageOptions) -> Option<IndexEntry> {
        // Another sanity check to be super extra sure.
        assert_eq!(log.prev_seq_no(), self.prev_seq_no);

        // Update state of the page.
        self.count += 1;
        self.prev_seq_no = log.seq_no();
        self.offset += log.size() as u64;

        // Next check if an index must be created.
        // This is because we index offset of log after indexed seq_no.
        let index = IndexEntry::new(self.offset, self.prev_seq_no);
        if self.index.apply(log, opts) { Some(index) } else { None }
    }
}

/// State of a page index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct IndexState {
    pub(super) count_since: usize,
    pub(super) bytes_since: usize,
}

impl IndexState {
    fn new() -> Self {
        Self {
            count_since: 0,
            bytes_since: 0,
        }
    }

    fn apply(&mut self, log: &Log<'_>, opts: PageOptions) -> bool {
        self.count_since += 1;
        self.bytes_since += log.size();
        self.index_prev(opts)
    }

    fn index_prev(&self, opts: PageOptions) -> bool {
        // Check if enough logs have accumulated.
        self.count_since > opts.index_sparse_count
        // Or if enough bytes have accumulated.
        || self.bytes_since > opts.index_sparse_bytes
    }

    fn reset(&mut self) {
        self.count_since = 0;
        self.bytes_since = 0;
    }
}

/// Status around pending I/O actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct PendingIo {
    pub(super) reset: bool,
    pub(super) query: u32,
    pub(super) append: bool,
}

impl PendingIo {
    fn has_pending(&self) -> bool {
        self.reset || self.append || self.query > 0
    }
}

/// Offset of an indexed sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexEntry {
    offset: u64,
    after_seq_no: u64,
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
