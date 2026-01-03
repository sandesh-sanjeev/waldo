//! Contiguous sequence of logs in storage.

mod file;
mod index;

pub use {file::FileOpts, index::IndexOpts};

use crate::storage::action::{Action, ActionCtx, Append, AsyncIo, Query};
use crate::storage::page::{file::PageFile, index::PageIndex};
use crate::storage::queue::IoQueue;
use crate::{AppendError, IoAction, IoFixedFd, LogIter, QueryError};
use assert2::let_assert;
use std::{io, os::fd::RawFd, path::Path};

/// Options for a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageOptions {
    /// Maximum number of log records in a page.
    pub capacity: u64,

    /// Options for file backing a page.
    pub file_opts: FileOpts,

    /// Options for index backing a page.
    pub index_opts: IndexOpts,
}

/// A chunk of contiguous sequence of logs within storage.
///
/// Each page is backed by a file on disk, some pre-allocated memory for
/// log index entries along with some other bits of in-memory state.
///
/// A page can be uninitialized (just empty space), empty (no logs) or
/// filled with logs. An uninitialized page can be initialized at runtime.
///
/// A page has a pre-determined maximum size. When the page runs out of capacity,
/// it must be reset or rotated away to make space for new log records.
#[derive(Debug)]
pub(super) struct Page {
    id: u32,
    capacity: u64,
    file: PageFile,
    index: PageIndex,
    state: Option<PageState>,
}

impl Page {
    /// Read the current state of the file from disk.
    ///
    /// Note that this method truncates a file if corruption is detected at the
    /// tip of the file (which might end of up being the entire file).
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identity of the page.
    /// * `path` - Path to the home directory.
    /// * `opts` - Options for the page.
    pub(super) fn open<P: AsRef<Path>>(id: u32, path: P, opts: PageOptions) -> io::Result<Self> {
        let mut file = PageFile::open(path, opts.file_opts)?;
        let mut index = PageIndex::new(opts.index_opts);

        // Read from disk and build state.
        let mut count = 0;
        let mut offset = 0;
        let mut prev_seq_no = None;
        let mut after_seq_no = None;
        {
            // Get raw bytes held in the underlying file.
            let buf = file.mmap()?;

            // Iterate through all the log records in the file.
            for log in LogIter(&buf) {
                // Make sure no corruption.
                if log.validate_data().is_err() {
                    break;
                }

                // Initialize starting sequence number for the page.
                if after_seq_no.is_none() {
                    after_seq_no = Some(log.prev_seq_no());
                    prev_seq_no = Some(log.prev_seq_no());
                }

                // Validate sequence for the log record.
                if prev_seq_no != Some(log.prev_seq_no()) {
                    break;
                }

                // Update state to track the discovered log record.
                count += 1;
                prev_seq_no = Some(log.seq_no());
                offset += log.size() as u64;
                index.apply(&log, offset);
            }
        }

        // Trim the file till the end of last valid log record.
        file.truncate(offset)?;

        // If we found successful logs, build state with it.
        let mut state = None;
        if let (Some(after_seq_no), Some(prev_seq_no)) = (after_seq_no, prev_seq_no) {
            state = Some(PageState {
                count,
                after_seq_no,
                prev_seq_no,
                resetting: false,
            });
        };

        // Return the newly opened page.
        Ok(Self {
            id,
            file,
            index,
            state,
            capacity: opts.capacity,
        })
    }

    /// Raw file descriptor to the underlying file.
    pub(super) fn raw_fd(&self) -> RawFd {
        self.file.raw_fd()
    }

    /// Set registered file in io-uring runtime.
    ///
    /// # Arguments
    ///
    /// * `file` - File registered in io-uring runtime.
    pub(super) fn set_io_file(&mut self, file: IoFixedFd) {
        self.file.set_io_file(file);
    }

    /// Returns metadata of the page, if page is initialized.
    pub(super) fn metadata(&self) -> Option<PageMetadata> {
        let state = self.state.as_ref()?;
        let file_state = self.file.state();
        let index_state = self.index.state();
        Some(PageMetadata {
            count: state.count,
            index_count: index_state.len as _,
            prev_seq_no: state.prev_seq_no,
            after_seq_no: state.after_seq_no,
            file_size: file_state.offset,
            index_size: (index_state.len * PageIndex::ENTRY_SIZE) as _,
        })
    }

    /// Returns true if the page is already initialized, false otherwise.
    pub(super) fn is_initialized(&self) -> bool {
        self.state.is_some()
    }

    /// Initialize a page.
    ///
    /// # Arguments
    ///
    /// * `after_seq_no` - Sequence number of the immediately previous log.
    pub(super) fn initialize(&mut self, after_seq_no: u64) {
        // Make sure page is not already initialized.
        let_assert!(None = &self.state);

        // Initialize the page with the new starting state.
        self.state.replace(PageState::new(after_seq_no));
    }

    /// Append a list of logs records into the page.
    ///
    /// If the page is initialized, logs are appended in the right sequence and there is
    /// no pending append or resets in page, then this initiates an async action to write
    /// a range of bytes into the underlying file.
    ///
    /// Note that this method returns false if the action could not be issued because the
    /// page ran out of space. When this happens space must be reclaimed from the oldest
    /// page to make room for the new append.
    ///
    /// # Arguments
    ///
    /// * `append` - Append action against the page.
    /// * `queue` - I/O queue to append I/O actions.
    pub(super) fn append(&mut self, append: Append, queue: &mut IoQueue) -> bool {
        let Append { buf, tx } = append;

        // Something is wrong if an action is routed to an uninitialized page.
        let Some(state) = self.state.as_ref().copied() else {
            tx.send_buf(buf, Err(AppendError::Fault("Append routed to uninitialized page")));
            return true;
        };

        // Return early if there is nothing to append.
        if buf.is_empty() {
            tx.send_buf(buf, Ok(()));
            return true;
        }

        // Return early if there is already an append in progress.
        // In theory we can support pipelining of writes, but it ain't simple.
        let pending_io = self.file.pending();
        if pending_io.append {
            tx.send_buf(buf, Err(AppendError::Conflict));
            return true;
        }

        // Re-queue the action if the page is currently being reset.
        // Once reset and initialization is complete, this page can accept logs.
        if pending_io.reset || state.resetting {
            queue.reprocess(Action::Append(Append { buf, tx }));
            return true;
        }

        // Perform validations on log records being appended.
        let mut error = None;
        let mut count = state.count;
        let mut is_overflow = false;
        let mut prev_seq_no = state.prev_seq_no;
        let mut file_state = self.file.state();
        let mut index_state = self.index.state();
        for log in &buf {
            // Make sure sequence of appended logs is not broken.
            if log.prev_seq_no() != prev_seq_no {
                error = Some(AppendError::Sequence(log.seq_no(), log.prev_seq_no(), prev_seq_no));
                break;
            }

            // Check if the log record caused the page to overflow.
            let size = log.size();
            if file_state.is_overflow(size) || index_state.is_overflow(&log) || count >= self.capacity {
                is_overflow = true;
                break;
            }

            // Consume the log record.
            count += 1;
            prev_seq_no = log.seq_no();
            index_state.apply(&log);
            file_state.apply(size);
        }

        // Return early if sequence validation failed.
        if let Some(error) = error {
            tx.send_buf(buf, Err(error));
            return true;
        }

        // Return early if there is no complete log in the buffer.
        if prev_seq_no == state.prev_seq_no {
            tx.send_buf(buf, Ok(()));
            return true;
        }

        // Re-queue the action and indicate that page does not have enough capacity.
        if is_overflow {
            queue.reprocess(Action::Append(Append { buf, tx }));
            return false;
        }

        // Enqueue I/O action for execution asynchronously.
        queue.issue(AsyncIo {
            id: self.id,
            ctx: ActionCtx::append(tx),
            action: self.file.append(buf),
        });
        true
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
    pub(super) fn query(&mut self, query: Query, queue: &mut IoQueue) {
        let Query {
            after_seq_no,
            mut buf,
            tx,
        } = query;

        // Something is wrong if an action is routed to an uninitialized page.
        let Some(state) = self.state.as_ref().copied() else {
            tx.send_buf(buf, Err(QueryError::Fault("Query routed to uninitialized page")));
            return;
        };

        // Return early if the query was incorrectly routed to this page.
        if after_seq_no < state.after_seq_no {
            tx.send_clear_buf(buf, Err(QueryError::Fault("Query routed to wrong page")));
            return;
        }

        // Return early if the page doesn't contain the requested range of logs.
        if after_seq_no >= state.prev_seq_no {
            tx.send_clear_buf(buf, Ok(()));
            return;
        }

        // We can fail the request saying requested log range is trimmed,
        // because once reset completes, this page won't contain requested logs.
        // There is an assumption here that rest will succeed. If that assumption
        // is violated users might observe a log range that has been reported to
        // have been trimmed. For that reason, we'll act as though there are no
        // new logs. That way, users know logs are trimmed the "regular" way.
        let pending_io = self.file.pending();
        if pending_io.reset || state.resetting {
            tx.send_clear_buf(buf, Ok(()));
            return;
        }

        // Find index to the closet log record that has seq_no <= after.
        let offset = self.index.offset(after_seq_no);

        // Do not attempt to read beyond known end of file.
        let file_len = self.file.state().offset;
        let remaining = file_len.saturating_sub(offset);
        let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);

        // Return early if there are no bytes to read.
        if remaining == 0 || buf.capacity() == 0 {
            tx.send_clear_buf(buf, Ok(()));
            return;
        }

        // Make enough space in buffer for new bytes.
        let len = std::cmp::min(remaining, buf.capacity());
        buf.set_len(len);

        // Enqueue I/O action for execution asynchronously.
        queue.issue(AsyncIo {
            id: self.id,
            ctx: ActionCtx::query(tx),
            action: self.file.query(offset, buf),
        });
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
    pub(super) fn reset(&mut self, queue: &mut IoQueue) {
        // No need to reset a page that is already empty.
        let Some(mut state) = self.state.as_ref().copied() else {
            return;
        };

        // Return early if there is already a pending reset.
        let pending_io = self.file.pending();
        if pending_io.reset {
            return;
        }

        // Store in-memory state that says page reset has begun.
        // This allows us to reject new actions while gracefully,
        // completing pending actions.
        state.resetting = true;
        self.state = Some(state);

        // If there is any pending I/O in the page, it cannot be reset.
        // The worker will just have to retry the operation after all the
        // pending I/O on this page have been drained.
        if pending_io.has_pending() {
            return;
        }

        // Enqueue I/O action for execution asynchronously.
        queue.issue(AsyncIo {
            id: self.id,
            ctx: ActionCtx::Reset,
            action: self.file.clear(),
        });
    }

    /// Process successfully completed async actions.
    ///
    /// # Arguments
    ///
    /// * `result` - Result of the I/O operation.
    /// * `action` - I/O action completed.
    /// * `ctx` - Context associated with the action.
    /// * `queue` - Queue to append I/O actions.
    pub(super) fn apply(&mut self, result: u32, mut action: IoAction, ctx: ActionCtx, queue: &mut IoQueue) {
        // Read latest state of the page.
        let_assert!(Some(state) = self.state.as_mut());

        // First complete the pending file I/O.
        let mut offset = self.file.state().offset;
        let file_result = self.file.apply(result, &mut action);

        // Handle commit for different types of actions.
        match ctx {
            ActionCtx::Fsync => {
                let_assert!(Ok(_) = file_result);
            }

            ActionCtx::Reset => {
                let_assert!(Ok(_) = file_result);
                self.index.clear();
                self.state = None;

                // Initiate a sync to disk.
                // Makes sure page reset is durably stored to disk.
                queue.issue(AsyncIo {
                    id: self.id,
                    ctx: ActionCtx::Fsync,
                    action: self.file.fsync(),
                });
            }

            ActionCtx::Query { tx, .. } => {
                let_assert!(Some(buf) = action.take_buf());
                if let Err(error) = file_result {
                    tx.send_clear_buf(buf, Err(error.into()));
                } else {
                    tx.send_buf(buf, Ok(()));
                }
            }

            ActionCtx::Append { tx } => {
                let_assert!(Some(buf) = action.take_buf());
                if let Err(error) = file_result {
                    tx.send_buf(buf, Err(error.into()));
                    return;
                }

                // Update state with committed logs.
                for log in &buf {
                    offset += log.size() as u64;
                    state.count += 1;
                    state.prev_seq_no = log.seq_no();
                    self.index.apply(&log, offset);
                }

                // Return result of the action.
                tx.send_buf(buf, Ok(()));
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
    /// * `queue` - Queue to append I/O actions.
    pub(super) fn abort(&mut self, error: io::Error, mut action: IoAction, ctx: ActionCtx, _queue: &mut IoQueue) {
        // Read latest state of the page.
        let_assert!(Some(state) = self.state.as_mut());

        // First complete the pending file I/O.
        self.file.abort(&mut action);

        // Handle abort for different types of actions.
        match ctx {
            ActionCtx::Fsync => {
                eprintln!("Page fsync aborted with error: {error:?}");
            }

            ActionCtx::Reset => {
                state.resetting = false;
                eprintln!("Page reset aborted with error: {error:?}");
            }

            ActionCtx::Query { tx, .. } => {
                let_assert!(Some(buf) = action.take_buf());
                tx.send_clear_buf(buf, Err(error.into()));
            }

            ActionCtx::Append { tx, .. } => {
                let_assert!(Some(buf) = action.take_buf());
                tx.send_buf(buf, Err(error.into()));
            }
        }
    }

    /// Reset page.
    ///
    /// Note that this method executes blocking I/O synchronously.
    pub(super) fn clear(&mut self) -> io::Result<()> {
        // If the page is not currently cleared, nothing else to do.
        if self.state.is_some() {
            return Ok(());
        }

        // Truncate file and reset underlying state.
        self.file.truncate(0)?;
        self.index.clear();
        self.state = None;
        Ok(())
    }

    /// Initiate graceful shutdown of this page.
    pub(super) fn close(self) -> io::Result<()> {
        self.file.close()
    }
}

/// Metadata associated with a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PageMetadata {
    pub(super) count: u64,
    pub(super) index_count: u64,
    pub(super) prev_seq_no: u64,
    pub(super) after_seq_no: u64,
    pub(super) file_size: u64,
    pub(super) index_size: u64,
}

/// State of a storage page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageState {
    count: u64,
    prev_seq_no: u64,
    after_seq_no: u64,
    resetting: bool,
}

impl PageState {
    fn new(after_seq_no: u64) -> Self {
        Self {
            count: 0,
            after_seq_no,
            resetting: false,
            prev_seq_no: after_seq_no,
        }
    }
}
