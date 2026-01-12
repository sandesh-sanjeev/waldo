//! A queue to buffer and schedule user/system actions.

use super::action::{Action, AsyncIo};
use std::collections::VecDeque;

/// A queue of asynchronous actions tracked in storage.
#[derive(Debug)]
pub(super) struct IoQueue {
    // Async actions initiated by one or more pages.
    issued: VecDeque<AsyncIo>,

    // Actions that have been queued for processing.
    queued: VecDeque<Action>,

    // Actions that were processed by a page, but must be reprocessed
    // because another thing has to happen. This will be re-processed
    // later as the scheduler sees fit.
    reprocess: VecDeque<Action>,
}

impl IoQueue {
    /// Create a new I/O queue.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Initially allocated capacity of the queue.
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            queued: VecDeque::with_capacity(capacity),
            issued: VecDeque::with_capacity(capacity),
            reprocess: VecDeque::with_capacity(capacity),
        }
    }

    /// Total number of I/O actions currently queued.
    pub(super) fn len(&self) -> usize {
        self.queued.len() + self.issued.len() + self.reprocess.len()
    }

    /// True if there is nothing queued, false otherwise.
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True if there are issued io actions.
    pub(super) fn is_empty_issued(&self) -> bool {
        self.issued.is_empty()
    }

    /// Issue a new page action.
    ///
    /// # Arguments
    ///
    /// * `action` - Action to issue.
    pub(super) fn issue(&mut self, action: AsyncIo) {
        self.issued.push_back(action);
    }

    /// Re-issue a previously issued page action.
    ///
    /// This allows the action to be retried immediately.
    ///
    /// # Arguments
    ///
    /// * `action` - Action to re-issue.
    pub(super) fn reissue(&mut self, action: AsyncIo) {
        self.issued.push_front(action);
    }

    /// Dequeue an issued page action.
    pub(super) fn pop_issued(&mut self) -> Option<AsyncIo> {
        self.issued.pop_front()
    }

    /// Enqueue an action to be processed.
    ///
    /// # Arguments
    ///
    /// * `action` - Action to enqueue for processing.
    pub(super) fn queue(&mut self, action: Action) {
        self.queued.push_back(action);
    }

    /// Dequeue an action for processing.
    pub(super) fn pop_queued(&mut self) -> Option<Action> {
        self.queued.pop_front()
    }

    /// Enqueue a storage action for reprocessing later.
    ///
    /// # Arguments
    ///
    /// * `action` - Action to enqueue.
    pub(super) fn reprocess(&mut self, action: Action) {
        self.reprocess.push_back(action);
    }

    /// Dequeue a pending storage action for reprocessing.
    pub(super) fn pop_reprocess(&mut self) -> Option<Action> {
        self.reprocess.pop_front()
    }
}
