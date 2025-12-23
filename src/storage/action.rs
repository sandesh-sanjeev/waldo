//! Asynchronous actions initiated in Storage.

use crate::{
    runtime::{IoAction, IoBuf},
    storage::{AppendError, BufResult, QueryError},
};
use std::collections::VecDeque;

/// Type alias for sender of action result that have shared buffer(s).
pub(super) type BufSender<T, E> = FateSender<BufResult<T, E>>;

/// An async I/O action initiated from a page.
#[derive(Debug)]
pub(super) struct PageIo {
    pub(super) id: u32,
    pub(super) ctx: ActionCtx,
    pub(super) action: IoAction,
}

/// A queue of asynchronous actions tracked in storage.
#[derive(Debug)]
pub(super) struct IoQueue {
    // Async actions initiated by one or more pages.
    issued: VecDeque<PageIo>,

    // Pending actions that have not yet been processed
    // by any of the pages. It could be due to retries,
    // scheduling, etc.
    pending: VecDeque<Action>,
}

impl IoQueue {
    /// Create a new I/O queue.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Initially allocated capacity of the queue.
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            issued: VecDeque::with_capacity(capacity),
            pending: VecDeque::with_capacity(capacity),
        }
    }

    /// Total number of I/O actions currently queued.
    pub(super) fn len(&self) -> usize {
        self.issued.len() + self.pending.len()
    }

    /// True if there is nothing queued, false otherwise.
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Issue a new page action.
    ///
    /// # Arguments
    ///
    /// * `action` - Action to issue.
    pub(super) fn issue(&mut self, action: PageIo) {
        self.issued.push_back(action);
    }

    /// Re-issue a previously issued page action.
    ///
    /// This allows the action to be retried immediately.
    ///
    /// # Arguments
    ///
    /// * `action` - Action to re-issue.
    pub(super) fn reissue(&mut self, action: PageIo) {
        self.issued.push_front(action);
    }

    /// Dequeue an issued page action.
    pub(super) fn dequeue_issued(&mut self) -> Option<PageIo> {
        self.issued.pop_front()
    }

    /// Enqueue a storage action.
    ///
    /// # Arguments
    ///
    /// * `action` - Action to enqueue.
    pub(super) fn pending(&mut self, action: Action) {
        self.pending.push_back(action);
    }

    /// Dequeue a pending storage action.
    pub(super) fn dequeue_pending(&mut self) -> Option<Action> {
        self.pending.pop_front()
    }
}

/// Different types of user initiated actions against storage.
#[derive(Debug)]
pub(super) enum Action {
    /// Action to query from storage.
    Query(Query),

    /// Action to append into storage.
    Append(Append),
}

/// A request to query for some bytes from page.
#[derive(Debug)]
pub(super) struct Query {
    /// Buffer shared with storage.
    pub(super) buf: IoBuf,

    /// Query for logs after this sequence number.
    pub(super) after_seq_no: u64,

    /// Sender to send action result asynchronously.
    pub(super) tx: BufSender<(), QueryError>,
}

/// A request to append some bytes into page.
#[derive(Debug)]
pub(super) struct Append {
    /// Buffer shared with storage.
    pub(super) buf: IoBuf,

    /// Sender to send action result asynchronously.
    pub(super) tx: BufSender<(), AppendError>,
}

/// Context associated with a pending storage action.
#[derive(Debug)]
pub(super) enum ActionCtx {
    /// Context associated with a query action.
    Query {
        /// Sender to send action result asynchronously.
        tx: BufSender<(), QueryError>,
    },

    /// Context associated with a append action.
    Append {
        /// Sender to send action result asynchronously.
        tx: BufSender<(), AppendError>,
    },

    /// System action to clear a page.
    Reset,
}

impl ActionCtx {
    /// Create context for a query action.
    ///
    /// # Arguments
    ///
    /// * `tx` - Sender for result of the action.
    pub(super) fn query(tx: BufSender<(), QueryError>) -> ActionCtx {
        ActionCtx::Query { tx }
    }

    /// Create context for an append action.
    ///
    /// # Arguments
    ///
    /// * `tx` - Sender for result of the action.
    pub(super) fn append(tx: BufSender<(), AppendError>) -> ActionCtx {
        ActionCtx::Append { tx }
    }
}

/// Error when fate of a storage action is lost.
#[derive(Debug, thiserror::Error)]
#[error("Fate of async operation was lost")]
pub(super) struct FateError;

/// A oneshot channel to notify fate of a storage action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AsyncFate;

impl AsyncFate {
    /// A new channel to send results of a storage action.
    pub(super) fn channel<T>() -> (FateSender<T>, FateReceiver<T>) {
        let (tx, rx) = oneshot::channel();
        (FateSender(tx), FateReceiver(rx))
    }
}

/// Sender to send fate of a storage action.
#[derive(Debug)]
pub(super) struct FateSender<T>(oneshot::Sender<T>);

impl<T, E> FateSender<BufResult<T, E>> {
    /// Send fate of an async operation.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer shared storage.
    /// * `value` - Result of the storage action.
    pub(super) fn send(self, buf: IoBuf, value: Result<T, E>) {
        if let Err(message) = self.0.send((buf, value)) {
            let (buf, _) = message.into_inner();
            drop(buf); // Explicitly return to pool, just cause.
        }
    }
}

/// Receiver for fate of a storage action.
#[derive(Debug)]
pub(super) struct FateReceiver<T>(oneshot::Receiver<T>);

impl<T> FateReceiver<T> {
    /// Receive fate of an async operation asynchronously.
    pub(super) async fn recv_async(self) -> Result<T, FateError> {
        self.0.await.map_err(|_| FateError)
    }
}
