//! Asynchronous actions initiated, processed and completed in Storage.

use crate::runtime::{IoAction, IoBuf};
use crate::{AppendError, Metadata, QueryError};
use std::io;

/// Type alias for results that involve I/O buffers.
type BufResult<T, E> = (IoBuf, Result<T, E>);

/// Type alias for sender of action result that have shared buffer(s).
pub(crate) type BufSender<T, E> = FateSender<BufResult<T, E>>;

/// Type alias for receiver of action result that have shared buffer(s).
pub(crate) type BufReceiver<T, E> = FateReceiver<BufResult<T, E>>;

/// An async I/O action initiated from a page.
#[derive(Debug)]
pub(crate) struct AsyncIo {
    pub(crate) id: u32,
    pub(crate) ctx: ActionCtx,
    pub(crate) action: IoAction,
}

impl AsyncIo {
    /// Create a new async I/O action.
    ///
    /// # Arguments
    ///
    /// * `id` - Identity of the page creating the action.
    /// * `ctx` - Context associated with the action.
    /// * `action` - I/O action to asynchronously perform.
    pub(crate) fn new(id: u32, ctx: ActionCtx, action: IoAction) -> Self {
        Self { id, ctx, action }
    }
}

/// Different types of user initiated actions against storage.
#[derive(Debug)]
pub(crate) enum Action {
    /// Action to query from storage.
    Query(Query),

    /// Action to append into storage.
    Append(Append),

    /// Action to get latest state of storage.
    Metadata(GetMetadata),
}

impl Action {
    /// Create a query action.
    ///
    /// # Arguments
    ///
    /// * `after_seq_no` - Sequence number to query logs after.
    /// * `buf` - Buffer to append log bytes read from storage.
    pub(crate) fn query(after_seq_no: u64, buf: IoBuf) -> (Self, BufReceiver<(), QueryError>) {
        let (tx, rx) = AsyncFate::channel();
        (Action::Query(Query { buf, tx, after_seq_no }), rx)
    }

    /// Create an append action.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer of logs to append to storage.
    pub(crate) fn append(buf: IoBuf) -> (Self, BufReceiver<(), AppendError>) {
        let (tx, rx) = AsyncFate::channel();
        (Action::Append(Append { buf, tx }), rx)
    }

    /// Create an action to get latest state.
    pub(crate) fn metadata() -> (Self, FateReceiver<Option<Metadata>>) {
        let (tx, rx) = AsyncFate::channel();
        (Action::Metadata(GetMetadata { tx }), rx)
    }
}

/// A request to get latest metadata from storage.
#[derive(Debug)]
pub(crate) struct GetMetadata {
    /// Sender to send action result asynchronously.
    pub(crate) tx: FateSender<Option<Metadata>>,
}

/// A request to query for some bytes from page.
#[derive(Debug)]
pub(crate) struct Query {
    /// Buffer shared with storage.
    pub(crate) buf: IoBuf,

    /// Query for logs after this sequence number.
    pub(crate) after_seq_no: u64,

    /// Sender to send action result asynchronously.
    pub(crate) tx: BufSender<(), QueryError>,
}

/// A request to append some bytes into page.
#[derive(Debug)]
pub(crate) struct Append {
    /// Buffer shared with storage.
    pub(crate) buf: IoBuf,

    /// Sender to send action result asynchronously.
    pub(crate) tx: BufSender<(), AppendError>,
}

/// Context associated with a pending storage action.
#[derive(Debug)]
pub(crate) enum ActionCtx {
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

    /// System action to sync data to disk.
    Fsync,
}

impl ActionCtx {
    /// Create context for a query action.
    ///
    /// # Arguments
    ///
    /// * `tx` - Sender for result of the action.
    pub(crate) fn query(tx: BufSender<(), QueryError>) -> ActionCtx {
        ActionCtx::Query { tx }
    }

    /// Create context for an append action.
    ///
    /// # Arguments
    ///
    /// * `tx` - Sender for result of the action.
    pub(crate) fn append(tx: BufSender<(), AppendError>) -> ActionCtx {
        ActionCtx::Append { tx }
    }
}

/// Error when fate of a storage action is lost.
#[derive(Debug, thiserror::Error)]
#[error("Fate of async operation was lost")]
pub(crate) struct FateError;

impl From<FateError> for io::Error {
    fn from(_value: FateError) -> Self {
        io::Error::new(io::ErrorKind::ConnectionAborted, "Fate sender dropped")
    }
}

/// A oneshot channel to notify fate of a storage action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AsyncFate;

impl AsyncFate {
    /// A new channel to send results of a storage action.
    pub(crate) fn channel<T>() -> (FateSender<T>, FateReceiver<T>) {
        let (tx, rx) = oneshot::channel();
        (FateSender(tx), FateReceiver(rx))
    }
}

/// Sender to send fate of a storage action.
#[derive(Debug)]
pub(crate) struct FateSender<T>(oneshot::Sender<T>);

impl<T> FateSender<T> {
    /// Send fate of an async operation.
    ///
    /// # Arguments
    ///
    /// * `value` - Result of the storage action.
    pub(crate) fn send(self, value: T) -> bool {
        self.0.send(value).is_ok()
    }
}

impl<T, E> FateSender<BufResult<T, E>> {
    /// Send fate of an async operation.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer shared storage.
    /// * `value` - Result of the storage action.
    pub(crate) fn send_buf(self, buf: IoBuf, value: Result<T, E>) {
        if let Err(message) = self.0.send((buf, value)) {
            let (buf, _) = message.into_inner();
            drop(buf); // Explicitly return to pool, just cause.
        }
    }

    /// Send fate of an async operation.
    ///
    /// This variant clear accumulated bytes in buffer before publishing result.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer shared storage.
    /// * `value` - Result of the storage action.
    pub(crate) fn send_clear_buf(self, mut buf: IoBuf, value: Result<T, E>) {
        buf.clear();
        self.send_buf(buf, value);
    }
}

/// Receiver for fate of a storage action.
#[derive(Debug)]
pub(crate) struct FateReceiver<T>(oneshot::Receiver<T>);

impl<T> FateReceiver<T> {
    /// Receive fate of an async operation asynchronously.
    pub(crate) async fn recv_async(self) -> Result<T, FateError> {
        self.0.await.map_err(|_| FateError)
    }
}
