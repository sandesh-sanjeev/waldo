//! Asynchronous actions initiated, processed and completed in Storage.

use crate::runtime::{IoAction, IoBuf};
use crate::{AppendError, Metadata, QueryError};
use std::io;

/// Type alias for results that involve I/O buffers.
type BufResult<T, E> = (IoBuf, Result<T, E>);

/// Type alias for sender of action result that have shared buffer(s).
pub(super) type BufSender<T, E> = FateSender<BufResult<T, E>>;

/// Type alias for receiver of action result that have shared buffer(s).
pub(super) type BufReceiver<T, E> = FateReceiver<BufResult<T, E>>;

/// An async I/O action initiated from a page.
#[derive(Debug)]
pub(super) struct AsyncIo {
    pub(super) id: u32,
    pub(super) ctx: ActionCtx,
    pub(super) action: IoAction,
}

impl AsyncIo {
    /// Create a new async I/O action.
    ///
    /// # Arguments
    ///
    /// * `id` - Identity of the page creating the action.
    /// * `ctx` - Context associated with the action.
    /// * `action` - I/O action to asynchronously perform.
    pub(super) fn new(id: u32, ctx: ActionCtx, action: IoAction) -> Self {
        Self { id, ctx, action }
    }
}

/// Different types of user initiated actions against storage.
#[derive(Debug)]
pub(super) enum Action {
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
    pub(super) fn query(after_seq_no: u64, buf: IoBuf) -> (Self, BufReceiver<(), QueryError>) {
        let (tx, rx) = AsyncFate::channel();
        (Action::Query(Query::new(buf, after_seq_no, tx)), rx)
    }

    /// Create an append action.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer of logs to append to storage.
    pub(super) fn append(buf: IoBuf) -> (Self, BufReceiver<(), AppendError>) {
        let (tx, rx) = AsyncFate::channel();
        (Action::Append(Append::new(buf, tx)), rx)
    }

    /// Create an action to get latest state.
    pub(super) fn metadata() -> (Self, FateReceiver<Option<Metadata>>) {
        let (tx, rx) = AsyncFate::channel();
        (Action::Metadata(GetMetadata::new(tx)), rx)
    }
}

/// A request to get latest metadata from storage.
#[derive(Debug)]
pub(super) struct GetMetadata {
    /// Sender to send action result asynchronously.
    pub(super) tx: FateSender<Option<Metadata>>,
}

impl GetMetadata {
    /// Create a new get metadata action.
    ///
    /// # Arguments
    ///
    /// * `tx` - Sender to send fate of an action.
    pub(super) fn new(tx: FateSender<Option<Metadata>>) -> Self {
        Self { tx }
    }
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

impl Query {
    /// Create a new query action.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer to populate with log record.
    /// * `after_seq_no` - Logs will be queried after this sequence number.
    /// * `tx` - Sender to send fate of an action.
    pub(super) fn new(buf: IoBuf, after_seq_no: u64, tx: BufSender<(), QueryError>) -> Self {
        Self { buf, after_seq_no, tx }
    }
}

/// A request to append some bytes into page.
#[derive(Debug)]
pub(super) struct Append {
    /// Buffer shared with storage.
    pub(super) buf: IoBuf,

    /// Sender to send action result asynchronously.
    pub(super) tx: BufSender<(), AppendError>,
}

impl Append {
    /// Create a new append action.
    ///
    /// # Arguments
    ///
    /// * `buf` - A buffer of logs to append into storage.
    /// * `tx` - Sender to send fate of an action.
    pub(super) fn new(buf: IoBuf, tx: BufSender<(), AppendError>) -> Self {
        Self { buf, tx }
    }
}

/// Context associated with a pending storage action.
#[derive(Debug)]
pub(super) enum ActionCtx {
    /// Context associated with a query action.
    Query(BufSender<(), QueryError>),

    /// Context associated with a append action.
    Append(BufSender<(), AppendError>),

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
    pub(super) fn query(tx: BufSender<(), QueryError>) -> ActionCtx {
        ActionCtx::Query(tx)
    }

    /// Create context for an append action.
    ///
    /// # Arguments
    ///
    /// * `tx` - Sender for result of the action.
    pub(super) fn append(tx: BufSender<(), AppendError>) -> ActionCtx {
        ActionCtx::Append(tx)
    }
}

/// Error when fate of a storage action is lost.
#[derive(Debug, thiserror::Error)]
#[error("Fate of async operation was lost")]
pub(super) struct FateError;

impl From<FateError> for io::Error {
    fn from(_value: FateError) -> Self {
        io::Error::new(io::ErrorKind::ConnectionAborted, "Fate sender dropped")
    }
}

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

impl<T> FateSender<T> {
    /// Send fate of an async operation.
    ///
    /// # Arguments
    ///
    /// * `value` - Result of the storage action.
    pub(super) fn send(self, value: T) -> bool {
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
    pub(super) fn send_buf(self, buf: IoBuf, value: Result<T, E>) {
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
    pub(super) fn send_clear_buf(self, mut buf: IoBuf, value: Result<T, E>) {
        buf.clear();
        self.send_buf(buf, value);
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
