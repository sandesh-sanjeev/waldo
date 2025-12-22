//! Oneshot channel to notify fate of a storage action.

use crate::{runtime::IoBuf, storage::BufResult};

/// Error when fate of a storage action is lost.
#[derive(Debug, thiserror::Error)]
#[error("Fate of async operation was lost, probably due to closing")]
pub(super) struct FateError;

/// A oneshot channel to notify fate of a storage action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AsyncFate;

impl AsyncFate {
    pub fn channel<T>() -> (FateSender<T>, FateReceiver<T>) {
        let (tx, rx) = oneshot::channel();
        (FateSender(tx), FateReceiver(rx))
    }
}

/// Sender to send fate of a storage action.
#[derive(Debug)]
pub(super) struct FateSender<T>(oneshot::Sender<T>);

impl<T, E> FateSender<BufResult<T, E>> {
    /// Send fate of an async operation.
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
