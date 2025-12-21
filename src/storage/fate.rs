use crate::{runtime::IoBuf, storage::BufResult};

#[derive(Debug, thiserror::Error)]
#[error("Fate of async operation was lost, probably due to closing")]
pub struct FateError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncFate;

impl AsyncFate {
    pub fn channel<T>() -> (FateSender<T>, FateReceiver<T>) {
        let (tx, rx) = oneshot::channel();
        (FateSender(tx), FateReceiver(rx))
    }
}

#[derive(Debug)]
pub struct FateSender<T>(oneshot::Sender<T>);

impl<T, E> FateSender<BufResult<T, E>> {
    /// Send fate of an async operation.
    pub fn send(self, buf: IoBuf, value: Result<T, E>) {
        if let Err(message) = self.0.send((buf, value)) {
            let (buf, _) = message.into_inner();
            drop(buf); // Explicitly return to pool, just cause.
        }
    }
}

#[derive(Debug)]
pub struct FateReceiver<T>(oneshot::Receiver<T>);

impl<T> FateReceiver<T> {
    /// Receive fate of an async operation.
    #[allow(dead_code)]
    pub fn recv(self) -> Result<T, FateError> {
        self.0.recv().map_err(|_| FateError)
    }

    /// Receive fate of an async operation asynchronously.
    pub async fn recv_async(self) -> Result<T, FateError> {
        self.0.await.map_err(|_| FateError)
    }
}
