//! Worker executing all storage actions.

use crate::Options;
use crate::action::{Action, ActionCtx, AsyncFate, AsyncIo};
use crate::queue::IoQueue;
use crate::ring::PageRing;
use crate::runtime::{BufPool, IoError, IoResponse, IoRuntime};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{io, path::Path, thread::JoinHandle, time::Duration};

/// A single threaded worker coordinating all storage actions.
#[derive(Debug)]
pub(crate) struct Worker {
    closing: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    /// Spawn a new storage worker.
    ///
    /// If successful, returns all the different components of storage.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the home directory of storage instance.
    /// * `opts` - Options to open storage.
    pub(crate) async fn spawn(path: &Path, opts: Options) -> io::Result<(BufPool, flume::Sender<Action>, Self)> {
        // Channel to return result of spawn.
        let (fate_tx, fate_rx) = AsyncFate::channel();

        // Flag to communicate intent to shutdown.
        let closing = Arc::new(AtomicBool::new(false));

        // Spawn the worker thread.
        let handle = {
            let closing = closing.clone();
            let path = path.to_path_buf();
            std::thread::spawn(move || match WorkerState::new(&path, opts) {
                Err(error) => {
                    let _ = fate_tx.send(Err(error));
                }

                Ok((pool, tx, state)) => {
                    if !fate_tx.send(Ok((pool, tx))) {
                        return;
                    }

                    state.process(closing);
                }
            })
        };

        // Wait for initialization of background worker is complete.
        let (pool, tx) = fate_rx.recv_async().await??;
        let worker = Worker {
            closing,
            handle: Some(handle),
        };

        // Return results from initialization.
        Ok((pool, tx, worker))
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.closing.store(true, Ordering::Relaxed);
            if let Err(e) = handle.join() {
                // TODO: Tracing, log, etc.
                eprintln!("Error dropping worker: {e:?}");
            }
        };
    }
}

/// State tracked by a storage worker.
struct WorkerState {
    ring: PageRing,
    io_queue: IoQueue,
    rx: flume::Receiver<Action>,
    runtime: IoRuntime<(u32, ActionCtx)>,
}

impl WorkerState {
    /// Amount of time waiting for an action to be available in shared channel.
    /// This is required because we do not want to assume that all the senders
    /// will be dropped during graceful shutdown.
    const ACTION_AWAIT_TIMEOUT: Duration = Duration::from_secs(1);

    fn new(path: &Path, opts: Options) -> io::Result<(BufPool, flume::Sender<Action>, Self)> {
        // Create I/O runtime for storage.
        let mut runtime = IoRuntime::new(opts.queue_depth.into())?;

        // Open storage files, parse pages and initialize backing ring buffer.
        let mut ring = PageRing::open(path, opts)?;
        ring.register(&mut runtime)?;

        // Create buffer pool to use with storage.
        let buf_pool = BufPool::registered(opts.pool, &mut runtime)?;

        // Create internal buffers to store async I/O actions.
        let queue_depth = usize::from(opts.queue_depth);
        let io_queue = IoQueue::with_capacity(queue_depth);
        let (tx, rx) = flume::bounded(queue_depth);

        // Build starting state for the worker.
        let state = Self {
            ring,
            io_queue,
            rx,
            runtime,
        };

        // Return all the different components of storage.
        Ok((buf_pool, tx, state))
    }

    fn process(mut self, closing: Arc<AtomicBool>) {
        loop {
            // Check if graceful shutdown has begun.
            // We'll continue to complete all pending work.
            let is_closing = closing.load(Ordering::Relaxed);

            // If not closing, await for an action, i.e, blocking wait for a processable
            // action to be available. If closing, we just want to flush all the pending
            // work and shutdown, so no blocking wait.
            if !is_closing {
                self.await_action();
            }

            // Drain the channel and queue up as many actions as possible for processing.
            self.try_actions();

            // Process any of the queued up I/O actions.
            self.process_actions();

            // If there is no work to do at this point it can mean a few different things:
            // 1. Closing as begun and we have flushed all work.
            // 2. All the actions where completed and did not need I/O.
            // 2. There are actions that must be retried.
            if self.runtime.pending_io() == 0 && self.io_queue.is_empty_issued() {
                if is_closing {
                    // Cause we have flushed on pending actions.
                    break;
                } else {
                    // Cause we just have to wait for more actions.
                    // This is expected and okay because some actions are no-op.
                    // And we timeout, rather than waiting for new work forever.
                    continue;
                }
            }

            // Submit queued up actions to the runtime.
            self.submit_actions();

            // Wait for at least one pending I/O action to complete.
            if let Err(e) = self.runtime.submit_and_wait(1) {
                eprintln!("Error from I/O uring runtime: {e}");
            }

            // Process all the completed actions.
            self.complete_actions();
        }

        // Close all the pages.
        if let Err(e) = self.ring.close() {
            eprintln!("Error closing storage ring buffer: {e}");
        }
    }

    fn await_action(&mut self) {
        // If there are pending I/O action, we will never wait for more work.
        // We will always prioritize completing work that was already started.
        if self.runtime.pending_io() > 0 {
            return;
        }

        // If there are already some queued by work, then we won't wait for more.
        if !self.io_queue.is_empty() {
            return;
        }

        // Return early when all senders have disconnected.
        // TODO: If we were to drop all the senders during drop, that would speed this up.
        let Ok(action) = self.rx.recv_timeout(Self::ACTION_AWAIT_TIMEOUT) else {
            return;
        };

        // Queue up the awaited action.
        self.io_queue.queue(action);
    }

    fn try_actions(&mut self) {
        // Retry pending actions in this iteration.
        while let Some(action) = self.io_queue.pop_reprocess() {
            self.io_queue.queue(action);
        }

        // Figure out how much more slots I/O uring runtime has.
        let sq_remaining = self.runtime.sq_remaining();
        let cq_remaining = self.runtime.cq_remaining();
        let io_remaining = std::cmp::min(sq_remaining, cq_remaining);

        // Fetch more actions from shared channel (if there is enough capacity).
        let mut remaining = io_remaining.saturating_sub(self.io_queue.len());
        while remaining > 0 {
            // Fetch the next submitted action.
            let action = match self.rx.try_recv() {
                Ok(action) => action,
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => break,
            };

            // Queue up the newly discovered action.
            remaining -= 1;
            self.io_queue.queue(action);
        }
    }

    fn process_actions(&mut self) {
        while let Some(action) = self.io_queue.pop_queued() {
            match action {
                Action::Metadata(state) => self.ring.metadata(state),
                Action::Query(query) => self.ring.query(query, &mut self.io_queue),
                Action::Append(append) => self.ring.append(append, &mut self.io_queue),
            }
        }
    }

    fn submit_actions(&mut self) {
        // Submit the I/O request into the runtime.
        // Attempt to fully saturate the io-uring.
        while let Some(action) = self.io_queue.pop_issued() {
            let attachment = (action.id, action.ctx);
            if let Err(((id, ctx), action)) = self.runtime.push(action.action, attachment) {
                // I/O runtime doesn't have enough space for more entries.
                // Add the action back to the front of the queue.
                self.io_queue.reissue(AsyncIo { id, ctx, action });
                break;
            }
        }
    }

    fn complete_actions(&mut self) {
        while let Some(result) = self.runtime.pop() {
            match result {
                Ok(IoResponse {
                    result,
                    attachment,
                    action,
                }) => {
                    let action = AsyncIo::new(attachment.0, attachment.1, action);
                    self.ring.apply(result, action, &mut self.io_queue);
                }

                Err(IoError {
                    error,
                    attachment,
                    action,
                }) => {
                    let action = AsyncIo::new(attachment.0, attachment.1, action);
                    self.ring.abort(error, action, &mut self.io_queue);
                }
            }
        }
    }
}
