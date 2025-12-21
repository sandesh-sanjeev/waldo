use crate::{
    runtime::IoRuntime,
    storage::{Action, Page, PageIo, action::ActionCtx},
};
use std::{
    collections::VecDeque,
    fs::File,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

#[derive(Debug)]
pub struct Worker {
    closing: Arc<AtomicBool>,
    handle: Option<JoinHandle<io::Result<()>>>,
}

impl Worker {
    pub(super) fn spawn(state: WorkerState) -> Self {
        // Flag to communicate intent to shutdown.
        let closing = Arc::new(AtomicBool::new(false));

        // Spawn the worker thread.
        let handle = {
            let closing = closing.clone();
            std::thread::spawn(|| state.process(closing))
        };

        // Return guard for the worker.
        Self {
            closing,
            handle: Some(handle),
        }
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
pub(super) struct WorkerState {
    pub(super) pages: Vec<Page>,
    pub(super) files: Vec<File>,
    pub(super) io_queue: VecDeque<PageIo>,
    pub(super) rx: flume::Receiver<Action>,
    pub(super) runtime: IoRuntime<(u32, ActionCtx)>,
}

impl WorkerState {
    const ACTION_AWAIT_TIMEOUT: Duration = Duration::from_secs(1);

    fn process(mut self, closing: Arc<AtomicBool>) -> io::Result<()> {
        loop {
            // Check if graceful shutdown has begun.
            let is_closing = closing.load(Ordering::Relaxed);

            // If not closing, await for an action.
            // If closing, we just want to flush all the pending work and shutdown.
            if !is_closing {
                self.await_action();
            }

            // Drain the io_queue and process all actions.
            self.try_actions();

            // If there is nothing to do, it can only mean one thing,
            // graceful shutdown as begun and we have flushed all pending work.
            // Or all the senders of actions have dropped, either case, shutdown.
            if self.runtime.pending_io() == 0 && self.io_queue.is_empty() {
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

            // Submit queued up actions.
            self.submit_actions();

            // Wait for at least one pending I/O action to complete.
            if let Err(e) = self.runtime.submit_and_wait(1) {
                eprintln!("Error from I/O uring runtime: {e}");
            }

            // Process all the completed actions.
            while let Some(result) = self.runtime.pop() {
                match result {
                    Ok(success) => {
                        // Find the page that must complete the I/O request.
                        let index = success.attachment.0;
                        let page = self
                            .pages
                            .get_mut(index as usize)
                            .expect("Got I/O response for a page that does not exist");

                        // Complete the I/O operation.
                        page.commit(success.result, success.action, success.attachment.1);
                    }

                    Err(error) => {
                        // Find the page that must complete the I/O request.
                        let index = error.attachment.0;
                        let page = self
                            .pages
                            .get(index as usize)
                            .expect("Got I/O response for a page that does not exist");

                        page.abort(error.error, error.action, error.attachment.1);
                    }
                }
            }
        }

        // Flush all file changes to disk when closing.
        // We can use io-uring to do this asynchronously, meh (for now).
        let mut error = None;
        for file in self.files {
            if let Err(e) = file.sync_all() {
                // TODO: Tracing / logging of errors.
                eprintln!("Error fsync of file: {file:?}, error: {e}");
                error = Some(Err(e));
            }
        }

        // Return the result from the worker.
        error.unwrap_or(Ok(()))
    }

    fn queue_action(&mut self, action: Action) {
        // Fetch the right page to perform the action on.
        let Some(page) = self.pages.last() else {
            unreachable!("Every ring buffer must have one open page");
        };

        page.action(action, &mut self.io_queue);
    }

    fn submit_actions(&mut self) {
        // Submit the I/O request into the runtime.
        // Attempt to fully saturate the io-uring.
        while let Some(action) = self.io_queue.pop_front() {
            let attachment = (action.id, action.ctx);
            if let Err(((id, ctx), action)) = self.runtime.push(action.action, attachment) {
                // I/O runtime doesn't have enough space for more entries.
                // Add the action back to the front of the queue.
                self.io_queue.push_front(PageIo { id, ctx, action });
                break;
            }
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
        let Ok(action) = self.rx.recv_timeout(Self::ACTION_AWAIT_TIMEOUT) else {
            return;
        };

        // Queue up the awaited action.
        self.queue_action(action);
    }

    fn try_actions(&mut self) {
        let remaining = self.runtime.sq_remaining();
        let mut remaining = remaining.saturating_sub(self.io_queue.len());
        while remaining > 0 {
            // Fetch the next submitted action.
            let action = match self.rx.try_recv() {
                Ok(action) => action,
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => break,
            };

            // Queue up the newly discovered action.
            self.queue_action(action);
            remaining -= 1;
        }
    }
}
