//! Worker executing all storage actions.

use crate::{
    runtime::{BufPool, IoRuntime, RawBytes},
    storage::{
        Action, Options, Page, Storage, StorageState,
        action::{ActionCtx, Append, AsyncFate, IoQueue, LoadState, PageIo, Query},
        session::QueryError,
    },
};
use assert2::let_assert;
use std::{
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

/// A single threaded worker coordinating all storage actions.
#[derive(Debug)]
pub(super) struct Worker {
    closing: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    pub(super) async fn spawn<P: AsRef<Path>>(path: P, opts: Options) -> io::Result<Storage> {
        let closing = Arc::new(AtomicBool::new(false));
        let (fate_tx, fate_rx) = AsyncFate::channel::<io::Result<BufPool>>();

        let queue_depth = usize::from(opts.queue_depth);
        let (tx, rx) = flume::bounded(queue_depth);

        // Spawn worker thread.
        let worker_close = closing.clone();
        let path = path.as_ref().to_path_buf();
        let handle = std::thread::spawn(move || {
            // Perform all the blocking I/O actions necessary to initialize storage and worker.
            let init_result = || {
                // First open all the pages of storage.
                let (next, mut pages) = Self::open_pages(path.as_ref(), opts)?;

                // Create I/O runtime for storage.
                let mut runtime = IoRuntime::new(opts.queue_depth.into())?;

                // Allocate all memory for buffer pool.
                let pool_size = usize::from(opts.pool.pool_size);
                let mut buffers = (0..pool_size)
                    .map(|_| RawBytes::allocate(opts.pool.buf_capacity, opts.pool.huge_buf))
                    .collect::<io::Result<Vec<_>>>()?;

                // Register buffers and files with the runtime.
                let files: Vec<_> = pages.iter().map(Page::raw_fd).collect();
                let files = runtime.register_files(&files)?;
                for (file, page) in files.into_iter().zip(pages.iter_mut()) {
                    page.set_io_file(file);
                }

                // Register allocated memory with io uring runtime and create buffer pool.
                unsafe { runtime.register_bufs(&mut buffers)? };
                let pool = BufPool::new(buffers);

                // Return initialized components.
                Ok((runtime, pool, pages, next))
            };

            // Do stuff depending on the results of initialization.
            match init_result() {
                Err(error) => {
                    fate_tx.send(Err(error));
                }

                Ok((runtime, pool, pages, next)) => {
                    if !fate_tx.send(Ok(pool)) {
                        eprintln!("Worker stopping cause fate receiver dropped");
                        return;
                    }

                    let io_queue = IoQueue::with_capacity(queue_depth);
                    let worker = WorkerState {
                        next,
                        pages,
                        rx,
                        runtime,
                        io_queue,
                    };

                    worker.process(worker_close);
                }
            };
        });

        // Handle to worker.
        let _worker = Arc::new(Worker {
            closing,
            handle: Some(handle),
        });

        // Wait till we hear about initialization result.
        let buf_pool = fate_rx.recv_async().await??;
        Ok(Storage { buf_pool, _worker, tx })
    }

    fn open_pages(path: &Path, opts: Options) -> io::Result<(usize, Vec<Page>)> {
        use rayon::prelude::*;

        // Initialize all the pages in parallel.
        let mut pages: Vec<_> = (0..opts.ring_size)
            .into_par_iter()
            .map(|id| {
                let file_name = format!("{id:0>10}.page");
                let file_path = path.join(file_name);
                Page::open(id, file_path, opts.page)
            })
            .collect::<io::Result<_>>()?;

        // Find latest page in storage.
        // That's the page with largest sequence number.
        let mut max = 0;
        let mut next = 0;
        for (i, page) in pages.iter().enumerate() {
            if let Some(state) = page.metadata()
                && state.prev_seq_no > max
            {
                next = i;
                max = state.prev_seq_no;
            }
        }

        // Figure out all the pages that can be preserved.
        let latest = &mut pages[next];
        if let Some(state) = latest.metadata() {
            let mut prev = next;
            let mut is_reset = false;
            let mut after_seq_no = state.after_seq_no;
            loop {
                // Next of the next page to check.
                let index = if prev == 0 { pages.len() - 1 } else { prev - 1 };
                if index == next {
                    break;
                }

                // Previous page in the ring buffer.
                let page = &mut pages[index];

                // An unconditional reset if a previous page was reset.
                if is_reset {
                    page.clear()?;
                } else if let Some(state) = page.metadata() {
                    // Make sure sequences match up with the earlier page.
                    if state.prev_seq_no != after_seq_no {
                        is_reset = true;
                        page.clear()?;
                    }

                    // Nice, everything checks out.
                    // We can use this page as is in storage.
                    after_seq_no = state.after_seq_no;
                } else {
                    // If this page is uninitialized, none of the pages
                    // before (in ring buffer) should be initialized either.
                    is_reset = true;
                    page.clear()?;
                }

                // For next iteration.
                prev = index;
            }
        } else {
            // In theory this is not necessary, for completeness.
            for page in pages.iter_mut() {
                page.clear()?;
            }
        }

        // Return parsed pages along with starting index.
        Ok((next, pages))
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
    pub(super) next: usize,
    pub(super) pages: Vec<Page>,
    pub(super) io_queue: IoQueue,
    pub(super) rx: flume::Receiver<Action>,
    pub(super) runtime: IoRuntime<(u32, ActionCtx)>,
}

impl WorkerState {
    /// Amount of time waiting for an action to be available in shared channel.
    /// This is required because we do not want to assume that all the senders
    /// will be dropped during graceful shutdown.
    const ACTION_AWAIT_TIMEOUT: Duration = Duration::from_secs(1);

    fn process(mut self, closing: Arc<AtomicBool>) {
        use rayon::prelude::*;

        loop {
            // Check if graceful shutdown has begun.
            // We'll continue to complete all pending work.
            let is_closing = closing.load(Ordering::Relaxed);

            // If not closing, await for an action, i.e, blocking wait for a processable
            // action to be available. If closing, we just want to flush all the pending
            // work and shutdown, so no blocking wait.
            if !is_closing {
                self.await_queue_action();
            }

            // Drain the channel and queue up as many actions as possible for processing.
            self.try_queue_actions();

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
            self.process_completions();
        }

        // Close all the pages.
        self.pages.into_par_iter().for_each(|page| {
            if let Err(e) = page.close() {
                eprintln!("Error during graceful close of page: {e}");
            }
        });
    }

    fn process_completions(&mut self) {
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
                    page.commit(success.result, success.action, success.attachment.1, &mut self.io_queue);
                }

                Err(error) => {
                    // Find the page that must complete the I/O request.
                    let index = error.attachment.0;
                    let page = self
                        .pages
                        .get_mut(index as usize)
                        .expect("Got I/O response for a page that does not exist");

                    page.abort(error.error, error.action, error.attachment.1, &mut self.io_queue);
                }
            }
        }
    }

    fn process_actions(&mut self) {
        while let Some(action) = self.io_queue.pop_queued() {
            match action {
                Action::State(state) => self.process_load_state(state),
                Action::Query(query) => self.process_query(query),
                Action::Append(append) => self.process_append(append),
            }
        }
    }

    fn process_load_state(&mut self, state: LoadState) {
        let LoadState { tx } = state;

        // Return early if storage has not been initialized.
        let page = &mut self.pages[self.next];
        let Some(state) = page.metadata() else {
            tx.send(None);
            return;
        };

        // Start populating state.
        // Iterate through rest of the pages to populate state.
        let mut state = StorageState {
            prev_seq_no: state.prev_seq_no,
            after_seq_no: state.after_seq_no,
            log_count: state.count,
            disk_size: state.file_size,
        };

        let mut prev = self.next;
        loop {
            // Next of the next page to check.
            let index = if prev == 0 { self.pages.len() - 1 } else { prev - 1 };

            // Check if we are just rotating over and over again.
            if index == self.next {
                break;
            }

            // Break if we can out of initialized pages.
            let Some(page_state) = &self.pages[index].metadata() else {
                break;
            };

            // Consume the page.
            prev = index;
            state.after_seq_no = page_state.after_seq_no;
            state.log_count += page_state.count;
        }

        // Return the fully populated state.
        tx.send(Some(state));
    }

    fn process_append(&mut self, append: Append) {
        // Latest page is the page where all the writes happen.
        let page = &mut self.pages[self.next];

        // If the latest page is not already initialized, it means
        // that storage was newly created or open with no log records.
        if !page.is_initialized() {
            // Fetch the first log to initialize the latest page.
            let mut iter = append.buf.into_iter();
            let Some(first) = iter.next() else {
                append.tx.send_buf(append.buf, Ok(()));
                return;
            };

            // Logs in storage will be after this sequence number.
            page.initialize(first.prev_seq_no());
        }

        // Attempt to append the buffer of logs into the latest page.
        if !page.append(append, &mut self.io_queue) {
            // Fetch the state of the previous page.
            // We need this to link the sequence numbers for the next page.
            let_assert!(Some(state) = page.metadata());

            // Fetch the next page in the ring buffer.
            let next = self.next + 1;
            let next = if next >= self.pages.len() { 0 } else { next };
            let next_page = &mut self.pages[next];

            // The the page is currently initialized,
            // it needs to be wiped clean for the new buffer of logs.
            if next_page.is_initialized() {
                next_page.reset(&mut self.io_queue);
                return;
            }

            // Otherwise the next page needs to be initialized before use.
            next_page.initialize(state.prev_seq_no);
            self.next = next;
        }
    }

    fn process_query(&mut self, query: Query) {
        let Query {
            mut buf,
            after_seq_no,
            tx,
        } = query;

        // Check if storage has not been fully initialized.
        // If storage hasn't been initialized yet, act as though requested
        // logs are not yet available in storage.
        let page = &mut self.pages[self.next];
        if !page.is_initialized() {
            buf.clear();
            tx.send_buf(buf, Ok(()));
            return;
        }

        // Find the page that actually contains the requested logs.
        let mut prev = None;
        loop {
            // Next of the next page to check.
            let index = prev.unwrap_or(self.next);

            // Check if we are just rotating over and over again.
            if prev.is_some() && index == self.next {
                tx.send_buf(buf, Err(QueryError::Trimmed(after_seq_no)));
                break;
            }

            // Get the next page to check.
            let page = &mut self.pages[index];
            let Some(state) = page.metadata() else {
                // There is no page that contains the requested range of logs.
                tx.send_buf(buf, Err(QueryError::Trimmed(after_seq_no)));
                break;
            };

            // If the page could contain requested logs, we attempt to read it.
            if state.after_seq_no <= after_seq_no {
                page.query(Query { buf, after_seq_no, tx }, &mut self.io_queue);
                break;
            }

            // Go back pages and check again.
            let next_prev = if index == 0 { self.pages.len() - 1 } else { index - 1 };
            prev = Some(next_prev);
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
                self.io_queue.reissue(PageIo { id, ctx, action });
                break;
            }
        }
    }

    fn await_queue_action(&mut self) {
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

    fn try_queue_actions(&mut self) {
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
            remaining -= 1;
            self.io_queue.queue(action);
        }

        // Retry pending actions in this iteration.
        while let Some(action) = self.io_queue.pop_reprocess() {
            self.io_queue.queue(action);
        }
    }
}
