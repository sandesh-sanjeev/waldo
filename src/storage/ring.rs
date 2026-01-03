//! A ring buffer of storage pages.

use crate::IoRuntime;
use crate::storage::action::{Append, AsyncIo, GetMetadata, Query};
use crate::storage::queue::IoQueue;
use crate::storage::{Metadata, Options, Page, QueryError};
use assert2::let_assert;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{io, path::Path};

/// A ring buffer of storage pages.
pub(super) struct PageRing {
    pub(super) next: usize,
    pub(super) pages: Vec<Page>,
}

impl PageRing {
    /// Open storage ring buffer.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the home directory of storage instance.
    /// * `opts` - Options to open storage.
    pub(super) fn open(path: &Path, opts: Options) -> io::Result<Self> {
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

        Ok(Self { next, pages })
    }

    /// Register ring buffer files in io-uring runtime.
    ///
    /// # Arguments
    ///
    /// * `runtime` - Runtime to register ring buffer files.
    pub(super) fn register<A>(&mut self, runtime: &mut IoRuntime<A>) -> io::Result<()> {
        let files: Vec<_> = self.pages.iter().map(Page::raw_fd).collect();
        let files = runtime.register_files(&files)?;
        for (file, page) in files.into_iter().zip(self.pages.iter_mut()) {
            page.set_io_file(file);
        }

        Ok(())
    }

    /// Process request to get storage metadata.
    ///
    /// # Arguments
    ///
    /// * `get_metadata` - Request to get storage metadata.
    pub(super) fn metadata(&mut self, get_metadata: GetMetadata) {
        let GetMetadata { tx } = get_metadata;

        // Return early if storage has not been initialized.
        let page = &mut self.pages[self.next];
        let Some(state) = page.metadata() else {
            tx.send(None);
            return;
        };

        // Start populating state.
        // Iterate through rest of the pages to populate state.
        let mut state = Metadata {
            prev_seq_no: state.prev_seq_no,
            after_seq_no: state.after_seq_no,
            log_count: state.count,
            index_count: state.index_count,
            disk_size: state.file_size,
            index_size: state.index_size,
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
            let Some(page_state) = self.pages[index].metadata() else {
                break;
            };

            // Consume the page.
            prev = index;
            state += page_state;
        }

        // Return the fully populated state.
        tx.send(Some(state));
    }

    /// Process request to append log buffer to storage.
    ///
    /// # Arguments
    ///
    /// * `append` - Request to append logs.
    /// * `queue` - Queue to issue I/O actions.
    pub(super) fn append(&mut self, append: Append, queue: &mut IoQueue) {
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
        if !page.append(append, queue) {
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
                next_page.reset(queue);
            } else {
                // Otherwise the next page needs to be initialized before use.
                next_page.initialize(state.prev_seq_no);
                self.next = next;
            }
        }
    }

    /// Process request to query logs from storage.
    ///
    /// # Arguments
    ///
    /// * `append` - Request to query logs.
    /// * `queue` - Queue to issue I/O actions.
    pub(super) fn query(&mut self, query: Query, queue: &mut IoQueue) {
        let Query { buf, after_seq_no, tx } = query;

        // Check if storage has not been fully initialized.
        // If storage hasn't been initialized yet, act as though requested
        // logs are not yet available in storage.
        let page = &mut self.pages[self.next];
        if !page.is_initialized() {
            tx.send_clear_buf(buf, Ok(()));
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
                page.query(Query { buf, after_seq_no, tx }, queue);
                break;
            }

            // Go back pages and check again.
            let next_prev = if index == 0 { self.pages.len() - 1 } else { index - 1 };
            prev = Some(next_prev);
        }
    }

    /// Apply results from a successful async I/O action.
    ///
    /// # Arguments
    ///
    /// * `result` - Result from kernel.
    /// * `action` - Completed async I/O action.
    /// * `queue` - Queue to issue I/O actions.
    pub(super) fn apply(&mut self, result: u32, action: AsyncIo, queue: &mut IoQueue) {
        // Find the page that must complete the I/O request.
        let page = self
            .pages
            .get_mut(action.id as usize)
            .expect("Got I/O response for a page that does not exist");

        // Complete the I/O operation.
        page.apply(result, action.action, action.ctx, queue);
    }

    /// Apply results from a failed async I/O action.
    ///
    /// # Arguments
    ///
    /// * `error` - Error result from kernel.
    /// * `action` - Completed async I/O action.
    /// * `queue` - Queue to issue I/O actions.
    pub(super) fn abort(&mut self, error: io::Error, action: AsyncIo, queue: &mut IoQueue) {
        // Find the page that must complete the I/O request.
        let page = self
            .pages
            .get_mut(action.id as usize)
            .expect("Got I/O response for a page that does not exist");

        // Complete the I/O operation.
        page.abort(error, action.action, action.ctx, queue);
    }

    /// Gracefully close all the the ring buffer pages.
    pub(super) fn close(self) -> io::Result<()> {
        self.pages
            .into_par_iter()
            .map(Page::close)
            .map(Result::err)
            .reduce(|| None, |a, b| a.or(b))
            .map_or(Ok(()), Err)
    }
}
