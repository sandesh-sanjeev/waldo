use crate::{
    runtime::{IoAction, IoFixedFd},
    storage::{
        Action,
        action::{ActionCtx, Append, Query},
    },
};
use std::{collections::VecDeque, io};

#[derive(Debug)]
pub(super) struct PageIo {
    pub(super) id: u32,
    pub(super) ctx: ActionCtx,
    pub(super) action: IoAction,
}

#[derive(Debug)]
pub(super) struct Page {
    // Index of the page in the ring buffer.
    pub(super) id: u32,

    // Number of bytes currently held in the page.
    pub(super) size: u64,

    // File handle to the underlying file on disk.
    pub(super) file: IoFixedFd,
}

impl Page {
    pub(super) fn new(id: u32, file: IoFixedFd) -> Self {
        Self { id, size: 0, file }
    }

    pub(super) fn action(&self, action: Action, queue: &mut VecDeque<PageIo>) {
        match action {
            Action::Query(query) => self.query(query, queue),
            Action::Append(append) => self.append(append, queue),
        };
    }

    pub(super) fn append(&self, append: Append, queue: &mut VecDeque<PageIo>) {
        let Append { buf, tx } = append;

        // Return early if there is nothing to append.
        if buf.is_empty() {
            tx.send(buf, Ok(()));
            return;
        }

        // Enqueue I/O action for execution asynchronously.
        queue.push_back(PageIo {
            id: self.id,
            ctx: ActionCtx::Append {
                tx,
                len: buf.len(),
                after: self.size,
            },
            action: IoAction::write_at(self.file, self.size, buf),
        });
    }

    pub(super) fn query(&self, query: Query, queue: &mut VecDeque<PageIo>) {
        let Query { after, mut buf, tx } = query;

        // Return early if there is nothing to query.
        if after >= self.size {
            buf.clear(); // Make sure to reflect no read.
            tx.send(buf, Ok(()));
            return;
        }

        // Do not attempt to read beyond known end of file.
        let remaining = self.size.saturating_sub(after);
        let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);
        let len = std::cmp::min(remaining, buf.capacity());
        buf.set_len(len); // Make enough space in buffer for new bytes.

        // Enqueue I/O action for execution asynchronously.
        queue.push_back(PageIo {
            id: self.id,
            ctx: ActionCtx::Query { tx, len, after },
            action: IoAction::read_at(self.file, after, buf),
        });
    }

    pub(super) fn commit(&mut self, result: u32, action: IoAction, ctx: ActionCtx) {
        match ctx {
            ActionCtx::Query { tx, len, .. } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");

                // Make sure all the bytes were read.
                if result != len as u32 {
                    let error = io::Error::new(io::ErrorKind::UnexpectedEof, "Incomplete read");
                    tx.send(buf, Err(error.into()));
                    return;
                }

                // Return bytes read and success.
                assert_eq!(len, buf.len());
                tx.send(buf, Ok(()));
            }

            ActionCtx::Append { tx, len, after } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");

                // Make sure all bytes written.
                if result != len as u32 {
                    let error = io::Error::new(io::ErrorKind::UnexpectedEof, "Incomplete write");
                    tx.send(buf, Err(error.into()));
                    return;
                }

                // Some sanity checks.
                assert_eq!(len, buf.len());
                assert_eq!(after, self.size);

                // Update state and return success.
                self.size += len as u64;
                tx.send(buf, Ok(()));
            }
        }
    }

    pub(super) fn abort(&self, error: io::Error, action: IoAction, ctx: ActionCtx) {
        match ctx {
            ActionCtx::Query { tx, .. } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");

                // Return error from the action.
                tx.send(buf, Err(error.into()));
            }

            ActionCtx::Append { tx, .. } => {
                // Get ownership of the shared buffer.
                let buf = action.take_buf().expect("Page action should have shared buffer");

                // Return error from the action.
                tx.send(buf, Err(error.into()));
            }
        }
    }
}
