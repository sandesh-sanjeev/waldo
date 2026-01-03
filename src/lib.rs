//! Waldo
//!
//! # Design
//!
//! ## I/O runtime
//!
//! An abstraction on top of io-uring that provides a safe interface to submit
//! and reap I/O requests. It's a fairly low level abstraction that is expected
//! to be used internally within the crate.

mod log;
mod runtime;
mod storage;

pub use log::{Error as LogError, Log, LogIter, SequencedLogIter};
pub use runtime::{BufPool, Bytes, IoBuf, PoolOptions, RawBytes};
pub use runtime::{IoAction, IoError, IoResponse, IoRuntime};
pub use runtime::{IoFile, IoFileFd, IoFixedFd};
pub use storage::{AppendError, FileOpts, IndexOpts, Metadata, Options, PageOptions, QueryError, Session, Storage};
