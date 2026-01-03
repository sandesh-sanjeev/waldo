//! Waldo

mod log;
mod runtime;
mod storage;

pub use log::{Error as LogError, Log, LogIter, SequencedLogIter};
pub use runtime::{BufPool, Bytes, IoBuf, PoolOptions, RawBytes};
pub use runtime::{IoAction, IoError, IoResponse, IoRuntime};
pub use runtime::{IoFile, IoFileFd, IoFixedFd};
pub use storage::{AppendError, FileOpts, IndexOpts, Metadata, Options, PageOptions, QueryError, QueryLogs, Storage};
