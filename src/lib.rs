//! # Waldo
//!
//! Waldo is an embedded, on-disk, ring buffer of sequential log records.
//!
//! Waldo is optimized for high throughput batched writes (in GB/s of logs) and large read
//! fanout (in 10,000s of readers). [Repository](https://github.com/sandesh-sanjeev/waldo#)
//! provides an overview of the high level design, capabilities and limitations.
//!
//! Note that this crate uses it's own bespoke io-uring based async runtime to drive
//! asynchronous disk I/O. It is exposed publicly when compiled with `benchmark` feature
//! enabled. It's just that, for benchmarks. Should probably go without saying, do not
//! compile with `benchmark` feature flag and take a dependency on the async runtime.
//!
//! ## Getting Started
//!
//! ```rust
//! use waldo::{Options, Waldo, Log, Cursor};
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//!
//! // Step 1: Construct options for waldo.
//! let options = Options {
//!     ring_size: 4,
//!     queue_depth: 4,
//!     pool_size: 4,
//!     huge_buf: false,
//!     buf_capacity: 2 * 1024 * 1024,
//!     page_capacity: 100_000,
//!     index_capacity: 1000,
//!     index_sparse_bytes: 16 * 1024,
//!     index_sparse_count: 100,
//!     file_o_dsync: true,
//!     file_capacity: 4 * 1024 * 1024,
//! };
//!
//! // Step 2: Open waldo with path to home directory on disk.
//! let temp_dir = tempdir::TempDir::new("waldo")?;
//! let mut waldo = Waldo::open(temp_dir.path(), options).await?;
//!
//! // Step 3: Create a Sink to append log records.
//! let log_1 = Log::new_borrowed(1, 0, b"1");
//! let log_2 = Log::new_borrowed(2, 1, b"2");
//! let logs = [log_1, log_2];
//! waldo.append(&logs).await?;
//!
//! // Step 4: Create a stream to query log records.
//! let query_logs = waldo.query(Cursor::After(0), true).await?;
//! let query_logs: Vec<_> = query_logs.into_iter().collect();
//! assert_eq!(logs, query_logs);
//! #    Ok(())
//! # }
//! ```
//!
//! ## Next steps
//!
//! It's that simple! Run benchmarks and find the ideal configuration for your workload.

// All code should be used, at least in tests.
#![cfg_attr(not(test), allow(dead_code))]

mod log;
mod storage;
mod waldo;

#[cfg(not(feature = "benchmark"))]
mod runtime;

#[cfg(feature = "benchmark")]
pub mod runtime;

pub use log::{Error as LogError, Log};
pub use storage::{AppendError, Error, Metadata, Options, QueryError};
pub use waldo::{Cursor, QueryLogs, Waldo};
