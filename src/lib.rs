//! # Waldo
//!
//! Waldo is an embedded, on-disk, ring buffer of sequential log records.
//!
//! Waldo provides a streaming style interface that is optimized for high throughput
//! writes (in GB/s of logs) and large read fanout (in 10,000s of readers). README in
//! [repository](https://github.com/sandesh-sanjeev/waldo#) provides an overview of the
//! high level design, capabilities and limitations.
//!
//! Note that this crate uses it's own bespoke io-uring based async runtime to drive
//! asynchronous disk I/O. It is exposed publicly when compiled with `benchmark` feature
//! enabled. It's just that, for benchmarks. Should probably go without saying, do not
//! compile with `benchmark` feature flag and take a dependency on the async runtime.
//!
//! ## Getting Started
//!
//! Open [`Waldo`] with path to home directory on disk. [`Options`] used to open Waldo allows
//! one  to tailor Waldo to their throughput, latency, concurrency and memory usage goals.
//!
//! ```rust,ignore
//! // Open storage with specific set of options.
//! let opts: Options = ..;
//! let storage = Waldo::open("test", opts).await?;
//! ```
//!
//! ### Sink
//!
//! Use a [`Sink`] to push new log records into storage.
//!
//! ```rust,ignore
//! // Create a new sink into storage.
//! let mut sink = storage.sink().await;
//!
//! // Push a new log record(s) into storage.
//! sink.push(Log::new_borrowed(1, 0, "first log")).await?;
//! sink.push(Log::new_borrowed(2, 1, "second log")).await?;
//!
//! // Flush once you're done.
//! sink.flush().await?;
//! ```
//!
//! ### Stream
//!
//! Use a [`Stream`] to discover new log records from storage.
//!
//! ```rust,ignore
//! // Create a new stream from storage.
//! let mut stream = storage.stream_after(0);
//!
//! // Wait for new batch of log records.
//! while let Ok(logs) = stream.next().await {
//!     for log in &logs {
//!         println!("{log:?}");
//!     }
//! }
//! ```
//!
//! ## Next steps
//!
//! It's that simple! Run benchmarks and find the ideal configuration for your workload.

// All code should be used, at least in tests.
#![cfg_attr(not(test), allow(dead_code))]

mod log;
mod storage;

#[cfg(not(feature = "benchmark"))]
mod runtime;

#[cfg(feature = "benchmark")]
pub mod runtime;

pub use log::{Error as LogError, Log};
pub use storage::{AppendError, Error, Metadata, Options, QueryError, Waldo};
pub use storage::{Cursor, Sink, Stream, StreamLogIter, StreamLogs};
