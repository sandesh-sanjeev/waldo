//! Waldo
//!
//! # Design
//!
//! ## I/O runtime
//!
//! An abstraction on top of io-uring that provides a safe interface to submit
//! and reap I/O requests. It's a fairly low level abstraction that is expected
//! to be used internally within the crate.

pub mod log;
pub mod runtime;
