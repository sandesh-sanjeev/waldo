# Waldo

Waldo is an asynchronous storage engine to hold sequential log records. It is a lock-free, append-only, on-disk 
ring buffer with excellent performance characteristics. It provides a minimal set of APIs to append new log records 
and query arbitrary log ranges. It was initially developed to store write ahead logs for an MVCC aware datastore, 
hence the name.

Waldo uses io-uring APIs in linux for batching and asynchronous execution of disk I/O. Consequently linux is
the only supported OS, and requires a relatively recent kernel version (6.8+).

[![Build Status][build-img]][build-url]
[![Documentation][doc-img]][doc-url]

[build-img]: https://github.com/sandesh-sanjeev/waldo/actions/workflows/ci.yml/badge.svg?branch=master
[build-url]: https://github.com/sandesh-sanjeev/waldo/actions/workflows/ci.yml

[doc-img]: https://img.shields.io/badge/crate-doc-green?style=flat
[doc-url]: https://sandesh-sanjeev.github.io/waldo/waldo/index.html

## Design

Storage is organized into pages where every page holds a contiguous chunk of log records. Only one page is ever
active for writes. When this page runs out of capacity, it is rotated away with another page. This might be an
empty page, or a page that contains oldest log records. If it's the latter, the page is truncated prior to use.

A page is physically backed by a file on disk and an in-memory index. Backing file is a flat file of serialized
log records. Index contains offsets to logs on file. Memory is precious and it is generally not useful to index 
every log record. The index can be made sparse, trading memory for some small penalty during log queries.

A single threaded worker coordinates all reads and writes from storage. Rest of the world interacts with this worker
using queues (io-uring) and channels (your application). All memory, except those necessary to create oneshot channels
is allocated once during initialization and reused.

Storage does no caching, it is entirely the responsibility of the OS and file system caches. While user space cache
with direct I/O seems attractive at first, alignment rules make it harder to implement correctly and more importantly
hurts performance with a single threaded writer.

When appending a batch of log records, atomicity is only guaranteed for a single log record, not for the entire batch.
For durability guarantees, enable `o_dsync` flag in `FileOptions`. This should be a great choice for most applications,
unless you really don't care about losing some logs from the tip of storage.

Finally, every open of storage results in parsing and validation of all storage files. Additionally any corruption is
automatically truncated away so that storage always holds a contiguous sequence of valid log records. There is
support to validate integrity of log records when iterating through queried logs. However recommendation is instead
having your own integrity checking mechanism or even better encrypt your logs - whatever makes sense for your use case.

## Gotchas

### Huge Pages

For best performance enable buffer pool allocation using huge pages. Note that when huge pages is enabled,
buffer capacity must be non-zero multiples of system huge page size, which is usually 2 MB. 

Configuration updates (and reboot) maybe necessary to make sure OS has enough huge pages available. On most distributions this can be done via sysctl, for example `sudo sysctl -w vm.nr_hugepages=256`.

### Memlock

Waldo registers all file descriptors and buffers used with kernel. This provides better performance by allowing
kernel to hold on to long term shared references. Unfortunately this counts against users memlock limits. 
Memlock limits for the user must be increased appropriately, for example via `/etc/security/limits.conf` update.

## Benchmarks

Benchmarks must be performed on your machine with your workload, otherwise it is meaningless. 

### Steady state

In this test all the readers are caught up to the tip of storage. They continue to query from storage at
the same rate as the writer. This is the best case scenario that is likely to result in high rate of file 
system cache hits, meaning most queries are really memcpy.

Linux VM (4 cores, 8 GB RAM) running on my M1 Mac Pro.

```text
Storage | InitTime: 0.56 s  | Logs: 3141632 in (21987328, 25128960]     | Size: 3.00 GB
Bench   | BufPoolSize: 256  | QueueDepth: 256       | Readers: 3200     | Delay: 200ms
Worker  | Logs: 1000000     | LogSize: 1024 B       | BatchSize: 2048   | Total: 0.95 GB
Writer  | 99.44s            | 10055.99 Logs/s       | 9.82 MB/s
Readers | 99.44s            | 32179177.69 Logs/s    | 31424.98 MB/s
```

### Run benchmarks

This crate provides benchmark binaries to benchmark Waldo on your hardware (or VM). These can be created by building
the crate with `--features benchmark`. Alternatively they can be executed directly using `cargo run`.

Check `--help` for available options and defaults, but here are examples.

```bash
# Generate benchmark binaries.
$ cargo build --release --features benchmark

# Execute benchmarks.
$ cargo run --release --bin storage --features benchmark -- --readers 1024 --count 1000000 --delay 100
```

### Run benchmarks with profiler

Use [`perf`](https://www.brendangregg.com/perf.html), [`cargo flamegraph`](https://crates.io/crates/flamegraph),
etc to run profiler with benchmarks. Benchmark binaries must be built/executed with `--profile bench` to include 
debug symbols in benchmark binaries.

Note that to capture kernel events without using sudo, some updates maybe necessary. Remember to revert back when no longer necessary.

```bash
$ echo 0 | sudo tee /proc/sys/kernel/kptr_restrict
$ echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

Check `--help` for available options and defaults, but here are examples.

```bash
# Generate benchmark binaries.
$ cargo build --profile bench --features benchmark

# Execute benchmarks with profiler.
$ perf record -g --call-graph dwarf -F 999 ./target/release/storage

# Flamegraphs are easy too.
$ cargo flamegraph --bin storage --features benchmark --profile bench -- --count 10000000
```
