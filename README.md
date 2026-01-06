# Waldo

Waldo is an asynchronous storage engine to hold sequential log records. It is a lock-free, append-only, on-disk 
ring buffer that is optimized for high throughput (in GB/s) writes and large read fanout (in 10,000s of concurrent
read streams). Waldo resides in your process (embedded), requires no maintenance and has excellent performance 
characteristics. It is being developed to store write ahead logs, hence the name. It can spot arbitrary log records 
pretty darn fast too.

Waldo uses io-uring APIs in linux for batching and asynchronous execution of disk I/O. Consequently linux is
the only supported OS. Additionally it requires a relatively recent kernel version (6.8+). This crate will compile
with any 5.10+ kernel, however you will certainly see runtime failures. Some of the io-uring opcodes used in this 
crate do not exist prior to version 6.8.

If that sounds good, look at [crate docs](https://sandesh-sanjeev.github.io/waldo/waldo/index.html) and find your
Waldo today.

[![Build Status][build-img]][build-url]
[![Documentation][doc-img]][doc-url]

[build-img]: https://github.com/sandesh-sanjeev/waldo/actions/workflows/ci.yml/badge.svg?branch=master
[build-url]: https://github.com/sandesh-sanjeev/waldo/actions/workflows/ci.yml

[doc-img]: https://img.shields.io/badge/crate-doc-green?style=flat
[doc-url]: https://sandesh-sanjeev.github.io/waldo/waldo/index.html

## Design

Storage is organized into pages where every page holds a contiguous chunk of log records. Only one page is ever
active for writes. When this page runs out of capacity, it is rotated away with another page. This is either an
empty page, or a page that contains oldest log records. If it's the latter, the page is truncated prior to use.

A page is physically backed by a file on disk and an in-memory index. Backing file is a flat file of serialized
log records. Index contains offsets to logs on file. It is generally not useful to index every log record and might
require significant memory depending on the workload. The index can be made sparse, trading memory for some small
penalty during log queries.

A single threaded worker coordinates all write into and reads from storage. Rest of the world interacts with this 
worker using queues (io-uring) and channels (your application). Waldo purely shares state via message passing not
locks/futex, importantly this allows the worker to optimally schedule reads/write for maximum throughput. Pinning
this worker to a single high performance/priority core might allow better latency, more experiments are necessary.

Every log record must contain a monotonically increasing sequence number to uniquely identify the record, sequence
number of previous log record and an associated payload. Logs must be appended in order and without gaps where current
and previous sequence numbers create an implicit "append iff previous == last committed record in storage" condition.
Conversely queried logs are always returned in order and without gaps.

When appending a batch of log records, atomicity is only guaranteed for a single log record, not for the entire batch.
For durability guarantees, enable `o_dsync` flag. This should be a great choice for most applications. It only only 
meaningfully impacts performance when appending in the order of GB/s, otherwise it makes little difference because
everything is async, non-blocking and lock-free.

Every `open` of Waldo results in parsing and validation of all storage files. Importantly any corruption is
automatically truncated away so that storage always holds a contiguous sequence of valid log records. There is
support to additionally validate integrity of log records when iterating through queried logs. However recommendation 
is instead having your own integrity checking mechanism or even better encrypt your logs - whatever makes sense for
your use case.

Waldo eagerly allocates most memory it requires when opened for the first time. The aim is to allocate all memory
upfront and prevent repeated malloc/free in hot code paths, which typically is expensive. There is some minimal 
amounts of heap allocations during runtime, primarily for creation of oneshot channels. However they are cheap and 
barely show up in profiler. Regardless, this crate will never support `no_std`.

Finally Waldo provides a streaming style API. The two halves of the stream are `sink` and `stream`. A Sink is a
buffered log writer to append new logs to storage. A Stream is well, a stream that starts delivering log records
using a provided starting seed sequence number.

## Caching

Waldo does no caching, it is entirely the responsibility of the OS and file system caches. Waldo makes no assumptions
around the append or access patterns. Every workload is different, it is important to benchmark and establish ideal
waldo parameters such as queue depth and buffer pool size. Lots of free RAM definitely helps and is automatically used!

## Unsafe

io-uring requires asynchronously sharing memory with the kernel. This is inherently an unsafe operation, i.e, buffers
must remain valid and untouched while kernel has a reference to the buffer. There is only one (safe) interface to
achieve this in async rust where futures can be cancelled. And that is taking ownership of buffer and stashing it away
in a place where the sun don't shine while kernel holds a reference, that's what this crate does.

There are unsafe uses in few other places to remove unnecessary bounds checking in hot code path, where Rust/LLVM do
not automatically elide bounds checking. They are trivially provable correct with manual inspection and specialized 
tools like Miri and Kani.

## Unsupported

These are non-goals and will probably never be supported.

* Non-Linux OS, or linux with kernel < 6.8.
* Concurrent access from multiple processes.
* Direct I/O, DMA, SPDK, etc.

## Gotchas

### Huge Pages

For best performance enable buffer pool allocation using huge pages. Note that when huge pages is enabled,
buffer capacity must be non-zero multiples of system huge page size, which is usually 2 MB. 

Configuration updates (and reboot) maybe necessary to make sure OS has enough huge pages available. On most 
distributions this can be done via sysctl, for example `sudo sysctl -w vm.nr_hugepages=256`.

### Memlock

Waldo registers all file descriptors and buffers used with kernel. This provides better performance by allowing
kernel to hold on to long term shared references. Unfortunately this counts against users memlock limits. 
Memlock limits for the user must be increased appropriately, for example via `/etc/security/limits.conf` update.

## Benchmarks

Benchmarks must be performed on your machine with your workload, otherwise it is meaningless. 

### Steady state

In this test all the readers are caught up to the tip of storage. They continue to stream from storage with the same 
rate as appends from writer. This is the best case scenario where high file cache hit rate is likely, meaning most 
queries are really memcpy (rather than read from disk).

An example result with Waldo on a Linux VM (4 cores, 8 GB RAM) running on my M1 Mac Pro. Upto 30 GB/s worth of log 
records queried and upto 1 GB/s worth of log records appended. Number of readers that can be supported is inversely 
proportional to rate with which logs are appended. 

Low append rate (10 MB/s) with 3000 concurrent readers.

```text
Bench   | BufPoolSize: 256  | QueueDepth: 256       | Readers: 3000     | Delay: 200ms
Worker  | Logs: 1000000     | LogSize: 1024 B       | BatchSize: 2048   | Total: 0.95 GB
Writer  | 97.76s            | 10229.61 Logs/s       | 9.99 MB/s
Readers | 97.76s            | 30688838.40 Logs/s    | 29969.57 MB/s
```

Medium append rate (100 MB/s) with 300 concurrent readers.

```text
Bench   | BufPoolSize: 256  | QueueDepth: 256       | Readers: 300      | Delay: 20ms
Worker  | Logs: 1000000     | LogSize: 1024 B       | BatchSize: 2048   | Total: 0.95 GB
Writer  | 9.84s             | 101628.45 Logs/s      | 99.25 MB/s
Readers | 9.84s             | 30488535.87 Logs/s    | 29773.96 MB/s
```

High appended rate (1 GB/s) with 1 concurrent reader.

```text
Bench   | BufPoolSize: 256  | QueueDepth: 256       | Readers: 1        | Delay: 1ms
Worker  | Logs: 50000000    | LogSize: 1024 B       | BatchSize: 2048   | Total: 47.68 GB
Writer  | 43.63s            | 1146052.75 Logs/s     | 1119.19 MB/s
Readers | 43.63s            | 1146052.75 Logs/s     | 1119.19 MB/s
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

Note that to capture kernel events without using sudo, some updates maybe necessary. Remember to revert back when 
no longer necessary.

```bash
$ echo 0 | sudo tee /proc/sys/kernel/kptr_restrict
$ echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

Check `--help` for available options and defaults, but here are examples.

```bash
# Generate benchmark binaries.
$ cargo build --profile bench --features benchmark

# Execute benchmarks with profiler.
$ perf record -g -F 999 ./target/release/storage

# Flamegraphs are easy too.
$ cargo flamegraph --bin storage --features benchmark --profile bench -- --count 10000000
```
