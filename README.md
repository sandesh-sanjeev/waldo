# Waldo

Waldo is an asynchronous storage engine to hold sequential log records. It is a lock-free, append-only, on-disk 
ring buffer that is optimized for high throughput (in GB/s) writes and massive read fanout (in 10,000s of concurrent
readers). Waldo resides in your process (embedded), requires no maintenance and has excellent performance 
characteristics. It is being developed to store write ahead logs, hence the name. It can spot arbitrary log records 
pretty darn fast too.

Waldo uses io-uring APIs in linux for batching and asynchronous execution of disk I/O. Consequently linux is
the only supported OS and requires a relatively recent kernel version 6.8+. While this crate will happily 
compile with any 5.10+ kernel, you will certainly see runtime failures. Some of the io-uring opcodes used in 
this crate do not exist prior to version 6.8.

Sounds good? Check out [crate docs](https://sandesh-sanjeev.github.io/waldo/waldo/index.html) and find your
Waldo today.

[![Build Status][build-img]][build-url]
[![Documentation][doc-img]][doc-url]
[![Coverage][coverage-img]][coverage-url]

[build-img]: https://github.com/sandesh-sanjeev/waldo/actions/workflows/ci.yml/badge.svg?branch=master
[build-url]: https://github.com/sandesh-sanjeev/waldo/actions/workflows/ci.yml

[doc-img]: https://img.shields.io/badge/crate-doc-green?style=flat
[doc-url]: https://sandesh-sanjeev.github.io/waldo/waldo/index.html

[coverage-img]: https://coveralls.io/repos/github/sandesh-sanjeev/waldo/badge.svg?branch=master
[coverage-url]: https://coveralls.io/github/sandesh-sanjeev/waldo?branch=master

## Design

A page is an append only data structure that holds a contiguous chunk of log records. User defined options influence
it's disk and memory footprint, ultimately limiting the maximum size of a page. Storage is organized into a ring buffer
of such pages. One (and only one) page is active for both reads and writes, this is where newly appended log records go.
Other pages are read-only, oldest of them will eventually be truncated and reused when the read-write page runs out
of capacity.

A page is physically backed by a file on disk and an in-memory index. Backing file is a flat file of serialized log 
records. Index contains offsets to logs on file. It is generally not useful to index every log record, requiring 
significant memory depending on the workload. The index can be made sparse, trading memory for some small penalty 
during random seeks. User defined options allow for fine grained control of this trade off.

A single threaded worker coordinates all write into and reads from storage. Rest of the world interacts with this 
worker using queues (io-uring) and channels (your application). Waldo purely shares state via message passing not
locks/futex. This allows the worker to optimally schedule reads/write for maximum throughput. Pinning this worker 
to a single high priority core might allow better latency, more experiments are necessary.

Every log record must contain a monotonically increasing sequence number to uniquely identify the record, sequence
number of previous log record and an associated payload. Logs must be appended in order and without gaps where the 
previous sequence number creates an implicit "append iff previous == last committed record in storage" condition. 
Conversely queried logs are always returned in order and without gaps. Log payload can be any arbitrary blob of bytes.
There is currently a maximum size limit of 1 MB per log, but that's really only to have some realistic practical limit.

When appending a batch of log records, atomicity is only guaranteed for a single log record, not for the entire batch.
For durability guarantees, enable `o_dsync` flag. This should be a great choice for most applications. It only 
meaningfully impacts performance when appending in the order of GB/s, otherwise it makes little to no difference because
everything is non-blocking and lock-free.

Every `open` of Waldo results in parsing and validation of all storage files. Importantly any corruption is
automatically truncated away so that storage always holds a contiguous sequence of valid log records. There is
support to optionally validate integrity of log records when iterating through queried logs. However recommendation 
is instead having your own integrity checking mechanism or even better encrypt your logs - whatever makes sense for
your use case.

Waldo eagerly allocates most memory it requires when opened for the first time. This memory is reused across read/write
actions, page rotations, etc. User defined options dictate the amount of memory allocated for the buffer pool. One exception 
currently is heap allocations during construction of oneshot channels. They are relatively infrequent and barely show up 
on profiler, I haven't been motivated enough to optimize it away.

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

## Future Work

Waldo does most of what I want it to do already. The only other planned feature addition is the ability to assign
priority to different streams. Otherwise only expect bug fixes, scheduling improvements and better test coverage, 
just polishing around the edges.

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

## Testing

As of now, we primarily use a combination of property based tests and Miri for testing. To execute these tests simply
run tests using your method of choice, such as `cargo test`. In the future, we plan on correctness proof using tools
such as Kani and Verus, stay tunned.

Note that we use `nextest` to execute tests.

```bash
$ cargo install cargo-nextest
$ cargo nextest run
```

### Coverage

We use `cargo-llvm-cov` with nextest runner to gather code coverage reports. Pro tip, use coverage gutters
vscode extension to view line coverage in vscode.

```bash
$ rustup component add llvm-tools-preview
$ cargo install cargo-llvm-cov

# To generate an HTML report
$ cargo llvm-cov nextest --open

# To generate lcov
$ cargo llvm-cov nextest --lcov --output-path lcov.info
```

## Benchmarks

Benchmarks must be performed on your machine with your workload, otherwise it is meaningless. 

This crate provides benchmark binaries to benchmark Waldo on your hardware (or VM). These can be created by building
the crate with `--features benchmark`. Alternatively they can be executed directly using `cargo run`. Check `--help` 
for available options and defaults.

### Steady state

In this test all the readers are caught up to the tip of storage. They continue to stream from storage with the same 
rate as appends from writer. This is the best case scenario where high file cache hit rate is likely, meaning most 
queries are really memcpy (rather than read from disk).

An example result with Waldo on a Linux VM (4 cores, 8 GB RAM) running on my M1 Mac Pro. Upto 30 GB/s worth of log 
records queried and upto 1 GB/s worth of log records appended. Number of readers that can be supported is inversely 
proportional to rate with which logs are appended.

Here we are going to be appending at relatively low rate of about 10 MB/s with 2500 concurrent read streams.

```bash
$ cargo run --release --bin storage --features benchmark -- \
--ring-size 4 \
--queue-depth 256 \
--pool-size 256 \
--page-capacity 1000000 \
--page-file-capacity-gb 1 \
--page-index-capacity 10000 \
--index-sparse-count 100 \
--index-sparse-bytes 131070 \
--log-size 1000 \
--readers 2500 \
--count 1000000 \
--delay 200 \
--o-dsync

State   | Pending:    0 | Disk: 3,699,108/3.53 GiB | Index: 36,622/572.22 KiB
Writer  | [00:01:36] [10,485/10.24 MiB] [#######################################################] [00:00:00]
Readers | [00:01:36] [26,212,500/25.00 GiB] [###################################################] [00:00:00]
```

Here we are going to be appending as fast as possible with a single read stream.

```bash
cargo run --release --bin storage --features benchmark -- \
--ring-size 4 \
--queue-depth 256 \
--pool-size 256 \
--page-capacity 1000000 \
--page-file-capacity-gb 1 \
--page-index-capacity 10000 \
--index-sparse-count 100 \
--index-sparse-bytes 131070 \
--log-size 1000 \
--readers 1 \
--count 10000000 \
--o-dsync

State   | Pending:    1 | Disk: 3,975,912/3.79 GiB | Index: 39,362/615.03 KiB
Writer  | [00:00:13] [832,509/813.00 MiB] [#####################################################] [00:00:00]
Readers | [00:00:13] [832,457/812.95 MiB] [#####################################################] [00:00:00]
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
