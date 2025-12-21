# Waldo

[![Build Status][build-img]][build-url]
[![Documentation][doc-img]][doc-url]

[build-img]: https://github.com/sandesh-sanjeev/waldo/actions/workflows/ci.yml/badge.svg?branch=master
[build-url]: https://github.com/sandesh-sanjeev/waldo/actions/workflows/ci.yml

[doc-img]: https://img.shields.io/badge/crate-doc-green?style=flat
[doc-url]: https://sandesh-sanjeev.github.io/waldo/waldo/index.html

## RLIMIT_MEMLOCK

This crate uses io-uring to drive async I/O operations. Registering buffers with io-uring allows for better
performance by allowing the kernel to hold long term references to shared memory. Unfortunately this also
counts against users memlock limit. Users who wish to take advantage of this, and users are encouraged to, 
should increase memlock limits appropriately.

On ArchLinux, default was set at 8 MB. After much struggle, I figured out that I had to use systemd to configure 
limits for my user (`/etc/systemd/user.conf`). But Ubuntu VM on my Mac needed `/etc/security/limits.conf` update.
You mileage may vary. 

## Huge Pages

For best performance enable huge pages when allocating I/O buffers. 

You'll need to make sure the OS has enough huge pages enabled and available. Also note that to lock huge pages in
memory, you also need to raise the memlock limits!

For example, to temporarily set the number of available huge pages:

```bash
$ sudo sysctl -w vm.nr_hugepages=256
```

## Benchmarks

Generate the benchmark binary.

```bash
$ cargo build --release --features benchmark
```

Run benchmark with profiler.

<https://crates.io/crates/flamegraph>

```bash
$ cargo flamegraph --bin bench_runtime --features benchmark --profile bench -- --register-file --register-buf --huge-pages --readers 31
```

To capture kernel events without using sudo, something like this is required:

```bash
$ echo 0 | sudo tee /proc/sys/kernel/kptr_restrict
$ echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```
