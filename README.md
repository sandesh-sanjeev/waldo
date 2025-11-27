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

## Benchmarks

Generate the benchmark binary.

```bash
$ cargo build --release --features benchmark
```

### io-uring runtime

```bash
$ ./target/release/bench_runtime --help
Arguments for the I/O runtime benchmark

Usage: runtime [OPTIONS]

Options:
      --path <PATH>                Path to the test file on disk [default: uring.tst]
      --readers <READERS>          Numbers of readers in benchmark [default: 4]
      --buffer-size <BUFFER_SIZE>  Size of buffers used for reads and writes in bytes [default: 2097152]
      --file-size <FILE_SIZE>      Maximum size of a file in bytes [default: 8589934592]
      --no-writer                  Run benchmark without a writer
      --register-file              Register files with the I/O uring runtime
      --register-buf               Register buffers with the I/O uring runtime
      --direct-io                  Enable direct file I/O, as opposed to Buffered I/O
      --huge-pages                 Allocate memory using huge pages
  -h, --help                       Print help
  -V, --version                    Print version
```
