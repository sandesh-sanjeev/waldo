//! Definition of benchmarks for I/O uring runtime.

use anyhow::Result;
use clap::Parser;
use indicatif::{HumanBytes, HumanCount, MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::Interval;
use tokio::{fs::create_dir_all, time::MissedTickBehavior};
use waldo::{Log, Metadata, Options, Sink, Stream, Waldo};

/// Arguments for the I/O runtime benchmark.
#[derive(Parser, Clone)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Path to the test file on disk.
    #[arg(long, default_value = "bench")]
    path: String,

    /// Enable data sync to disk with every write.
    #[arg(long, action)]
    o_dsync: bool,

    /// Maximum number of pages in storage.
    #[arg(long, default_value = "4")]
    ring_size: u32,

    /// Maximum concurrency supported by storage.
    #[arg(long, default_value = "256")]
    queue_depth: u32,

    /// Number of pre-allocated I/O buffers.
    #[arg(long, default_value = "256")]
    pool_size: u16,

    /// Maximum number of logs in a page.
    #[arg(long, default_value = "1000000")]
    page_capacity: u64,

    /// Maximum size of file backing a page in GB.
    #[arg(long, default_value = "1")]
    page_file_capacity_gb: u64,

    /// Maximum size of index backing a page.
    #[arg(long, default_value = "10000")]
    page_index_capacity: usize,

    /// Maximum gap between indexed log records.
    #[arg(long, default_value = "100")]
    index_sparse_count: usize,

    /// Maximum gap in bytes between indexed log records.
    #[arg(long, default_value = "131070")]
    index_sparse_bytes: usize,

    /// Maximum number of logs to append in benchmark.
    #[arg(long, default_value = "1000000")]
    count: u64,

    /// Size of payload of a log record.
    #[arg(long, default_value = "1000")]
    log_size: usize,

    /// Numbers of readers in benchmark.
    #[arg(long, default_value = "31")]
    readers: u32,

    /// Delay in milliseconds between appends to storage.
    #[arg(long, default_value = None)]
    delay: Option<u64>,
}

impl From<&Arguments> for Options {
    fn from(value: &Arguments) -> Self {
        Self {
            ring_size: value.ring_size,
            queue_depth: value.queue_depth,
            huge_buf: true,
            pool_size: value.pool_size,
            buf_capacity: 2 * 1024 * 1024,
            page_capacity: value.page_capacity,
            index_capacity: value.page_index_capacity,
            index_sparse_bytes: value.index_sparse_bytes,
            index_sparse_count: value.index_sparse_count,
            file_o_dsync: value.o_dsync,
            file_capacity: value.page_file_capacity_gb * 1024 * 1024 * 1024,
        }
    }
}

impl From<&Arguments> for BenchOptions {
    fn from(value: &Arguments) -> Self {
        Self {
            count: value.count,
            log_size: value.log_size,
            delay: value.delay.map(Duration::from_millis),
            batch_size: (2 * 1024 * 1024) / value.log_size,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments.
    let args = Arguments::parse();
    let opts = Options::from(&args);
    let b_opts = BenchOptions::from(&args);

    // Some common configurations.
    let count = args.count;
    let readers = args.readers;
    let log_size = 24 + args.log_size;

    // Create directory to store benchmark files.
    create_dir_all(&args.path).await?;

    // Open storage at the given path.
    let storage = Waldo::open(&args.path, opts).await?;

    // To track progress.
    let counter = Arc::new(Counters::new());

    // Number of workers participating in the benchmark.
    let mut workers = JoinSet::new();

    // Spawn writer.
    let sink = storage.sink().await;
    let writer = Writer::new(sink, b_opts, counter.clone());
    workers.spawn(writer.run());

    // Spawn readers.
    for _ in 0..args.readers {
        let prev = storage.prev_seq_no().unwrap_or(0);
        let stream = storage.stream_after(prev);
        let readers = Reader::new(stream, b_opts, counter.clone());
        workers.spawn(readers.run());
    }

    // Reporter prints to console that is blocking I/O.
    // So spawn a new thread to execute the async task.
    workers.spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let reporter = Reporter { storage, counter };
            reporter.run(count, readers, log_size).await
        })
    });

    // Wait for all workers to complete.
    while let Some(worker) = workers.join_next().await {
        worker??;
    }

    Ok(())
}

/// Options for a benchmark worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BenchOptions {
    log_size: usize,
    count: u64,
    batch_size: usize,
    delay: Option<Duration>,
}

/// A writer that appends new log record into storage.
struct Writer {
    sink: Sink,
    data: Vec<u8>,
    opts: BenchOptions,
    counter: Arc<Counters>,
    delay: Option<Interval>,
}

impl Writer {
    fn new(sink: Sink, opts: BenchOptions, counter: Arc<Counters>) -> Self {
        let data = vec![69; opts.log_size];
        let delay = opts.delay.map(|duration| {
            let mut interval = tokio::time::interval(duration);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            interval
        });

        Self {
            opts,
            sink,
            delay,
            data,
            counter,
        }
    }

    async fn run(mut self) -> Result<()> {
        self.counter.start();

        // Run the writer until complete.
        let mut appended = 0;
        let mut prev_seq_no = self.sink.prev_seq_no().unwrap_or(0);
        while appended < self.opts.count {
            // Self throttle if required.
            if let Some(interval) = &mut self.delay {
                interval.tick().await;
            };

            // Append the next batch of log records.
            for _ in 0..self.opts.batch_size {
                let seq_no = prev_seq_no + 1;
                let log = Log::new_borrowed(seq_no, prev_seq_no, &self.data);
                self.sink.push(log).await?;
                prev_seq_no = seq_no;
            }

            // Make sure buffers logs are flushed to storage.
            self.sink.flush().await?;

            // Update for the next iteration.
            appended += self.opts.batch_size as u64;
            self.counter.add_count(self.opts.batch_size as _, Role::Writer);
        }

        // Tell the reporter that this worker is complete.
        Ok(self.counter.complete())
    }
}

/// A reader that streams new log record from storage.
struct Reader {
    stream: Stream,
    opts: BenchOptions,
    counter: Arc<Counters>,
}

impl Reader {
    fn new(stream: Stream, opts: BenchOptions, counter: Arc<Counters>) -> Self {
        Self { stream, opts, counter }
    }

    async fn run(mut self) -> Result<()> {
        self.counter.start();

        // Run the reader until complete.
        let mut observed = 0;
        while observed < self.opts.count {
            // Get next set of log records.
            let logs = self.stream.next().await?;
            let count = logs.into_iter().count() as u64;

            // Consume the next set of log records.
            observed += count;
            self.counter.add_count(count, Role::Reader);
        }

        // Tell the reporter that this worker is complete.
        Ok(self.counter.complete())
    }
}

/// A reporter to report on the progress of benchmark.
struct Reporter {
    storage: Waldo,
    counter: Arc<Counters>,
}

impl Reporter {
    async fn run(self, count: u64, readers: u32, log_size: usize) -> Result<()> {
        let pb = MultiProgress::new();

        let state = pb.add(ProgressBar::no_length());
        state.set_style(ProgressStyle::with_template("State   | {msg}")?);

        let writer = pb.add(ProgressBar::new(count));
        writer.set_style(
            ProgressStyle::with_template("Writer  | [{elapsed_precise}] [{msg}][{wide_bar:.cyan/blue}] {pos}/{len}")?
                .progress_chars("#>-"),
        );

        let readers = pb.add(ProgressBar::new(count * readers as u64));
        readers.set_style(
            ProgressStyle::with_template("Readers | [{elapsed_precise}] [{msg}][{wide_bar:.cyan/blue}] {pos}/{len}")?
                .progress_chars("#>-"),
        );

        let mut writer_prev = 0;
        let mut readers_prev = 0;
        while self.counter.workers() > 0 {
            let Some(metadata) = self.storage.metadata().await else {
                std::thread::sleep(Duration::from_secs(1));
                continue;
            };

            let Metadata {
                log_count,
                index_count,
                disk_size,
                index_size,
                pending_appends: appends,
                pending_queries: queries,
                pending_fsyncs: fsyns,
                pending_resets: resets,
                ..
            } = metadata;

            let pending_io = appends + queries + fsyns + resets;
            let disks = HumanBytes(disk_size);
            let indexs = HumanBytes(index_size);
            let logs = HumanCount(log_count);
            let entries = HumanCount(index_count);
            state.set_message(format!(
                "Pending: {pending_io: >4} | Disk: {logs}/{disks} | Index: {entries}/{indexs}"
            ));

            let current = self.counter.get_count(Role::Writer);
            let progress = current.saturating_sub(writer_prev);
            writer_prev = current;

            let lps = HumanCount(progress);
            let mbps = HumanBytes(progress * log_size as u64);
            writer.set_message(format!("{lps}/{mbps}"));
            writer.set_position(current);

            let current = self.counter.get_count(Role::Reader);
            let progress = current.saturating_sub(readers_prev);
            readers_prev = current;

            let lps = HumanCount(progress);
            let mbps = HumanBytes(progress * log_size as u64);
            readers.set_message(format!("{lps}/{mbps}"));
            readers.set_position(current);

            std::thread::sleep(Duration::from_secs(1));
        }

        // Complete all the reports.
        state.finish();
        writer.finish();
        readers.finish();

        // Wait for storage to close.
        self.storage.close().await;
        Ok(())
    }
}

/// Different types of workers running benchmarks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Writer,
    Reader,
}

/// Counters that are shared across threads/tasks.
#[derive(Debug)]
struct Counters {
    writer: AtomicU64,
    workers: AtomicU32,
    readers: AtomicU64,
}

impl Counters {
    fn new() -> Self {
        Self {
            writer: AtomicU64::new(0),
            readers: AtomicU64::new(0),
            workers: AtomicU32::new(0),
        }
    }

    fn workers(&self) -> u32 {
        self.workers.load(Ordering::Relaxed)
    }

    fn start(&self) {
        self.workers.fetch_add(1, Ordering::Relaxed);
    }

    fn complete(&self) {
        self.workers.fetch_sub(1, Ordering::Relaxed);
    }

    fn add_count(&self, count: u64, role: Role) {
        match role {
            Role::Writer => self.writer.fetch_add(count, Ordering::Relaxed),
            Role::Reader => self.readers.fetch_add(count, Ordering::Relaxed),
        };
    }

    fn get_count(&self, role: Role) -> u64 {
        match role {
            Role::Writer => self.writer.load(Ordering::Relaxed),
            Role::Reader => self.readers.load(Ordering::Relaxed),
        }
    }
}
