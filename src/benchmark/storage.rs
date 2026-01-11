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
        let batch_size = (2 * 1024 * 1024) / value.log_size;
        let delay = value.delay.map(Duration::from_millis);

        Self {
            log_size: value.log_size,
            log_count: value.count,
            batch_size,
            delay,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments.
    let args = Arguments::parse();
    let opts = Options::from(&args);
    let b_opts = BenchOptions::from(&args);

    // Random log data to use with log records.
    let count = args.count;

    // Size of a log record.
    let log_size = 24 + args.log_size;

    // Create directory to store benchmark files.
    create_dir_all(&args.path).await?;

    // Open storage at the given path.
    let storage = Waldo::open(&args.path, opts).await?;

    // Range of log records to append into storage for this benchmark.
    let prev = storage.prev_seq_no().unwrap_or(0);

    // To track progress.
    let readers = args.readers;
    let writer_progress = Arc::new(AtomicU64::new(0));
    let readers_progress = Arc::new(AtomicU64::new(0));
    let workers_progress = Arc::new(AtomicU32::new(readers + 1));

    // Number of workers participating in the benchmark.
    let mut workers = JoinSet::new();

    // Spawn writer.
    let sink = storage.sink().await;
    let writer = Writer::new(sink, b_opts, writer_progress.clone(), workers_progress.clone());
    workers.spawn(writer.run());

    // Spawn readers.
    for _ in 0..args.readers {
        let stream = storage.stream_after(prev);
        let readers = Reader::new(stream, b_opts, readers_progress.clone(), workers_progress.clone());
        workers.spawn(readers.run());
    }

    // Reporter prints to console that is blocking I/O.
    // So spawn a new thread to execute the async task.
    workers.spawn_blocking(move || {
        let reporter = Reporter {
            storage,
            writer: writer_progress,
            readers: readers_progress,
            workers: workers_progress,
        };

        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async { reporter.run(count, readers, log_size).await })
    });

    // Wait for all workers to complete.
    while let Some(worker) = workers.join_next().await {
        worker??;
    }

    Ok(())
}

/// A reporter to report on the progress of benchmark.
struct Reporter {
    storage: Waldo,
    writer: Arc<AtomicU64>,
    readers: Arc<AtomicU64>,
    workers: Arc<AtomicU32>,
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
        while self.workers.load(Ordering::Relaxed) > 0 {
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

            let current = self.writer.load(Ordering::Relaxed);
            let progress = current.saturating_sub(writer_prev);
            writer_prev = current;

            let lps = HumanCount(progress);
            let mbps = HumanBytes(progress * log_size as u64);
            writer.set_message(format!("{lps}/{mbps}"));
            writer.set_position(current);

            let current = self.readers.load(Ordering::Relaxed);
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

/// Options for a benchmark worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BenchOptions {
    log_size: usize,
    log_count: u64,
    batch_size: usize,
    delay: Option<Duration>,
}

/// A writer that appends new log record into storage.
struct Writer {
    sink: Sink,
    data: Vec<u8>,
    opts: BenchOptions,
    counter: Arc<AtomicU64>,
    delay: Option<Interval>,
    done: Arc<AtomicU32>,
}

impl Writer {
    fn new(sink: Sink, opts: BenchOptions, counter: Arc<AtomicU64>, done: Arc<AtomicU32>) -> Self {
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
            done,
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut appended = 0;
        let mut prev_seq_no = self.sink.prev_seq_no().unwrap_or(0);
        while appended < self.opts.log_count {
            // Self throttle if required.
            if let Some(interval) = &mut self.delay {
                interval.tick().await;
            };

            for _ in 0..self.opts.batch_size {
                // Write next log record into sink.
                let seq_no = prev_seq_no + 1;
                let log = Log::new_borrowed(seq_no, prev_seq_no, &self.data);
                self.sink.push(log).await?;

                appended += 1;
                prev_seq_no = seq_no;
            }

            // Make sure buffers logs are flushed to storage.
            self.sink.flush().await?;
            self.counter.fetch_add(self.opts.batch_size as _, Ordering::Relaxed);
        }

        self.done.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }
}

/// A reader that streams new log record from storage.
struct Reader {
    stream: Stream,
    opts: BenchOptions,
    counter: Arc<AtomicU64>,
    done: Arc<AtomicU32>,
}

impl Reader {
    fn new(stream: Stream, opts: BenchOptions, counter: Arc<AtomicU64>, done: Arc<AtomicU32>) -> Self {
        Self {
            stream,
            opts,
            counter,
            done,
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut observed = 0;
        while let Ok(logs) = self.stream.next().await {
            // Consume new log records discovered in stream.
            let mut count = 0;
            for _log in &logs {
                observed += 1;
                count += 1;
            }

            // Break once we have completed all log records.
            self.counter.fetch_add(count, Ordering::Relaxed);
            if observed >= self.opts.log_count {
                break;
            }
        }

        self.done.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }
}
