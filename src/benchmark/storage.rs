//! Definition of benchmarks for I/O uring runtime.

use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::{
    ops::AddAssign,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::fs::create_dir_all;
use waldo::{
    log::Log,
    storage::{Options, PageOptions, PoolOptions, Storage},
};

/// Arguments for the I/O runtime benchmark.
#[derive(Parser, Clone)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Path to the test file on disk.
    #[arg(long, default_value = "bench")]
    path: String,

    /// Maximum number of pages in storage.
    #[arg(long, default_value = "8")]
    ring_size: u32,

    /// Maximum concurrency supported by storage.
    #[arg(long, default_value = "64")]
    queue_depth: u16,

    /// Number of pre-allocated I/O buffers.
    #[arg(long, default_value = "64")]
    pool_size: u16,

    /// Maximum number of logs in a page.
    #[arg(long, default_value = "2000000")]
    page_capacity: u64,

    /// Maximum size of file backing a page in GB.
    #[arg(long, default_value = "1")]
    page_file_capacity_gb: u64,

    /// Maximum size of index backing a page.
    #[arg(long, default_value = "20000")]
    page_index_capacity: usize,

    /// Maximum gap between indexed log records.
    #[arg(long, default_value = "100")]
    index_sparse_count: usize,

    /// Maximum gap in bytes between indexed log records.
    #[arg(long, default_value = "65535")]
    index_sparse_bytes: usize,

    /// Maximum number of logs to append in benchmark.
    #[arg(long, default_value = "10000000")]
    count: u64,

    /// Size of payload of a log record.
    #[arg(long, default_value = "492")]
    log_size: usize,

    /// Numbers of readers in benchmark.
    #[arg(long, default_value = "63")]
    readers: u32,
}

impl From<&Arguments> for Options {
    fn from(value: &Arguments) -> Self {
        Self {
            ring_size: value.ring_size,
            queue_depth: value.queue_depth,
            pool: PoolOptions {
                huge_buf: true,
                pool_size: value.pool_size,
                buf_capacity: 2 * 1024 * 1024,
            },
            page: PageOptions {
                page_capacity: value.page_capacity,
                index_capacity: value.page_index_capacity,
                index_sparse_count: value.index_sparse_count,
                index_sparse_bytes: value.index_sparse_bytes,
                file_capacity: value.page_file_capacity_gb * 1024 * 1024 * 1024,
            },
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments.
    let args = Arguments::parse();
    let opts = Options::from(&args);

    // Random log data to use with log records.
    let count = args.count;

    // Size of a log record.
    let log_size = 20 + args.log_size;
    let log_data = vec![5; args.log_size];

    // Maximum number of log records that can be stored in allocated buffers.
    let batch_size = opts.pool.buf_capacity / log_size;

    // Create directory to store benchmark files.
    create_dir_all(&args.path).await?;

    // Open storage at the given path.
    let storage = Storage::create(&args.path, 0, Options::from(&args))?;

    // To track progress.
    let appended_logs = Arc::new(AtomicU64::new(0));

    // Thread to print progress of benchmark.
    let progress_logs = appended_logs.clone();
    let progress = std::thread::spawn(move || {
        let pb = MultiProgress::new();

        // Define all the progress bars.
        let count_pd = pb.add(ProgressBar::new(count));
        count_pd.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] [{msg} Logs/s][{wide_bar:.cyan/blue}] {pos}/{len}")?
                .progress_chars("#>-"),
        );

        let size_pd = pb.add(ProgressBar::new(count * log_size as u64));
        size_pd.set_style(
            ProgressStyle::default_bar()
                .template("[{eta_precise}] [{msg} MB/s] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}")?
                .progress_chars("#>-"),
        );

        // Keep the progress bar updated while benchmark is in progress.
        let mut prev_progress = 0;
        loop {
            // Figure out progress in benchmark.
            let progress = progress_logs.load(Ordering::Relaxed);
            let new_progress = progress - prev_progress;
            let progress_mbps = (new_progress as f64 * log_size as f64) / (1024.0 * 1024.0);

            // Update the UI with progress.
            count_pd.set_message(format!("{new_progress}"));
            count_pd.set_position(progress);

            size_pd.set_message(format!("{progress_mbps:.2}"));
            size_pd.set_position(progress * log_size as u64);

            // Quit when benchmark is complete.
            if progress >= count {
                break;
            }

            // Sleep for a bit before next iteration.
            prev_progress = progress;
            std::thread::sleep(Duration::from_secs(1));
        }

        Ok::<_, anyhow::Error>(())
    });

    // Number of workers participating in the benchmark.
    let mut workers = Vec::new();

    // Define writer.
    // Write hangs on to the session for the entire duration of benchmark.
    // This makes sure writer makes progress and is not subject to scheduling delays (most part).
    {
        let storage = storage.clone();
        workers.push(tokio::spawn(async move {
            let mut prev_seq_no = 0;
            let mut stats = Stats::default();
            let mut session = storage.session().await;
            let mut logs = Vec::with_capacity(batch_size);
            while prev_seq_no < count {
                // Logs to write to page.
                logs.clear();
                for _ in 0..batch_size {
                    let seq_no = prev_seq_no + 1;
                    logs.push(Log::new_borrowed(seq_no, prev_seq_no, &log_data));
                    prev_seq_no = seq_no;
                }

                // Append the next set of bytes.
                let start = Instant::now();
                session.append(&logs).await?;

                // Update for next iteration.
                stats.appends += 1;
                stats.append_time += start.elapsed();
                appended_logs.fetch_add(batch_size as _, Ordering::Relaxed);
            }

            Ok::<_, anyhow::Error>(stats)
        }));
    }

    // Define all the readers.
    for _ in 0..args.readers {
        let storage = storage.clone();
        workers.push(tokio::spawn(async move {
            let mut prev_seq_no = 0;
            let mut stats = Stats::default();
            let mut session = storage.session().await;
            while prev_seq_no < count {
                // Attempt to fetch the next set of logs.
                let start = Instant::now();
                let logs = session.query(prev_seq_no).await?;

                // Consume logs queried from storage.
                stats.queries += 1;
                stats.query_time += start.elapsed();
                for log in logs {
                    assert_eq!(prev_seq_no, log.prev_seq_no());
                    prev_seq_no = log.seq_no();
                }
            }

            Ok(stats)
        }));
    }

    // Wait for readers and writes to complete.
    let mut stats = Stats::default();
    for worker in workers {
        stats += worker.await??;
    }

    // Wait for the progress bar thread to complete.
    progress.join().expect("Progress bar thread completed in error")?;
    stats.append_stats(count, log_size);
    stats.query_stats(count, log_size, args.readers);
    Ok(())
}

/// Stats from a benchmark worker.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
struct Stats {
    queries: u64,
    appends: u64,
    query_time: Duration,
    append_time: Duration,
}

impl Stats {
    fn append_stats(&self, count: u64, log_size: usize) {
        // Total time spent in storage.
        let seconds = self.append_time.as_secs_f64();
        let seconds = if seconds == 0.0 { 1.0 } else { seconds };

        // Stats for query.
        let tps = self.appends as f64 / seconds;
        let mbps = (count as f64 * log_size as f64) / (seconds * 1024.0 * 1024.0);

        // Calculate avg latency for the operation.
        let appends = if self.appends == 0 { 1 } else { self.appends };
        let latency = self.append_time.as_millis() / appends as u128;
        println!("Writer | {latency} ms, {tps:.2} TPS and {mbps:.2} MB/s");
    }

    fn query_stats(&self, count: u64, log_size: usize, readers: u32) {
        // Total time spent in storage.
        let seconds = self.query_time.as_secs_f64();
        let seconds = if seconds == 0.0 { 1.0 } else { seconds };

        // Stats for query.
        let tps = self.queries as f64 / seconds;

        // Total mbps across all readers.
        let query_time = seconds / readers as f64;
        let total_count = count as f64 * readers as f64;
        let mbps = (total_count * log_size as f64) / (query_time * 1024.0 * 1024.0);

        // Calculate avg latency for the operation.
        let queries = if self.queries == 0 { 1 } else { self.queries };
        let latency = self.query_time.as_millis() / queries as u128;
        println!("Readers {readers}  | {latency} ms, {tps:.2} TPS and {mbps:.2} MB/s");
    }
}

impl AddAssign for Stats {
    fn add_assign(&mut self, rhs: Self) {
        self.queries += rhs.queries;
        self.appends += rhs.appends;
        self.query_time += rhs.query_time;
        self.append_time += rhs.append_time;
    }
}
