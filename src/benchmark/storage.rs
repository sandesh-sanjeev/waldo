//! Definition of benchmarks for I/O uring runtime.

use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::{
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
    #[arg(long, default_value = "4")]
    ring_size: u32,

    /// Maximum concurrency supported by storage.
    #[arg(long, default_value = "256")]
    queue_depth: u16,

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
    #[arg(long, default_value = "65535")]
    index_sparse_bytes: usize,

    /// Maximum number of logs to append in benchmark.
    #[arg(long, default_value = "10000000")]
    count: u64,

    /// Size of payload of a log record.
    #[arg(long, default_value = "250")]
    log_size: usize,

    /// Numbers of readers in benchmark.
    #[arg(long, default_value = "255")]
    readers: u32,

    /// Milliseconds between append operations.
    #[arg(long, default_value = "50")]
    append_delay: u64,

    /// Milliseconds between query operations.
    #[arg(long, default_value = "50")]
    query_delay: u64,
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
        let size_pd = pb.add(ProgressBar::new(count * log_size as u64));

        size_pd.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
                .progress_chars("#>-"),
        );

        count_pd.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta}")?
                .progress_chars("#>-"),
        );

        // Keep the progress bar updated while benchmark is in progress.
        loop {
            // Update progress bar.
            let progress = progress_logs.load(Ordering::Relaxed);
            count_pd.set_position(progress);
            size_pd.set_position(progress * log_size as u64);

            // Quit when benchmark is complete.
            if progress >= count {
                break;
            }

            // Sleep for a bit before next iteration.
            std::thread::sleep(Duration::from_secs(1));
        }

        Ok::<_, anyhow::Error>(())
    });

    // Number of workers participating in the benchmark.
    let mut workers = Vec::new();

    // Define writer.
    {
        let storage = storage.clone();
        let delay = Duration::from_millis(args.append_delay);
        workers.push(tokio::spawn(async move {
            let mut prev_seq_no = 0;
            let mut interval = tokio::time::interval(delay);
            let mut logs = Vec::with_capacity(batch_size);
            let mut session = storage.session_async().await;
            while prev_seq_no < count {
                // To run with a specific cadence.
                interval.tick().await;

                // Logs to write to page.
                logs.clear();
                for _ in 0..batch_size {
                    let seq_no = prev_seq_no + 1;
                    logs.push(Log::new_borrowed(seq_no, prev_seq_no, &log_data));
                    prev_seq_no = seq_no;
                }

                // Append the next set of bytes.
                session.append(&logs).await?;
                appended_logs.fetch_add(batch_size as _, Ordering::Relaxed);
            }

            Ok::<_, anyhow::Error>(())
        }));
    }

    // Define all the readers.
    for _ in 0..args.readers {
        let storage = storage.clone();
        let delay = Duration::from_millis(args.query_delay);
        workers.push(tokio::spawn(async move {
            let mut prev_seq_no = 0;
            let mut interval = tokio::time::interval(delay);
            let mut session = storage.session_async().await;
            while prev_seq_no < count {
                // To run with a specific cadence.
                interval.tick().await;

                // Append the next set of bytes.
                for log in session.query(prev_seq_no).await? {
                    assert_eq!(prev_seq_no, log.prev_seq_no());
                    prev_seq_no = log.seq_no();
                }
            }

            Ok(())
        }));
    }

    // Wait for readers and writes to complete.
    let start = Instant::now();
    for worker in workers {
        worker.await??;
    }

    // Wait for the progress bar thread to complete.
    progress.join().expect("Progress bar thread completed in error")?;

    // For calculating rates.
    let seconds = start.elapsed().as_secs_f64();
    let seconds = if seconds == 0.0 { 1.0 } else { seconds };
    let mbps = seconds * (1024.0 * 1024.0);

    // Total number of bytes in logs.
    let bytes_size = (count * (args.log_size as u64 + 20)) as f64;

    // Print stats for writer.
    let rate = count as f64 / seconds;
    let byte_rate = bytes_size / mbps;
    println!("Writer: {rate:.2} Logs/s and {byte_rate:.2} MBps");

    // Print stats for readers.
    if args.readers > 0 {
        let rate = (count as f64 * args.readers as f64) / seconds;
        let byte_rate = (bytes_size * args.readers as f64) / mbps;
        println!("{} Readers: {rate:.2} Logs/s and {byte_rate:.2} MB/s", args.readers);
    }
    Ok(())
}
