//! Definition of benchmarks for I/O uring runtime.

use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::{fs::create_dir_all, time::MissedTickBehavior};
use waldo::{FileOpts, IndexOpts, Log, Metadata, Options, PageOptions, PoolOptions, Waldo};

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
    #[arg(long, default_value = "131070")]
    index_sparse_bytes: usize,

    /// Maximum number of logs to append in benchmark.
    #[arg(long, default_value = "1000000")]
    count: u64,

    /// Size of payload of a log record.
    #[arg(long, default_value = "1000")]
    log_size: usize,

    /// Numbers of readers in benchmark.
    #[arg(long, default_value = "3200")]
    readers: u32,

    /// Delay between storage actions in milliseconds.
    #[arg(long, default_value = "200")]
    delay: u64,
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
                capacity: value.page_capacity,
                index_opts: IndexOpts {
                    capacity: value.page_index_capacity,
                    sparse_count: value.index_sparse_count,
                    sparse_bytes: value.index_sparse_bytes,
                },
                file_opts: FileOpts {
                    o_dsync: value.o_dsync,
                    capacity: value.page_file_capacity_gb * 1024 * 1024 * 1024,
                },
            },
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments.
    let args = Arguments::parse();
    let opts = Options::from(&args);

    // Amount of time to sleep between iterations.
    let delay = Duration::from_millis(args.delay);

    // Random log data to use with log records.
    let count = args.count;

    // Size of a log record.
    let log_size = 24 + args.log_size;
    let log_data = vec![5; args.log_size];

    // Maximum number of log records that can be stored in allocated buffers.
    let batch_size = opts.pool.buf_capacity / log_size;

    // Create directory to store benchmark files.
    create_dir_all(&args.path).await?;

    // Open storage at the given path.
    let start = Instant::now();
    let storage = Waldo::open(&args.path, Options::from(&args)).await?;
    let state = storage.metadata().await;
    let init_time = start.elapsed().as_secs_f64();

    // Print starting state of storage.
    if let Some(Metadata {
        after_seq_no: after,
        prev_seq_no: prev,
        disk_size: size,
        log_count: count,
        ..
    }) = &state
    {
        let size = (*size as f64) / (1024.0 * 1024.0 * 1024.0);
        println!("Storage | InitTime: {init_time:.2} s | Logs: {count} in ({after}, {prev}] | Size: {size:.2} GB");
    } else {
        println!("Storage | InitTime: {init_time:.2} s | Empty");
    }

    // Print benchmark parameters.
    let readers = args.readers;
    let pool_size = opts.pool.pool_size;
    let queue_depth = opts.queue_depth;
    let write_size = (log_size as f64 * count as f64) / (1024.0 * 1024.0 * 1024.0);
    println!("Bench | BufPoolSize: {pool_size} | QueueDepth: {queue_depth} | Readers: {readers} | Delay: {delay:?}");
    println!("Worker | Logs: {count} | LogSize: {log_size} B | BatchSize: {batch_size} | Total: {write_size:.2} GB");

    // Range of log records to append into storage for this benchmark.
    let prev = state.map(|state| state.prev_seq_no).unwrap_or(0);
    let end = prev + count;

    // To track progress.
    let latest_seq_no = Arc::new(AtomicU64::new(prev));

    // Thread to print progress of benchmark.
    let progress_seq_no = latest_seq_no.clone();
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
        let mut prev_seq_no = prev;
        let mut progress_count = 0;
        loop {
            // Figure out progress in benchmark.
            let latest_seq_no = progress_seq_no.load(Ordering::Relaxed);
            let new_count = latest_seq_no - prev_seq_no;
            let progress_mbps = (new_count as f64 * log_size as f64) / (1024.0 * 1024.0);

            // Update in-memory state.
            prev_seq_no = latest_seq_no;
            progress_count += new_count;

            // Update the UI with progress.
            count_pd.set_message(format!("{new_count:}"));
            count_pd.set_position(progress_count);

            size_pd.set_message(format!("{progress_mbps:.2}"));
            size_pd.set_position(progress_count * log_size as u64);

            // Quit when benchmark is complete.
            if prev_seq_no >= end {
                break;
            }

            // Sleep for a bit before next iteration.
            std::thread::sleep(Duration::from_secs(1));
        }

        // Complete and return.
        count_pd.finish_and_clear();
        size_pd.finish_and_clear();
        Ok::<_, anyhow::Error>(())
    });

    // Number of workers participating in the benchmark.
    let mut workers = Vec::new();

    // Define all the readers.
    for _ in 0..args.readers {
        let storage = storage.clone();
        workers.push(tokio::spawn(async move {
            let mut prev_seq_no = prev;
            let mut interval = tokio::time::interval(delay);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            while prev_seq_no < end {
                // Make sure enough time has passed.
                interval.tick().await;

                // Fetch and process next set of logs.
                let logs = storage.query(prev_seq_no).await?;
                for log in &logs {
                    prev_seq_no = log?.seq_no();
                }
            }

            Ok(())
        }));
    }

    // Define writer.
    // Write hangs on to the session for the entire duration of benchmark.
    // This makes sure writer makes progress and is not subject to scheduling delays (most part).
    {
        let storage = storage.clone();
        workers.push(tokio::spawn(async move {
            let mut prev_seq_no = prev;
            let mut logs = Vec::with_capacity(batch_size);

            let mut interval = tokio::time::interval(delay);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            while prev_seq_no < end {
                // Make sure enough time has passed.
                interval.tick().await;

                // Logs to write to page.
                logs.clear();
                for _ in 0..batch_size {
                    let seq_no = prev_seq_no + 1;
                    logs.push(Log::new_borrowed(seq_no, prev_seq_no, &log_data));
                    prev_seq_no = seq_no;
                }

                // Append the next set of bytes.
                storage.append(&logs).await?;
                latest_seq_no.store(prev_seq_no, Ordering::Relaxed);
            }

            Ok::<_, anyhow::Error>(())
        }));
    }

    // Wait for readers and writes to complete.
    let start = Instant::now();
    for worker in workers {
        worker.await??;
    }

    // Total number of readers in the test.
    let readers = args.readers as f64;

    // Total time spent in storage.
    let time = start.elapsed();
    let seconds = time.as_secs_f64();
    let seconds = if seconds == 0.0 { 1.0 } else { seconds };
    let per_mb_s = seconds * 1024.0 * 1024.0;

    // Total number of things written to storage.
    let w_count = count as f64;
    let r_count = w_count * readers;
    let w_bytes = log_size as f64 * w_count;
    let r_bytes = readers * w_bytes;

    // Calculate rates
    let w_tps = count as f64 / seconds;
    let w_mbps = w_bytes as f64 / per_mb_s;
    let r_tps = r_count as f64 / seconds;
    let r_mbps = r_bytes as f64 / per_mb_s;

    // Wait for the progress bar thread to complete.
    progress.join().expect("Progress bar thread completed in error")?;
    println!("Writer | {seconds:.2}s | {w_tps:.2} Logs/s | {w_mbps:.2} MB/s");
    println!("Readers | {seconds:.2}s | {r_tps:.2} Logs/s | {r_mbps:.2} MB/s");
    Ok(())
}
