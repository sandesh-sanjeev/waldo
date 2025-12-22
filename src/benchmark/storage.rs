//! Definition of benchmarks for I/O uring runtime.

use clap::Parser;
use std::time::{Duration, Instant};
use tokio::fs::create_dir_all;
use waldo::{
    log::Log,
    storage::{IndexOptions, Options, Storage},
};

/// Arguments for the I/O runtime benchmark.
#[derive(Parser, Clone)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Path to the test file on disk.
    #[arg(long, default_value = "bench")]
    path: String,

    #[arg(long, default_value = "128")]
    queue_depth: u16,

    #[arg(long, default_value = "128")]
    pool_size: u16,

    /// Numbers of readers in benchmark.
    #[arg(long, default_value = "64")]
    readers: u32,

    /// Size of buffers used for reads and writes in MB.
    #[arg(long, default_value = "2")]
    buf_capacity: usize,

    /// Maximum number of logs to append in benchmark.
    #[arg(long, default_value = "1000000")]
    count: u64,

    #[arg(long, default_value = "10000")]
    batch_size: usize,

    #[arg(long, default_value = "20")]
    delay: u64,

    #[arg(long, default_value = "250")]
    log_data_size: usize,
}

impl From<&Arguments> for Options {
    fn from(value: &Arguments) -> Self {
        Self {
            ring_size: 1,
            huge_buf: true,
            pool_size: value.pool_size,
            queue_depth: value.queue_depth,
            buf_capacity: value.buf_capacity * 1024 * 1024, // From MB
            index_opts: IndexOptions {
                sparse_count: 100,
                sparse_bytes: u16::MAX as usize,
                capacity: (value.count as usize / 100) + 1,
            },
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments.
    let args = Arguments::parse();
    let delay = Duration::from_millis(args.delay);

    // Random log data to use with log records.
    let count = args.count;
    let batch_size = args.batch_size;
    let log_data = vec![5; args.log_data_size];

    // Create directory to store benchmark files.
    create_dir_all(&args.path).await?;

    // Open storage at the given path.
    let storage = Storage::open(&args.path, 0, Options::from(&args))?;

    // Number of workers participating in the benchmark.
    let mut workers = Vec::new();

    // Define writer.
    {
        let storage = storage.clone();
        workers.push(tokio::spawn(async move {
            let mut prev_seq_no = 0;
            let mut interval = tokio::time::interval(delay);
            let mut logs = Vec::with_capacity(batch_size);
            while prev_seq_no < count {
                // To run with a specific cadence.
                interval.tick().await;

                // Logs to write to page.
                logs.clear();
                for _ in 0..batch_size {
                    logs.push(Log::new_borrowed(prev_seq_no + 1, prev_seq_no, &log_data));
                    prev_seq_no += 1;
                }

                // Append the next set of bytes.
                let mut session = storage.session().await;
                session.append(&logs).await?;
            }

            Ok::<_, anyhow::Error>(())
        }));
    }

    // Define all the readers.
    for _ in 0..args.readers {
        let storage = storage.clone();
        workers.push(tokio::spawn(async move {
            let mut prev_seq_no = 0;
            let mut interval = tokio::time::interval(delay);
            while prev_seq_no < count {
                // To run with a specific cadence.
                interval.tick().await;

                // Append the next set of bytes.
                let mut session = storage.session().await;
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

    // For calculating rates.
    let seconds = start.elapsed().as_secs_f64();
    let seconds = if seconds == 0.0 { 1.0 } else { seconds };

    // Total number of bytes in logs.
    let mbps = seconds * (1024.0 * 1024.0);
    let bytes_size = (count * (args.log_data_size as u64 + 20)) as f64;

    // Print stats for writer.
    let rate = count as f64 / seconds;
    let byte_rate = bytes_size / mbps;
    println!("Writer: {rate:.2} Logs/s and {byte_rate:.2} MBps");

    // Print stats for readers.
    if args.readers > 0 {
        let rate = (count as f64 * args.readers as f64) / seconds;
        println!("{} Readers: {rate:.2} Logs/s", args.readers);
    }
    Ok(())
}
