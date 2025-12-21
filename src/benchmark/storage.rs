//! Definition of benchmarks for I/O uring runtime.

use clap::Parser;
use std::time::{Duration, Instant};
use tokio::fs::create_dir_all;
use waldo::storage::{Options, Storage};

/// Arguments for the I/O runtime benchmark.
#[derive(Parser, Clone)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Path to the test file on disk.
    #[arg(long, default_value = "bench")]
    path: String,

    #[arg(long, default_value = "128")]
    queue_depth: u16,

    #[arg(long, default_value = "256")]
    pool_size: u16,

    /// Numbers of readers in benchmark.
    #[arg(long, default_value = "127")]
    readers: u32,

    /// Size of buffers used for reads and writes in MB.
    #[arg(long, default_value = "2")]
    buf_capacity: usize,

    /// Maximum size of a file in GB.
    #[arg(long, default_value = "1")]
    file_size: u64,

    #[arg(long, default_value = "20")]
    append_interval: u64,

    #[arg(long, default_value = "20")]
    query_interval: u64,
}

impl From<&Arguments> for Options {
    fn from(value: &Arguments) -> Self {
        Self {
            ring_size: 1,
            huge_buf: true,
            pool_size: value.pool_size,
            queue_depth: value.queue_depth,
            buf_capacity: value.buf_capacity * 1024 * 1024, // From MB
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments.
    let args = Arguments::parse();
    let buf_capacity = args.buf_capacity * (1024 * 1024); // From MB
    let file_size = args.file_size * (1024 * 1024 * 1024); // From GB

    // Create directory to store benchmark files.
    create_dir_all(&args.path).await?;

    // Open storage at the given path.
    let (storage, _worker) = Storage::open(&args.path, Options::from(&args))?;

    // Number of workers participating in the benchmark.
    let mut workers = Vec::new();

    // Define writer.
    {
        let storage = storage.clone();
        let append_delay = Duration::from_millis(args.append_interval);

        workers.push(tokio::spawn(async move {
            let mut offset = 0;
            let mut interval = tokio::time::interval(append_delay);
            while offset < file_size {
                // To run with a specific cadence.
                interval.tick().await;

                // Append the next set of bytes.
                let mut session = storage.session().await;
                offset += session.append().await? as u64;
            }

            Ok::<_, anyhow::Error>(())
        }));
    }

    // Define all the readers.
    for _ in 0..args.readers {
        let storage = storage.clone();
        let query_delay = Duration::from_millis(args.query_interval);
        workers.push(tokio::spawn(async move {
            let mut offset = 0;
            let mut interval = tokio::time::interval(query_delay);
            while offset < file_size {
                // To run with a specific cadence.
                interval.tick().await;

                // Append the next set of bytes.
                let mut session = storage.session().await;
                offset += session.query(offset).await? as u64;
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
    let operations = file_size / buf_capacity as u64;
    let seconds = start.elapsed().as_secs_f64();
    let seconds = if seconds == 0.0 { 1.0 } else { seconds };
    let mbps = 1024.0 * 1024.0 * seconds;

    // Print stats for writer.
    let tps = operations as f64 / seconds;
    let rate = file_size as f64 / mbps;
    println!("Writer: {tps:.2} TPS, {rate:.2} MB/s");

    // Print stats for readers.
    if args.readers > 0 {
        let tps = (operations as f64 * args.readers as f64) / seconds;
        let rate = (file_size as f64 * args.readers as f64) / mbps;
        println!("{} Readers: {tps:.2} TPS, {rate:.2} MB/s", args.readers);
    }
    Ok(())
}
