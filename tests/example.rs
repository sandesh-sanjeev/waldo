//! Integration tests for Waldo that serves as an example for usage.

use anyhow::Result;
use tokio::task::LocalSet;
use waldo::{Cursor, Log, Options, Waldo};

#[tokio::test]
async fn getting_started() -> Result<()> {
    // Open waldo in a given path.
    let opts = waldo_options();
    let temp_dir = tempdir::TempDir::new("waldo")?;

    // Number of logs to append and query from storage.
    let count = 500_000;

    // Just to open and close a bunch of times.
    let mut prev = 0;
    for _ in 0..2 {
        // Open storage in the given path.
        // If there is no waldo initialized in directory, new instance will be created.
        let waldo = Waldo::open(temp_dir.path(), opts).await?;
        let metadata = waldo.metadata().await;

        // Make sure sequence numbers match up.
        assert_eq!(waldo.prev_seq_no().unwrap_or(0), prev);
        assert_eq!(metadata.map(|m| m.prev_seq_no).unwrap_or(0), prev);

        // Spawn readers and writer.
        let workers = LocalSet::new();
        workers.spawn_local(appender(waldo.clone(), count));
        for _ in 0..2 {
            workers.spawn_local(reader(waldo.clone(), prev, count));
        }

        // Wait for workers to complete and close.
        workers.await;
        waldo.close().await;

        // For the next iteration.
        prev += count as u64;
    }

    Ok(())
}

/// An appender to push new log records.
async fn appender(storage: Waldo, count: usize) -> Result<()> {
    let mut logs = Vec::with_capacity(1000);
    let mut prev = storage.prev_seq_no().unwrap_or(0);

    // Write a bunch of records into storage.
    let mut remaining = count;
    while remaining > 0 {
        // Next batch of records to append.
        // There must be no gaps in between.
        logs.clear();
        let batch_size = std::cmp::min(remaining, logs.capacity());
        for _ in 0..batch_size {
            logs.push(Log::new_borrowed(prev + 1, prev, b"data5121"));
            prev += 1;
        }

        // Append next set of records.
        storage.append(&logs).await?;
        remaining -= batch_size;
    }

    // Close storage and return.
    Ok(storage.close().await)
}

/// A reader that streams some number of log records.
async fn reader(mut storage: Waldo, after: u64, count: usize) -> Result<()> {
    let mut prev = after;
    let mut counter = 0;

    // Wait for a range of records to be available.
    // Make sure logs contain expected contents.
    while counter < count {
        // (Optional) Wait for storage if have new records.
        storage.watch_for_after(prev).await?;

        // Process the next batch of records.
        let logs = storage.query(Cursor::After(prev)).await?;
        for log in logs.into_iter() {
            log.validate_data()?; // Optional
            assert_eq!(log, Log::new_borrowed(prev + 1, prev, b"data5121"));

            counter += 1;
            prev = log.seq_no();
        }
    }

    // Close storage and return.
    Ok(storage.close().await)
}

fn waldo_options() -> Options {
    Options {
        ring_size: 4,
        queue_depth: 4,
        pool_size: 4,
        huge_buf: false,
        buf_capacity: 2 * 1024 * 1024,
        page_capacity: 100_000,
        index_capacity: 1000,
        index_sparse_bytes: 16 * 1024,
        index_sparse_count: 100,
        file_o_dsync: true,
        file_capacity: 4 * 1024 * 1024,
    }
}
