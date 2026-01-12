//! Integration tests for Waldo, also serves as an example for usage.
use anyhow::Result;
use tokio::task::JoinSet;
use waldo::{Log, Options, Waldo};

#[tokio::test]
async fn example() -> Result<()> {
    // Open waldo in a given path.
    let opts = waldo_options();
    let temp_dir = tempdir::TempDir::new("waldo")?;

    // Just to open and close a bunch of times.
    for _ in 0..2 {
        // Open storage in the given path.
        // If there is no waldo initialized in directory, new instance will be created.
        let waldo = Waldo::open(temp_dir.path(), opts).await?;
        let prev = waldo.metadata().await.map(|metadata| metadata.prev_seq_no).unwrap_or(0);

        // Number of logs to append and query from storage.
        let count = 10000;
        let mut workers = JoinSet::new();

        // Spawn a worker to append log into storage.
        let mut sink = waldo.sink().await;
        workers.spawn(async move {
            let mut appended = sink.prev_seq_no().unwrap_or(0);
            for _ in 0..count {
                let log = Log::new_borrowed(appended + 1, appended, b"data");
                sink.push(log.borrow()).await?;
                appended = log.seq_no();
            }

            sink.flush().await?;
            Ok::<_, anyhow::Error>(())
        });

        // Spawn a few workers to stream logs from storage.
        for _ in 0..2 {
            let mut counter = 0;
            let mut observed = prev;
            let mut stream = waldo.stream_after(observed);
            workers.spawn(async move {
                while counter < count {
                    let logs = stream.next().await?;
                    for log in logs.into_iter() {
                        let expected = Log::new_borrowed(observed + 1, observed, b"data");
                        assert_eq!(expected, log);

                        counter += 1;
                        observed = log.seq_no();
                    }
                }

                Ok::<_, anyhow::Error>(())
            });
        }

        // Wait for workers to complete and close.
        while let Some(worker) = workers.join_next().await {
            worker??;
        }
        waldo.close().await
    }

    Ok(())
}

fn waldo_options() -> Options {
    Options {
        ring_size: 4,
        queue_depth: 4,
        pool_size: 4,
        huge_buf: false,
        buf_capacity: 8 * 1024,
        page_capacity: 500,
        index_capacity: 50,
        index_sparse_bytes: 1024,
        index_sparse_count: 10,
        file_o_dsync: true,
        file_capacity: 4 * 1024 * 1024,
    }
}
