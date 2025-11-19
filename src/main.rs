use std::{
    fs::OpenOptions,
    io::{Error, ErrorKind, Result},
    time::{Duration, Instant},
};
use waldo::runtime::{IoAction, IoBuf, IoError, IoFile, IoFixedBuf, IoResponse, IoRuntime};

const QUEUE_DEPTH: u32 = 8;

const PATH: &str = "/home/darkstar/waldo/tst.uring";

const BUFFER_SIZE: usize = 1024 * 1024; // 1 MB

const MAX_FILE_SIZE: u64 = 16 * 1024 * 1024 * 1024; // 16 GB

const MAX_APPENDS: u64 = MAX_FILE_SIZE / BUFFER_SIZE as u64;

struct Bencher {
    writer: bool,
    offset: u64,
    pending: bool,
    completed: u64,
    elapsed: Duration,
    io_start: Instant,
    buf: Option<IoFixedBuf>,
}

impl Bencher {
    fn new(writer: bool, buf: IoFixedBuf) -> Self {
        Self {
            writer,
            offset: 0,
            completed: 0,
            pending: false,
            buf: Some(buf),
            io_start: Instant::now(),
            elapsed: Duration::from_secs(0),
        }
    }

    fn next_io(&mut self, file: IoFile) -> Option<IoAction> {
        if self.pending || self.completed >= MAX_APPENDS {
            return None;
        }

        // Use my buffer for the duration of the I/O.
        self.pending = true;
        let buf = self.buf.take().expect("Should have buffer");

        // Perform different actions depending on the type of worker.
        self.io_start = Instant::now();
        if self.writer {
            Some(IoAction::write_at_fixed(file, self.offset, buf))
        } else {
            Some(IoAction::read_at_fixed(file, self.offset, buf))
        }
    }

    fn discard_io(&mut self, error: IoError<usize>) {
        self.pending = false;
        match error.action {
            IoAction::ReadFixed { buf, .. } => {
                self.buf.replace(buf);
            }

            IoAction::WriteFixed { buf, .. } => {
                self.buf.replace(buf);
            }

            _ => panic!("Unsupported io action"),
        }
    }

    fn complete_io(&mut self, result: IoResponse<usize>) {
        self.pending = false;
        self.elapsed += self.io_start.elapsed();
        match result.action {
            IoAction::ReadFixed { buf, .. } => {
                self.buf.replace(buf);
                if result.result as usize == BUFFER_SIZE {
                    self.completed += 1;
                    self.offset += BUFFER_SIZE as u64;
                }
            }

            IoAction::WriteFixed { buf, .. } => {
                self.buf.replace(buf);
                if result.result as usize == BUFFER_SIZE {
                    self.completed += 1;
                    self.offset += BUFFER_SIZE as u64;
                }
            }

            _ => panic!("Unsupported io action"),
        }
    }
}

fn main() -> Result<()> {
    // I/O uring based async runtime.
    let mut storage = IoRuntime::new(QUEUE_DEPTH)?;

    // File that we are going to be testing with.
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        //.custom_flags(libc::O_DIRECT | libc::O_DSYNC)
        .open(PATH)?;

    // Register file with the runtime.
    let files = vec![file];
    let f_files = storage.register_files(&files)?;

    // Allocate all memory upfront.
    let buf: Vec<_> = (0..QUEUE_DEPTH)
        .map(|_| IoBuf::allocate(BUFFER_SIZE))
        .collect::<Result<Vec<_>>>()?;

    // Safety: Buffers exist for the life of this process.
    // Register all the memory we are going to be using with the runtime.
    let bufs = unsafe { storage.register_bufs(buf)? };

    // Define all the benchmark workers.
    let mut workers: Vec<_> = bufs
        .into_iter()
        .enumerate()
        .map(|(i, buf)| {
            if i == 0 {
                Bencher::new(true, buf)
            } else {
                Bencher::new(false, buf)
            }
        })
        .collect();

    // Run repeatedly till all I/O is completed.
    while workers.iter().any(|worker| worker.completed < MAX_APPENDS) {
        // Attempt to fully saturate the io_uring.
        for (i, worker) in workers.iter_mut().enumerate() {
            // Fetch the next io request from the worker.
            if let Some(action) = worker.next_io(f_files[0].into()) {
                // Push the new io request into the runtime.
                // If push results in an error, it means that the runtime is busy.
                // When that happens, the only thing we can do is wait for completions.
                if let Err((attachment, action)) = storage.push(action, i) {
                    let result = IoError {
                        action,
                        attachment,
                        error: Error::new(ErrorKind::ResourceBusy, "Busy"),
                    };

                    worker.discard_io(result);
                    break;
                };
            }
        }

        // Wait for at least one I/O request to complete.
        storage.submit_and_wait(1)?;

        // Process all the completions.
        while let Some(event) = storage.pop() {
            match event {
                Err(error) => return Err(error.error),
                Ok(result) => {
                    // Find the worker this result is for.
                    let i = result.attachment;
                    let worker = &mut workers[i];

                    // Mark the I/O as complete.
                    worker.complete_io(result);
                }
            }
        }
    }

    for worker in workers {
        let writer = worker.writer;
        let seconds = worker.elapsed.as_secs_f64();
        let seconds = if seconds == 0.0 { 1.0 } else { seconds };
        let mbps = 1024.0 * 1024.0 * seconds;

        let total_io = MAX_FILE_SIZE as f64;
        let rate = total_io / mbps;
        println!("({writer}) Worker completed in {seconds:.2}s with rate {rate:.2} MB/s");
    }
    Ok(())
}
