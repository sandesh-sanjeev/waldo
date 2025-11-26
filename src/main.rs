use std::{fs::OpenOptions, io::Result, marker::PhantomData, time::Instant};
use waldo::runtime::{IoAction, IoBuf, IoFixedBuf, IoFixedFd, IoResponse, IoRuntime};

const PATH: &str = "tst.uring";

const READERS: u32 = 16;

const QUEUE_DEPTH: u32 = READERS + 1;

const BUFFER_SIZE: u32 = 2 * 1024 * 1024;

const MAX_FILE_SIZE: u64 = 32 * 1024 * 1024 * 1024;

const MAX_APPENDS: u64 = MAX_FILE_SIZE / BUFFER_SIZE as u64;

/// A worker who is working as a file writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Writer;

/// A worker who is working as a file reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reader;

/// A worker driving I/O for the benchmark.
struct Worker<R> {
    offset: u64,
    file: IoFixedFd,
    role: PhantomData<R>,
    buf: Option<IoFixedBuf>,
}

impl<R> Worker<R> {
    fn new(file: IoFixedFd, mut buf: IoFixedBuf) -> Self {
        buf.resize(13, BUFFER_SIZE as usize);

        Self {
            file,
            offset: 0,
            buf: Some(buf),
            role: PhantomData,
        }
    }

    fn has_completed(&self) -> bool {
        self.offset == MAX_FILE_SIZE
    }

    fn has_pending_io(&self) -> bool {
        self.buf.is_none()
    }
}

impl Worker<Reader> {
    fn next_io(&mut self) -> Option<IoAction> {
        let buf = self.buf.take()?;
        Some(IoAction::read_at_fixed(self.file, self.offset, buf))
    }

    fn complete_io(&mut self, result: IoResponse<usize>) {
        let IoAction::ReadFixed { buf, .. } = result.action else {
            panic!("Unsupported io action: {:?}", result.action);
        };

        self.buf.replace(buf);
        if result.result as u32 == BUFFER_SIZE {
            self.offset += BUFFER_SIZE as u64;
        }
    }
}

impl Worker<Writer> {
    fn next_io(&mut self) -> Option<IoAction> {
        let buf = self.buf.take()?;
        Some(IoAction::write_at_fixed(self.file, self.offset, buf))
    }

    fn complete_io(&mut self, result: IoResponse<usize>) {
        let IoAction::WriteFixed { buf, .. } = result.action else {
            panic!("Unsupported io action: {:?}", result.action);
        };

        self.buf.replace(buf);
        if result.result as u32 == BUFFER_SIZE {
            self.offset += BUFFER_SIZE as u64;
        }
    }
}

fn main() -> Result<()> {
    // I/O uring based async runtime.
    let mut runtime = IoRuntime::new(QUEUE_DEPTH)?;

    // File that we are going to be testing with.
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        //.custom_flags(libc::O_DIRECT)
        .open(PATH)?;

    // Register file with the runtime.
    let files = vec![file];
    let f_files = runtime.register_files(&files)?;

    // Allocate all memory upfront.
    let buf: Vec<_> = (0..QUEUE_DEPTH)
        .map(|_| IoBuf::allocate(BUFFER_SIZE))
        .collect::<Result<Vec<_>>>()?;

    // Safety: Buffers exist for the life of this process.
    // Register all the memory we are going to be using with the runtime.
    let mut bufs = unsafe { runtime.register_bufs(buf)? };

    // Define all the benchmark workers.
    let buf = bufs.pop().expect("Should have allocated buffer");
    let mut writer = Worker::<Writer>::new(f_files[0], buf);
    let mut readers = Vec::with_capacity(READERS as usize);
    for _ in 0..READERS {
        let buf = bufs.pop().expect("Should have allocated buffer");
        readers.push(Worker::<Reader>::new(f_files[0], buf));
    }

    // Run repeatedly till all I/O is completed.
    let start = Instant::now();
    while !writer.has_completed() || readers.iter().any(|worker| !worker.has_completed()) {
        // First the writer.
        if !writer.has_completed() && !writer.has_pending_io() {
            let action = writer.next_io().expect("No pending I/O");
            runtime.push(action, usize::MAX).expect("Should have space for all I/O");
        }

        // Then any readers.
        // To save on wasted I/O, readers only read after writer has made enough progress.
        // This is how it would be in our library as well.
        for (i, reader) in readers.iter_mut().enumerate() {
            if !reader.has_completed() && !reader.has_pending_io() && reader.offset < writer.offset {
                let action = reader.next_io().expect("No pending I/O");
                runtime.push(action, i).expect("Should have space for all I/O");
            }
        }

        // Wait for at least one I/O request to complete.
        runtime.submit_and_wait(1)?;

        // Process all the completions.
        while let Some(event) = runtime.pop() {
            match event {
                Err(error) => return Err(error.error),
                Ok(result) => {
                    let i = result.attachment;
                    if i == usize::MAX {
                        writer.complete_io(result);
                    } else {
                        readers[i].complete_io(result);
                    }
                }
            }
        }
    }

    // For calculating rates.
    let seconds = start.elapsed().as_secs_f64();
    let seconds = if seconds == 0.0 { 1.0 } else { seconds };
    let mbps = 1024.0 * 1024.0 * seconds;

    // Print stats for writer.
    let tps = MAX_APPENDS as f64 / seconds;
    let rate = MAX_FILE_SIZE as f64 / mbps;
    println!("Writer: {tps:.2} TPS, {rate:.2} MB/s");

    // Print stats for readers.
    let tps = (MAX_APPENDS as f64 * READERS as f64) / seconds;
    let rate = (MAX_FILE_SIZE as f64 * READERS as f64) / mbps;
    println!("{READERS} Readers: {tps:.2} TPS, {rate:.2} MB/s");
    Ok(())
}
