//! Definition of benchmarks for I/O uring runtime.

use clap::Parser;
use std::fs::{File, OpenOptions};
use std::io::Result;
use std::marker::PhantomData;
use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};
use std::time::Instant;
use waldo::runtime::{BufPool, IoAction, IoBuf, IoFile, IoResponse, IoRuntime, PoolOptions};

/// Arguments for the I/O runtime benchmark.
#[derive(Parser, Clone)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Path to the test file on disk.
    #[arg(long, default_value = "uring.tst")]
    path: String,

    /// Numbers of readers in benchmark.
    #[arg(long, default_value = "4")]
    readers: u32,

    /// Size of buffers used for reads and writes in MB.
    #[arg(long, default_value = "2")]
    buffer_size: u32,

    /// Maximum size of a file in GB.
    #[arg(long, default_value = "8")]
    file_size: u64,

    /// Run benchmark without a writer.
    #[arg(long, action)]
    no_writer: bool,

    /// Register files with the I/O uring runtime.
    #[arg(long, action)]
    register_file: bool,

    /// Register buffers with the I/O uring runtime.
    #[arg(long, action)]
    register_buf: bool,

    /// Enable direct file I/O, as opposed to Buffered I/O.
    #[arg(long, action)]
    direct_io: bool,

    /// Allocate memory using huge pages.
    #[arg(long, action)]
    huge_pages: bool,
}

fn main() -> Result<()> {
    // Parse command line arguments.
    let mut args = Arguments::parse();
    args.buffer_size = args.buffer_size * (1024 * 1024); // From MB
    args.file_size = args.file_size * (1024 * 1024 * 1024); // From GB

    // I/O uring based async runtime.
    let queue_depth = if args.no_writer { args.readers } else { args.readers + 1 };
    let mut runtime = IoRuntime::new(queue_depth)?;

    // Open file for the benchmark.
    let file = bench_file(&args)?;
    let start_file_size = file.metadata()?.len();

    // Register file with runtime, if we have to.
    let (_file, io_file) = if args.register_file {
        let mut files = vec![file.as_raw_fd()];
        let f_files = runtime.register_files(&files)?;
        (files.pop().expect("File should be present"), IoFile::from(f_files[0]))
    } else {
        let io_file = IoFile::from(&file);
        (file.as_raw_fd(), io_file)
    };

    let opts = PoolOptions {
        pool_size: queue_depth as _,
        buf_capacity: args.buffer_size as _,
        huge_buf: args.huge_pages,
    };

    // Create buffer pool for the test.
    let buffer_pool = if args.register_buf {
        BufPool::registered(opts, &mut runtime)?
    } else {
        BufPool::unregistered(opts)?
    };

    // Define the writer, if required.
    let mut writer = None;
    if !args.no_writer {
        writer = Some(Worker::<Writer>::new(io_file, &args));
    }

    // Define all the readers.
    let mut readers = Vec::with_capacity(args.readers as usize);
    for _ in 0..args.readers {
        readers.push(Worker::<Reader>::new(io_file, &args));
    }

    // Run repeatedly till all I/O is completed.
    let start = Instant::now();
    loop {
        // Check if all workers have completed.
        let readers_complete = readers.iter().all(Worker::has_completed);
        let writer_complete = writer.as_ref().map(Worker::has_completed).unwrap_or(true);
        if writer_complete && readers_complete {
            break;
        }

        // Create I/O requests for the writer.
        if let Some(worker) = writer.as_mut()
            && !worker.has_completed()
            && !worker.has_pending_io()
        {
            let bytes = buffer_pool.take();
            let action = worker.next_io(bytes);
            runtime.push(action, usize::MAX).expect("Should have space for all I/O");
        }

        // To save on wasted I/O, readers only read after writer has made enough progress.
        // In other words, we do not try to read beyond the end of the file.
        let file_size = writer.as_ref().map(|worker| worker.offset).unwrap_or(start_file_size);

        // Create I/O requests for readers.
        for (i, worker) in readers.iter_mut().enumerate() {
            if !worker.has_completed() && !worker.has_pending_io() && worker.offset < file_size {
                let bytes = buffer_pool.take();
                let action = worker.next_io(bytes);
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
                        let worker = writer.as_mut().expect("Should have writer");
                        worker.complete_io(result);
                    } else {
                        readers[i].complete_io(result);
                    }
                }
            }
        }
    }

    // For calculating rates.
    let file_size = if args.no_writer {
        start_file_size
    } else {
        args.file_size
    };
    let operations = file_size / u64::from(args.buffer_size);
    let seconds = start.elapsed().as_secs_f64();
    let seconds = if seconds == 0.0 { 1.0 } else { seconds };
    let mbps = 1024.0 * 1024.0 * seconds;

    // Print stats for writer.
    if !args.no_writer {
        let tps = operations as f64 / seconds;
        let rate = args.file_size as f64 / mbps;
        println!("Writer: {tps:.2} TPS, {rate:.2} MB/s");
    }

    // Print stats for readers.
    if args.readers > 0 {
        let tps = (operations as f64 * args.readers as f64) / seconds;
        let rate = (args.file_size as f64 * args.readers as f64) / mbps;
        println!("{} Readers: {tps:.2} TPS, {rate:.2} MB/s", args.readers);
    }
    Ok(())
}

/// Create or open file for benchmark.
fn bench_file(args: &Arguments) -> Result<File> {
    let mut builder = OpenOptions::new();

    // Create file if one doesn't exist on disk.
    builder.create(true);
    builder.write(true);

    // If there is a writer, truncate file to make space for new test.
    if !args.no_writer {
        builder.truncate(true);
    }

    // If there are readers, we need read access for the file.
    if args.readers > 0 {
        builder.read(true);
    }

    // Enable direct file I/O, if requested.
    if args.direct_io {
        builder.custom_flags(libc::O_DIRECT);
    }

    // Finally open the file with all the options.
    builder.open(&args.path)
}

/// A worker who is working as a file writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Writer;

/// A worker who is working as a file reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reader;

/// A worker driving I/O for the benchmark.
struct Worker<'a, R> {
    offset: u64,
    file: IoFile,
    args: &'a Arguments,
    role: PhantomData<R>,
    pending_io: bool,
}

impl<'a, R> Worker<'a, R> {
    fn new(file: IoFile, args: &'a Arguments) -> Self {
        Self {
            file,
            args,
            offset: 0,
            role: PhantomData,
            pending_io: false,
        }
    }

    fn has_completed(&self) -> bool {
        self.offset == self.args.file_size
    }

    fn has_pending_io(&self) -> bool {
        self.pending_io
    }
}

impl Worker<'_, Reader> {
    fn next_io(&mut self, mut buf: IoBuf) -> IoAction {
        buf.set_len(self.args.buffer_size as _);
        self.pending_io = true;
        IoAction::read_at(self.file, self.offset, buf)
    }

    fn complete_io<T>(&mut self, result: IoResponse<T>) {
        self.pending_io = false;
        if result.result == self.args.buffer_size {
            self.offset += self.args.buffer_size as u64;
        }
    }
}

impl Worker<'_, Writer> {
    fn next_io(&mut self, mut buf: IoBuf) -> IoAction {
        buf.set_len(self.args.buffer_size as _);
        self.pending_io = true;
        IoAction::write_at(self.file, self.offset, buf)
    }

    fn complete_io<T>(&mut self, result: IoResponse<T>) {
        self.pending_io = false;
        if result.result == self.args.buffer_size {
            self.offset += self.args.buffer_size as u64;
        }
    }
}
