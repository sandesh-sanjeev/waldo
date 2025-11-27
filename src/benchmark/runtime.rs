//! Definition of benchmarks for I/O uring runtime.

use clap::Parser;
use std::{
    fs::{File, OpenOptions},
    io::Result,
    marker::PhantomData,
    os::unix::fs::OpenOptionsExt,
    time::Instant,
};
use waldo::runtime::{IoAction, IoBuf, IoFile, IoFixedBuf, IoResponse, IoRuntime};

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

    /// Size of buffers used for reads and writes in bytes.
    #[arg(long, default_value = "2097152")]
    buffer_size: u32,

    /// Maximum size of a file in bytes.
    #[arg(long, default_value = "8589934592")]
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
    let args = Arguments::parse();

    // I/O uring based async runtime.
    let queue_depth = if args.no_writer { args.readers } else { args.readers + 1 };
    let mut runtime = IoRuntime::new(queue_depth)?;

    // Open file for the benchmark.
    let file = bench_file(&args)?;
    let start_file_size = file.metadata()?.len();

    // Register file with runtime, if we have to.
    let (_file, io_file) = if args.register_file {
        let mut files = vec![file];
        let f_files = runtime.register_files(&files)?;
        (files.pop().expect("File should be present"), IoFile::from(f_files[0]))
    } else {
        let io_file = IoFile::from(&file);
        (file, io_file)
    };

    // Allocate all memory for benchmark upfront.
    let mut bufs: Vec<_> = if !args.register_buf {
        (0..queue_depth)
            .map(|_| Vec::with_capacity(args.buffer_size as _))
            .map(BenchBuf::Vec)
            .collect()
    } else if args.huge_pages && !args.register_buf {
        let bufs: Vec<_> = (0..queue_depth)
            .map(|_| IoBuf::allocate(args.buffer_size, true))
            .collect::<Result<Vec<_>>>()?;

        bufs.into_iter().map(BenchBuf::Buf).collect()
    } else {
        let bufs: Vec<_> = (0..queue_depth)
            .map(|_| IoBuf::allocate(args.buffer_size, args.huge_pages))
            .collect::<Result<Vec<_>>>()?;

        // Safety: Buffers exist for the life of this process.
        // Register all the memory we are going to be using with the runtime.
        let bufs = unsafe { runtime.register_bufs(bufs)? };
        bufs.into_iter().map(BenchBuf::Fixed).collect()
    };

    // Define the writer, if required.
    let mut writer = None;
    if !args.no_writer {
        let buf = bufs.pop().expect("Should have allocated buffer");
        writer = Some(Worker::<Writer>::new(io_file, buf, &args));
    }

    // Define all the readers.
    let mut readers = Vec::with_capacity(args.readers as usize);
    for _ in 0..args.readers {
        let buf = bufs.pop().expect("Should have allocated buffer");
        readers.push(Worker::<Reader>::new(io_file, buf, &args));
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
            let action = worker.next_io().expect("No pending I/O");
            runtime.push(action, usize::MAX).expect("Should have space for all I/O");
        }

        // To save on wasted I/O, readers only read after writer has made enough progress.
        // In other words, we do not try to read beyond the end of the file.
        let file_size = writer.as_ref().map(|worker| worker.offset).unwrap_or(start_file_size);

        // Create I/O requests for readers.
        for (i, worker) in readers.iter_mut().enumerate() {
            if !worker.has_completed() && !worker.has_pending_io() && worker.offset < file_size {
                let action = worker.next_io().expect("No pending I/O");
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

/// Different types of buffers used for benchmarks.
enum BenchBuf {
    Buf(IoBuf),
    Vec(Vec<u8>),
    Fixed(IoFixedBuf),
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
    buf: Option<BenchBuf>,
    args: &'a Arguments,
    role: PhantomData<R>,
}

impl<'a, R> Worker<'a, R> {
    fn new(file: IoFile, mut buf: BenchBuf, args: &'a Arguments) -> Self {
        // Make sure all bytes in the buffer are visible.
        // To make that happen, we fill buffer with arbitrary values.
        match &mut buf {
            BenchBuf::Buf(buf) => buf.resize(args.buffer_size as _, 13),
            BenchBuf::Vec(buf) => buf.resize(args.buffer_size as _, 13),
            BenchBuf::Fixed(buf) => buf.resize(args.buffer_size as _, 13),
        }

        Self {
            file,
            args,
            offset: 0,
            buf: Some(buf),
            role: PhantomData,
        }
    }

    fn has_completed(&self) -> bool {
        self.offset == self.args.file_size
    }

    fn has_pending_io(&self) -> bool {
        self.buf.is_none()
    }
}

impl Worker<'_, Reader> {
    fn next_io(&mut self) -> Option<IoAction> {
        let action = match self.buf.take()? {
            BenchBuf::Buf(buf) => IoAction::read_at(self.file, self.offset, buf),
            BenchBuf::Vec(buf) => IoAction::read_at_vec(self.file, self.offset, buf),
            BenchBuf::Fixed(buf) => IoAction::read_at_fixed(self.file, self.offset, buf),
        };

        Some(action)
    }

    fn complete_io<T>(&mut self, result: IoResponse<T>) {
        let buf = match result.action {
            IoAction::Read { buf, .. } => BenchBuf::Buf(buf),
            IoAction::ReadVec { buf, .. } => BenchBuf::Vec(buf),
            IoAction::ReadFixed { buf, .. } => BenchBuf::Fixed(buf),
            other => panic!("Unsupported worker action: {other:?}"),
        };

        self.buf.replace(buf);
        if result.result == self.args.buffer_size {
            self.offset += self.args.buffer_size as u64;
        }
    }
}

impl Worker<'_, Writer> {
    fn next_io(&mut self) -> Option<IoAction> {
        let action = match self.buf.take()? {
            BenchBuf::Buf(buf) => IoAction::write_at(self.file, self.offset, buf),
            BenchBuf::Vec(buf) => IoAction::write_at_vec(self.file, self.offset, buf),
            BenchBuf::Fixed(buf) => IoAction::write_at_fixed(self.file, self.offset, buf),
        };

        Some(action)
    }

    fn complete_io<T>(&mut self, result: IoResponse<T>) {
        let buf = match result.action {
            IoAction::Write { buf, .. } => BenchBuf::Buf(buf),
            IoAction::WriteVec { buf, .. } => BenchBuf::Vec(buf),
            IoAction::WriteFixed { buf, .. } => BenchBuf::Fixed(buf),
            other => panic!("Unsupported worker action: {other:?}"),
        };

        self.buf.replace(buf);
        if result.result == self.args.buffer_size {
            self.offset += self.args.buffer_size as u64;
        }
    }
}
