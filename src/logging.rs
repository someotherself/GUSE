use tracing_subscriber::EnvFilter;

// pub type LogBuffer = Arc<Mutex<Vec<String>>>;

// #[derive(Clone)]
// pub struct BufferWriter(pub LogBuffer);

// impl<'a> MakeWriter<'a> for BufferWriter {
//     type Writer = BufferGuard;

//     fn make_writer(&'a self) -> Self::Writer {
//         BufferGuard(self.0.clone())
//     }
// }

// // The guard that actually implements `Write`
// pub struct BufferGuard(pub LogBuffer);

// impl std::io::Write for BufferGuard {
//     fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
//         let s = String::from_utf8_lossy(buf).to_string();
//         let mut guard = self.0.lock().unwrap();
//         guard.push(s);
//         Ok(buf.len())
//     }
//     fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
// }

// // https://users.rust-lang.org/t/the-best-ring-buffer-library/58489/5
// #[derive(Debug)]
// struct RingBuffer<T> {
//     inner: VecDeque<T>,
// }

// impl<T> RingBuffer<T> {
//     pub fn new(capacity: usize) -> Self {
//         Self {
//             inner: VecDeque::with_capacity(capacity),
//         }
//     }

//     pub fn push(&mut self, item: T) {
//         if self.inner.len() == self.inner.capacity() {
//             self.inner.pop_front();
//             self.inner.push_back(item);
//             debug_assert!(self.inner.len() == self.inner.capacity());
//         } else {
//             self.inner.push_back(item);
//         }
//     }

//     pub fn pop(&mut self) -> Option<T> {
//         self.inner.pop_front()
//     }
// }

pub fn init_logging(verbosity: u8) {
    let level = match verbosity {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    tracing_subscriber::fmt::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

// pub fn init_logging(buffer: LogBuffer) {
//     // Our custom writer, which will append to `buffer`
//     let writer = BufferWriter(buffer.clone());

//     // Only record logs from `fuser` at DEBUG or higher
//     let fuse_layer = tracing_subscriber::fmt::layer()
//         .with_target(true)
//         .with_ansi(false)              // disable colors, it’s UI territory
//         .with_writer(writer)
//         .with_filter(tracing_subscriber::filter::EnvFilter::new("fuser=debug"));

//     // Send your own app logs to stdout as usual
//     // let stdout_layer = tracing_subscriber::fmt::layer()
//     //     .with_filter(tracing_subscriber::filter::EnvFilter::new("info"));

//     tracing_subscriber::registry()
//         .with(fuse_layer)
//         // .with(stdout_layer)
//         .init();
// }
