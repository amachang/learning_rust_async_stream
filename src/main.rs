use std::{
    error,
    io,
    time::{
        Duration,
    },
    pin::{
        Pin,
    },
    task::{
        Context,
        Poll,
    },
};

use futures::{
    AsyncRead,
    AsyncWrite,
    StreamExt,
    TryStreamExt,
    Sink,
    ready,
};

use tokio::{
    net::{
        UnixStream,
    },
};

use tokio_util::{
    compat::{
        TokioAsyncReadCompatExt,
        TokioAsyncWriteCompatExt
    },
};

use tokio_stream::{
    wrappers::{
        IntervalStream,
    },
};

use chrono::{
    DateTime,
    Utc,
    serde::{
        ts_milliseconds,
    },
};

use sysinfo::{
    System,
    SystemExt,
    RefreshKind,
    CpuExt,
    CpuRefreshKind,
};

use serde::{
    Serialize,
    Deserialize,
};

use serde_json::{
    Deserializer,
};

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    #[serde(with = "ts_milliseconds")]
    date_time: DateTime<Utc>,
    cpu_usages: Vec<f32>,
}

impl Record {
    fn new() -> Self {
        let system = System::new_with_specifics(RefreshKind::new().with_cpu(CpuRefreshKind::everything()));
        Self {
            date_time: Utc::now(),
            cpu_usages: system.cpus().iter().map(|c| c.cpu_usage()).collect(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let (tx, rx) = UnixStream::pair()?;

    let sender_task = tokio::spawn(object_stream_to_async_write(tx.compat_write()));
    let receiver_task = tokio::spawn(async_read_to_object_stream(rx.compat()));

    let results = futures::future::join_all(vec![sender_task, receiver_task]).await;
    for r in results {
        r??
    }
    Ok(())
}

async fn object_stream_to_async_write(mut async_write: impl AsyncWrite + Unpin) -> io::Result<()> {

    // IntervalStream implements Stream<Item = Instant>
    let stream = IntervalStream::new(tokio::time::interval(Duration::from_secs(1)));

    // Stream<Item = Instant> into Stream<Item = io::Result<Bytes>> implements TryStream<Ok = Bytes, Error = io::Error>
    let stream = stream.map(|_| -> io::Result<Vec<u8>> {
        Ok((serde_json::to_string(&Record::new())? + "\n").into_bytes())
    });

    // TryStream<Ok = Bytes, Error = io::Error> into AsyncRead
    let async_read = stream.into_async_read();

    // tokio::AsyncRead into futures::AsyncRead by compat method
    futures::io::copy(async_read, &mut async_write).await?;

    Ok(())
}

#[pin_project::pin_project]
pub struct MyAsyncWrite {
    #[pin]
    sink: RecordSink,
    buffer: Vec<u8>,
}

impl MyAsyncWrite
{
    fn new(sink: RecordSink) -> Self {
        Self { sink, buffer: Vec::new() }
    }
}

impl AsyncWrite for MyAsyncWrite
{
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        let mut this = self.project();
        ready!(this.sink.as_mut().poll_ready(cx)?);

        this.buffer.extend_from_slice(buf);
        let deserializer = Deserializer::from_slice(&mut this.buffer);
        let mut deserializer = deserializer.into_iter::<Record>();

        for record in &mut deserializer {
            let record = record?;
            this.sink.as_mut().start_send(record)?
        }

        let n = deserializer.byte_offset();
        *this.buffer = Vec::from(&this.buffer[n..]);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let this = self.project();
        this.sink.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let this = self.project();
        this.sink.poll_close(cx)
    }
}

struct RecordSink {
}

impl Sink<Record> for RecordSink {
    type Error = io::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, record: Record) -> io::Result<()> {
        println!("Record arrived: {:?}", record);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn async_read_to_object_stream(async_read: impl AsyncRead) -> io::Result<()> {
    // Sink
    let sink = RecordSink { };

    // Sink to AsyncWriter
    let mut async_write = MyAsyncWrite::new(sink);

    // tokio::AsyncRead into futures::AsyncRead by compat method
    futures::io::copy(async_read, &mut async_write).await?;

    Ok(())
}













