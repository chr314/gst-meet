use std::sync::{Arc, RwLock};

use crate::source::MediaType;

/// Unique per conference and never reused.
pub type StreamId = u64;

/// Passed to the sink factory to identify the stream being activated.
#[derive(Debug, Clone)]
pub struct StreamInfo {
  pub stream_id: StreamId,
  pub participant_id: String,
  pub ssrc: u32,
  pub media_type: MediaType,
  pub source_name: Option<String>,
}

/// Fired when a remote stream is activated or removed.
#[derive(Debug, Clone)]
pub struct StreamEvent {
  pub stream_id: StreamId,
  pub participant_id: String,
  pub ssrc: u32,
  pub media_type: MediaType,
  pub source_name: Option<String>,
  pub kind: StreamEventKind,
  /// Set on a `Removed` caused by an error local to this stream; the rest of the conference keeps running.
  pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamEventKind {
  /// A new stream is active and linked downstream. Use for notification only.
  Added,
  /// Fires once the stream is gone: its per-stream consumer has been drained (or the drain timed out) and
  /// disposed of. It also fires without a preceding `Added` when the activation failed after the sink factory
  /// had returned an element, so that the consumer can drop whatever it created for it.
  ///
  /// The event of every stream that ends with a drain or with the end of the session has been handled by the
  /// application when the drain, or the end of the session, is reported. Only a drain asked for from the
  /// handler of an event itself cannot wait for the events that come after it.
  Removed,
}

/// Only produced in [`ReceiveMode::Encoded`](crate::ReceiveMode::Encoded).
#[derive(Debug, Clone)]
pub enum SyncSample {
  SenderReport {
    ssrc: u32,
    ntp_time: u64,
    rtp_time: u32,
  },
  /// The RTP timestamp of a packet at the depayloader input and the running time of its PTS.
  RtpMapping {
    stream_id: StreamId,
    participant_id: String,
    media_type: MediaType,
    ssrc: u32,
    rtp_time: u32,
    pts_running_time_ns: u64,
  },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
  Checking,
  Connected,
  Failed,
}

pub(crate) type SyncSampleCallback = Arc<dyn Fn(SyncSample) + Send + Sync>;

/// A plain lock, not an async one: samples come from streaming threads.
#[derive(Clone, Default)]
pub(crate) struct SyncSampleSink(Arc<RwLock<Option<SyncSampleCallback>>>);

impl SyncSampleSink {
  pub(crate) fn set(&self, f: SyncSampleCallback) {
    *self.0.write().unwrap_or_else(|e| e.into_inner()) = Some(f);
  }

  pub(crate) fn emit(&self, sample: impl FnOnce() -> SyncSample) {
    let callback = self.0.read().unwrap_or_else(|e| e.into_inner()).clone();
    if let Some(callback) = callback {
      callback(sample());
    }
  }
}
