//! The stream controller: one actor task per session that owns every decision about which rtpbin pad feeds
//! which consumer.
//!
//! rtpbin pads are never parked and never left dangling. Each one ends in a barrier inside the session bin
//! (see `StreamBarrier`) that swallows whatever happens downstream, so the controller is free to link and unlink
//! the session's ghost pad to a chain whenever it wants. All its state is owned by the actor; everything else
//! talks to it through [`StreamsHandle`], and the slow GStreamer work runs on blocking threads and reports back
//! with a command.

use std::{
  collections::{HashMap, HashSet},
  future::Future,
  pin::Pin,
  sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, OnceLock,
  },
  thread::ThreadId,
  time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use gstreamer::{prelude::*, Bin, FlowError, GhostPad, PadProbeId, PadProbeReturn, PadProbeType};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, warn};

use super::{
  chain::{self, Chain, ChainSpec},
  codec::Codec,
  consumer::{remove_element, Consumer, ConsumerRequest, EosWatch},
  elements::StreamBarrier,
  owner::{Owner, StreamPart},
  rtp::SsrcClearer,
  types::{StreamEvent, StreamEventKind, StreamId, StreamInfo, SyncSampleSink},
};
use crate::{
  conference::ReceiveMode,
  source::{MediaType, RemoteSource},
};

/// After a failure the consumer is probably broken and won't see the EOS.
const ERROR_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// How long an old chain gets to flush into the consumer during a codec switch before the swap is forced.
const SWITCH_TIMEOUT: Duration = Duration::from_secs(2);

/// `drain` and `shutdown` return at most this long after their timeout, even if a teardown hangs.
const TEARDOWN_GRACE: Duration = Duration::from_secs(5);

/// How long the streams of a new participant wait for the application to be done with it (its `on_participant`
/// callback may build the bin they are to be linked to) before they are activated regardless.
const PARTICIPANT_READY_TIMEOUT: Duration = Duration::from_secs(2);

/// An rtpbin receive pad: `recv_rtp_src_<session>_<ssrc>_<pt>`. The same SSRC can legitimately appear on several
/// pads over time as its payload type changes.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(super) struct PadKey {
  pub(super) ssrc: u32,
  pub(super) pt: u8,
}

impl PadKey {
  pub(super) fn parse(pad_name: &str) -> Option<Self> {
    let mut parts = pad_name.strip_prefix("recv_rtp_src_")?.split('_');
    let _session: u32 = parts.next()?.parse().ok()?;
    Some(Self {
      ssrc: parts.next()?.parse().ok()?,
      pt: parts.next()?.parse().ok()?,
    })
  }
}

/// The session-bin side of an rtpbin pad: the pad, its barrier and the ghost pad that leaves the session bin.
pub(super) struct PadEntry {
  pub(super) key: PadKey,
  /// In order of creation.
  pub(super) seq: u64,
  pub(super) rtpbin_pad: gstreamer::Pad,
  /// `clear-ssrc` has been issued: rtpbin is about to remove the pad.
  pub(super) clearing: bool,
  pub(super) barrier: StreamBarrier,
  pub(super) ghost: GhostPad,
}

impl PadEntry {
  /// Blocks.
  fn dispose(self, session_bin: &Bin) {
    if let Some(sink) = self.barrier.static_pad("sink") {
      if let Some(peer) = sink.peer() {
        let _ = peer.unlink(&sink);
      }
    }
    if let Err(e) = self.ghost.set_active(false) {
      warn!("failed to deactivate {}: {:?}", self.ghost.name(), e);
    }
    if let Err(e) = session_bin.remove_pad(&self.ghost) {
      warn!("failed to remove {}: {:?}", self.ghost.name(), e);
    }
    remove_element(session_bin, &self.barrier);
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureOrigin {
  /// An element of the chain of this generation posted an error.
  Chain(u64),
  /// An element of the stream's own consumer posted an error.
  Consumer,
  /// Something downstream of the barrier returned a failing flow.
  Flow,
}

#[derive(Clone)]
pub(super) struct StreamHooks {
  pub(super) stream_event: Arc<dyn Fn(StreamEvent) + Send + Sync>,
  pub(super) sink_factory: Arc<dyn Fn(StreamInfo) -> Option<gstreamer::Element> + Send + Sync>,
  pub(super) has_stream_handler: Arc<dyn Fn() -> bool + Send + Sync>,
  pub(super) participant_ready: ParticipantReady,
  pub(super) sync_sample: SyncSampleSink,
}

/// Called when a source of a participant the session has not seen yet is announced. The future completes once the
/// application has had its say about the participant. The streams of the participant are not activated before.
pub(crate) type ParticipantReady = Arc<dyn Fn(&str) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub(super) struct StreamsParams {
  pub(super) session: u64,
  pub(super) pipeline: gstreamer::Pipeline,
  pub(super) session_bin: Bin,
  pub(super) clearer: Arc<SsrcClearer>,
  pub(super) codecs: Vec<Codec>,
  pub(super) mode: ReceiveMode,
  pub(super) video_scale: (u16, u16),
  pub(super) drain_timeout: Duration,
  pub(super) stream_ids: Arc<AtomicU64>,
  pub(super) hooks: StreamHooks,
}

impl StreamsParams {
  fn owner(&self, stream_id: StreamId, part: StreamPart) -> Owner {
    Owner::Stream {
      session: self.session,
      stream_id,
      part,
    }
  }

  fn chain_spec<'a>(
    &self,
    codec: &'a Codec,
    media_type: MediaType,
    stream_id: StreamId,
    generation: u64,
  ) -> ChainSpec<'a> {
    ChainSpec {
      codec,
      media_type,
      mode: self.mode,
      video_scale: self.video_scale,
      session: self.session,
      stream_id,
      generation,
    }
  }

  fn chain_name(stream_id: StreamId, generation: u64) -> String {
    format!("chain_{stream_id}_{generation}")
  }
}

type DoneReceiver = watch::Receiver<Option<bool>>;

enum Cmd {
  SourcesAdded(Vec<RemoteSource>),
  SourcesRemoved(Vec<u32>),
  ParticipantLeft(String),
  ParticipantReady(String),
  /// Activate whatever can be activated now: a sink factory or a stream handler may have been registered.
  Reevaluate,
  PadAdded(PadEntry),
  PadRemoved {
    key: PadKey,
    pad: gstreamer::Pad,
  },
  PadData(PadKey),
  PadFailed {
    key: PadKey,
    flow: FlowError,
    at: Instant,
  },
  /// Resolves once the stream events queued so far have been handed to the application.
  FlushEvents {
    reply: oneshot::Sender<()>,
  },
  StreamFailed {
    stream_id: StreamId,
    origin: FailureOrigin,
    message: String,
  },
  Activated {
    stream_id: StreamId,
    result: std::result::Result<Activation, ActivationFailure>,
  },
  Switched {
    stream_id: StreamId,
    result: Box<SwitchResult>,
  },
  Retired {
    stream_id: StreamId,
    drained: bool,
  },
  Drain {
    timeout: Duration,
    reply: oneshot::Sender<Vec<DoneReceiver>>,
  },
  Shutdown {
    timeout: Duration,
    /// Whether the reply waits for the application to have seen the events (see `StreamsHandle::shutdown`).
    flush: bool,
    reply: oneshot::Sender<()>,
  },
}

enum EventMsg {
  Event(StreamEvent),
  /// Answered when everything before it has been handed over.
  Flush(oneshot::Sender<()>),
}

#[derive(Clone)]
pub(super) struct StreamsHandle {
  tx: mpsc::UnboundedSender<Cmd>,
  /// The thread that runs the application's `stream_event` hook.
  event_thread: Arc<OnceLock<ThreadId>>,
  #[cfg(test)]
  chain_generations: Arc<AtomicU64>,
}

pub(super) fn spawn(params: StreamsParams) -> StreamsHandle {
  let (tx, rx) = mpsc::unbounded_channel();
  let event_thread = Arc::new(OnceLock::new());
  let chain_generations = Arc::new(AtomicU64::new(0));
  let handle = StreamsHandle {
    tx: tx.clone(),
    event_thread: event_thread.clone(),
    #[cfg(test)]
    chain_generations: chain_generations.clone(),
  };

  // The hook is the application's. Whatever it does, even waiting for the controller, must not keep the
  // controller from answering, so events are handed to it from a thread of its own, in order.
  let (events, mut queued_events) = mpsc::unbounded_channel();
  let stream_event = params.hooks.stream_event.clone();
  tokio::task::spawn_blocking(move || {
    let _ = event_thread.set(std::thread::current().id());
    while let Some(message) = queued_events.blocking_recv() {
      match message {
        EventMsg::Event(event) => stream_event(event),
        EventMsg::Flush(reached) => {
          let _ = reached.send(());
        },
      }
    }
  });

  let actor = Actor {
    params: Arc::new(params),
    tx,
    events,
    chain_generations,
    sources: HashMap::new(),
    pads: HashMap::new(),
    streams: HashMap::new(),
    participants: HashMap::new(),
    warned_pads: HashSet::new(),
    draining: false,
    shutdown_reply: None,
  };
  tokio::spawn(actor.run(rx));
  handle
}

impl StreamsHandle {
  pub(super) fn sources_added(&self, sources: Vec<RemoteSource>) {
    let _ = self.tx.send(Cmd::SourcesAdded(sources));
  }

  pub(super) fn sources_removed(&self, ssrcs: Vec<u32>) {
    let _ = self.tx.send(Cmd::SourcesRemoved(ssrcs));
  }

  pub(super) fn participant_left(&self, participant_id: &str) {
    let _ = self.tx.send(Cmd::ParticipantLeft(participant_id.to_owned()));
  }

  /// Tries again to activate the streams that found no consumer.
  pub(super) fn reevaluate(&self) {
    let _ = self.tx.send(Cmd::Reevaluate);
  }

  pub(super) fn pad_added(&self, entry: PadEntry) {
    let _ = self.tx.send(Cmd::PadAdded(entry));
  }

  pub(super) fn pad_removed(&self, key: PadKey, pad: gstreamer::Pad) {
    let _ = self.tx.send(Cmd::PadRemoved { key, pad });
  }

  pub(super) fn pad_data(&self, key: PadKey) {
    let _ = self.tx.send(Cmd::PadData(key));
  }

  pub(super) fn pad_failed(&self, key: PadKey, flow: FlowError) {
    let _ = self.tx.send(Cmd::PadFailed {
      key,
      flow,
      at: Instant::now(),
    });
  }

  pub(crate) fn stream_failed(&self, stream_id: StreamId, origin: FailureOrigin, message: String) {
    let _ = self.tx.send(Cmd::StreamFailed {
      stream_id,
      origin,
      message,
    });
  }

  /// Sends EOS through every stream that has a consumer of its own and waits for it to arrive, at most
  /// `timeout`. Afterwards no stream is activated any more. The `Removed` events of the streams have been handed
  /// to the application's hook when this returns, unless it is called from that hook: then the ones queued behind
  /// the event being handled are still to come.
  pub(super) async fn drain(&self, timeout: Duration) -> Result<()> {
    let (reply, receivers) = oneshot::channel();
    self
      .tx
      .send(Cmd::Drain { timeout, reply })
      .map_err(|_| anyhow!("the stream controller has stopped"))?;
    let receivers = receivers.await.context("the stream controller has stopped")?;
    let result = wait_for_streams(receivers, timeout).await;
    self.flush_events().await;
    result
  }

  /// Drains, then stops the controller. Like `drain`, it returns after the events have been handed over.
  pub(super) async fn shutdown(&self, timeout: Duration) {
    let (reply, stopped) = oneshot::channel();
    let flush = !self.on_event_thread();
    if self.tx.send(Cmd::Shutdown { timeout, flush, reply }).is_err() {
      return;
    }
    if tokio::time::timeout(timeout + TEARDOWN_GRACE, stopped).await.is_err() {
      warn!(
        "the streams did not shut down {:?} after the drain timeout",
        TEARDOWN_GRACE
      );
    }
  }

  #[cfg(test)]
  pub(super) fn chains_started(&self) -> u64 {
    self.chain_generations.load(Ordering::SeqCst)
  }

  /// Whether this is the thread of the application's `stream_event` hook. Waiting there for the hook to be done
  /// with the events would wait for itself.
  fn on_event_thread(&self) -> bool {
    self.event_thread.get() == Some(&std::thread::current().id())
  }

  async fn flush_events(&self) {
    if self.on_event_thread() {
      return;
    }
    let (reply, flushed) = oneshot::channel();
    if self.tx.send(Cmd::FlushEvents { reply }).is_err() {
      return;
    }
    if tokio::time::timeout(TEARDOWN_GRACE, flushed).await.is_err() {
      warn!(
        "the application's stream event hook is slow: {:?} and still busy",
        TEARDOWN_GRACE
      );
    }
  }
}

async fn wait_for_streams(receivers: Vec<DoneReceiver>, timeout: Duration) -> Result<()> {
  let all_done = async {
    let mut undrained = 0;
    for mut done in receivers {
      match done.wait_for(|result| result.is_some()).await {
        Ok(result) => undrained += usize::from(*result != Some(true)),
        Err(_) => undrained += 1,
      }
    }
    undrained
  };
  match tokio::time::timeout(timeout + TEARDOWN_GRACE, all_done).await {
    Ok(0) => Ok(()),
    Ok(undrained) => bail!("{undrained} stream(s) could not be drained"),
    Err(_) => bail!(
      "streams are still being torn down {:?} after the drain timeout",
      TEARDOWN_GRACE
    ),
  }
}

#[derive(Clone)]
enum Reason {
  SourceRemoved,
  Drain,
  Failure { origin: FailureOrigin, message: String },
}

#[derive(Clone)]
struct Retirement {
  reason: Reason,
  /// How long the consumer gets to see the EOS; the session's drain timeout if not set.
  timeout: Option<Duration>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamState {
  Activating,
  Active,
  Switching,
  Retiring,
}

struct Parts {
  /// `None` when only stream events are wanted.
  chain: Option<Chain>,
  consumer: Consumer,
}

/// The chain a stream moves to while it is `Switching`.
struct Switch {
  target: PadKey,
  /// What the target pad's barrier reported while the new chain was being brought up. The chain that is
  /// installed in the end is the stream's, and so is its failure.
  failure: Option<(FlowError, Instant)>,
}

struct StreamEntry {
  id: StreamId,
  source: RemoteSource,
  participant_id: String,
  pad: PadKey,
  /// The chain whose errors are the stream's: the one it is on, or is moving to. Those of a chain that was
  /// replaced say nothing about the stream.
  chain_generation: u64,
  state: StreamState,
  switch: Option<Switch>,
  parts: Option<Parts>,
  /// Failures the pad's barrier reported before this are about an earlier stream on the same pad.
  linked_at: Instant,
  /// Asked for while the stream was not in a state to be retired.
  pending: Option<Retirement>,
  retirement: Option<Retirement>,
  clear_ssrc: bool,
  done: watch::Sender<Option<bool>>,
}

struct SourceEntry {
  source: RemoteSource,
  /// Payload types whose stream failed or could not be set up. Not used again until the source is announced
  /// again.
  failed_pts: HashSet<u8>,
}

struct Actor {
  params: Arc<StreamsParams>,
  tx: mpsc::UnboundedSender<Cmd>,
  events: mpsc::UnboundedSender<EventMsg>,
  /// Generations are unique among the chains of a session, which is all the attribution of errors needs.
  chain_generations: Arc<AtomicU64>,
  sources: HashMap<u32, SourceEntry>,
  pads: HashMap<PadKey, PadEntry>,
  /// Keyed by SSRC: there is at most one stream per SSRC.
  streams: HashMap<u32, StreamEntry>,
  /// The participants of the session's sources, and whether the application is done with them.
  participants: HashMap<String, bool>,
  warned_pads: HashSet<PadKey>,
  draining: bool,
  /// Answered once the last stream is gone. The flag is whether the answer waits for the events to have been
  /// handed over.
  shutdown_reply: Option<(bool, oneshot::Sender<()>)>,
}

impl Actor {
  async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Cmd>) {
    while let Some(cmd) = rx.recv().await {
      self.handle(cmd);
      if self.shutdown_reply.is_some() && self.streams.is_empty() {
        if let Some((flush, reply)) = self.shutdown_reply.take() {
          // The events of the streams that just ended are queued already; the answer follows them.
          if flush {
            let _ = self.events.send(EventMsg::Flush(reply));
          }
          else {
            let _ = reply.send(());
          }
        }
        break;
      }
    }
    debug!("stream controller of session {} stopped", self.params.session);
  }

  fn handle(&mut self, cmd: Cmd) {
    match cmd {
      Cmd::SourcesAdded(sources) => self.sources_added(sources),
      Cmd::SourcesRemoved(ssrcs) => {
        for ssrc in ssrcs {
          self.source_removed(ssrc);
        }
      },
      Cmd::ParticipantReady(participant_id) => {
        let Some(ready) = self.participants.get_mut(&participant_id) else {
          return;
        };
        *ready = true;
        self.try_activate_participant(&participant_id);
      },
      Cmd::Reevaluate => {
        let ssrcs: Vec<u32> = self.sources.keys().copied().collect();
        for ssrc in ssrcs {
          self.try_activate(ssrc);
        }
      },
      Cmd::ParticipantLeft(participant_id) => {
        self.participants.remove(&participant_id);
        let ssrcs: Vec<u32> = self
          .sources
          .values()
          .filter(|entry| entry.source.participant_id.as_deref() == Some(participant_id.as_str()))
          .map(|entry| entry.source.ssrc)
          .collect();
        for ssrc in ssrcs {
          self.source_removed(ssrc);
        }
      },
      Cmd::PadAdded(entry) => {
        let key = entry.key;
        // rtpbin gives a new pad the name of one it is about to remove when an SSRC is cleared and its packets
        // go on; the old one's removal is reported later.
        if self.pads.contains_key(&key) {
          self.retire_stream_on(key);
          if let Some(old) = self.pads.remove(&key) {
            self.dispose_pad(old);
          }
        }
        self.pads.insert(key, entry);
        self.try_activate(key.ssrc);
      },
      Cmd::PadRemoved { key, pad } => {
        if self.pads.get(&key).is_some_and(|entry| entry.rtpbin_pad == pad) {
          self.retire_stream_on(key);
          if let Some(entry) = self.pads.remove(&key) {
            self.dispose_pad(entry);
          }
        }
      },
      Cmd::PadData(key) => self.pad_data(key),
      Cmd::PadFailed { key, flow, at } => self.pad_failed(key, flow, at),
      Cmd::FlushEvents { reply } => {
        let _ = self.events.send(EventMsg::Flush(reply));
      },
      Cmd::StreamFailed {
        stream_id,
        origin,
        message,
      } => self.stream_failed(stream_id, origin, message),
      Cmd::Activated { stream_id, result } => self.activated(stream_id, result),
      Cmd::Switched { stream_id, result } => self.switched(stream_id, *result),
      Cmd::Retired { stream_id, drained } => self.retired(stream_id, drained),
      Cmd::Drain { timeout, reply } => {
        self.draining = true;
        let _ = reply.send(self.retire_all(timeout));
      },
      Cmd::Shutdown { timeout, flush, reply } => {
        self.draining = true;
        self.shutdown_reply = Some((flush, reply));
        self.retire_all(timeout);
      },
    }
  }

  /// The stream that is fed by `key`, if any, ends: its pad is going away.
  fn retire_stream_on(&mut self, key: PadKey) {
    if self.streams.get(&key.ssrc).is_some_and(|stream| stream.pad == key) {
      self.begin_retire(
        key.ssrc,
        Retirement {
          reason: Reason::SourceRemoved,
          timeout: None,
        },
      );
    }
  }

  fn dispose_pad(&self, entry: PadEntry) {
    let bin = self.params.session_bin.clone();
    tokio::task::spawn_blocking(move || entry.dispose(&bin));
  }

  fn retire_all(&mut self, timeout: Duration) -> Vec<DoneReceiver> {
    let ssrcs: Vec<u32> = self.streams.keys().copied().collect();
    for ssrc in ssrcs {
      self.begin_retire(
        ssrc,
        Retirement {
          reason: Reason::Drain,
          timeout: Some(timeout),
        },
      );
    }
    self.streams.values().map(|stream| stream.done.subscribe()).collect()
  }

  fn sources_added(&mut self, sources: Vec<RemoteSource>) {
    for source in sources {
      let ssrc = source.ssrc;
      if let Some(participant_id) = &source.participant_id {
        if !self.participants.contains_key(participant_id) {
          self.participants.insert(participant_id.clone(), false);
          self.await_participant(participant_id);
        }
      }
      self.sources.insert(
        ssrc,
        SourceEntry {
          source,
          failed_pts: HashSet::new(),
        },
      );
      self.try_activate(ssrc);
    }
  }

  /// The streams of the participant stay down until the application is done with it, or the wait times out. No
  /// stream entry exists in the meantime, so a drain or a shutdown has nothing to wait for.
  fn await_participant(&self, participant_id: &str) {
    let ready = (self.params.hooks.participant_ready)(participant_id);
    let (tx, participant_id) = (self.tx.clone(), participant_id.to_owned());
    tokio::spawn(async move {
      if tokio::time::timeout(PARTICIPANT_READY_TIMEOUT, ready).await.is_err() {
        warn!(
          "participant {} was not ready after {:?}, activating its streams regardless",
          participant_id, PARTICIPANT_READY_TIMEOUT
        );
      }
      let _ = tx.send(Cmd::ParticipantReady(participant_id));
    });
  }

  fn try_activate_participant(&mut self, participant_id: &str) {
    let ssrcs: Vec<u32> = self
      .sources
      .values()
      .filter(|entry| entry.source.participant_id.as_deref() == Some(participant_id))
      .map(|entry| entry.source.ssrc)
      .collect();
    for ssrc in ssrcs {
      self.try_activate(ssrc);
    }
  }

  fn source_removed(&mut self, ssrc: u32) {
    self.sources.remove(&ssrc);
    match self.streams.get_mut(&ssrc) {
      Some(stream) => {
        stream.clear_ssrc = true;
        self.begin_retire(
          ssrc,
          Retirement {
            reason: Reason::SourceRemoved,
            timeout: None,
          },
        );
      },
      None => self.clear_ssrc(ssrc),
    }
  }

  /// rtpbin keeps the jitterbuffer and the pads of an SSRC for as long as it lives (`autoremove` is off); this
  /// frees them. `pad-removed` follows, which removes the barrier and the ghost pad.
  fn clear_ssrc(&mut self, ssrc: u32) {
    let mut any = false;
    for pad in self.pads.values_mut().filter(|pad| pad.key.ssrc == ssrc) {
      pad.clearing = true;
      any = true;
    }
    if !any {
      return;
    }
    self.params.clearer.request(ssrc);
  }

  fn codec_for(&self, pt: u8, media_type: MediaType) -> Option<&Codec> {
    self
      .params
      .codecs
      .iter()
      .find(|codec| codec.is(pt) && codec.is_audio() == media_type.is_audio())
  }

  /// The pad of `ssrc` that most recently had data, then the newest one. Payload types we cannot receive and the
  /// ones that failed are skipped.
  fn choose_pad(&self, ssrc: u32, entry: &SourceEntry) -> Option<PadKey> {
    self
      .pads
      .values()
      .filter(|pad| pad.key.ssrc == ssrc && !pad.clearing && !entry.failed_pts.contains(&pad.key.pt))
      .filter(|pad| self.codec_for(pad.key.pt, entry.source.media_type).is_some())
      .max_by_key(|pad| (pad.barrier.last_data(), pad.seq))
      .map(|pad| pad.key)
  }

  fn try_activate(&mut self, ssrc: u32) {
    if self.draining || self.streams.contains_key(&ssrc) {
      return;
    }
    let Some(source_entry) = self.sources.get(&ssrc) else {
      return;
    };
    let Some(participant_id) = source_entry.source.participant_id.clone() else {
      return;
    };
    if self.participants.get(&participant_id) != Some(&true) {
      return;
    }
    let Some(pad_key) = self.choose_pad(ssrc, source_entry) else {
      return;
    };
    let source = source_entry.source.clone();
    let Some(codec) = self.codec_for(pad_key.pt, source.media_type).cloned() else {
      return;
    };
    let Some(ghost) = self.pads.get(&pad_key).map(|pad| pad.ghost.clone()) else {
      return;
    };

    let stream_id = self.params.stream_ids.fetch_add(1, Ordering::SeqCst);
    let generation = self.chain_generations.fetch_add(1, Ordering::SeqCst);
    let (done, _) = watch::channel(None);
    self.streams.insert(
      ssrc,
      StreamEntry {
        id: stream_id,
        source: source.clone(),
        participant_id: participant_id.clone(),
        pad: pad_key,
        chain_generation: generation,
        state: StreamState::Activating,
        switch: None,
        parts: None,
        linked_at: Instant::now(),
        pending: None,
        retirement: None,
        clear_ssrc: false,
        done,
      },
    );

    debug!("activating stream {} for ssrc {} on {:?}", stream_id, ssrc, pad_key);
    let params = self.params.clone();
    let tx = self.tx.clone();
    tokio::task::spawn_blocking(move || {
      let result = activate(&params, stream_id, generation, &source, &participant_id, &codec, &ghost);
      let _ = tx.send(Cmd::Activated { stream_id, result });
    });
  }

  /// A pad whose barrier has nothing linked delivers. This is reported once per period of the barrier, and the
  /// controller can only act on it while the stream is `Active`; what arrives in any other state is looked at
  /// again by [`Self::reconsider`] once the stream is.
  fn pad_data(&mut self, key: PadKey) {
    match self.streams.get(&key.ssrc) {
      None => self.try_activate(key.ssrc),
      Some(stream) if stream.state == StreamState::Active && stream.pad != key => self.consider_switch(key),
      Some(_) => {},
    }
  }

  /// Looks at the pads of the stream of `ssrc` again, as the notifications that came while it could not act on
  /// them are not repeated: if another pad has had data since the one the stream is on, the stream moves.
  fn reconsider(&mut self, ssrc: u32) {
    let Some(stream) = self.streams.get(&ssrc) else {
      return self.try_activate(ssrc);
    };
    if stream.state != StreamState::Active {
      return;
    }
    let Some(entry) = self.sources.get(&ssrc) else {
      return;
    };
    let last_data = |key: PadKey| self.pads.get(&key).and_then(|pad| pad.barrier.last_data());
    let Some(best) = self.choose_pad(ssrc, entry) else {
      return;
    };
    if best != stream.pad && last_data(best) > last_data(stream.pad) {
      self.consider_switch(best);
    }
  }

  fn consider_switch(&mut self, key: PadKey) {
    let Some(stream) = self.streams.get(&key.ssrc) else {
      return;
    };
    let has_chain = stream.parts.as_ref().is_some_and(|parts| parts.chain.is_some());
    if !has_chain {
      return;
    }
    if self
      .sources
      .get(&key.ssrc)
      .is_some_and(|entry| entry.failed_pts.contains(&key.pt))
    {
      return;
    }
    let Some(new_codec) = self.codec_for(key.pt, stream.source.media_type).cloned() else {
      if self.warned_pads.insert(key) {
        info!("ignoring {:?}: payload type {} cannot be received", key, key.pt);
      }
      return;
    };
    self.start_switch(key, new_codec);
  }

  fn start_switch(&mut self, new_pad: PadKey, new_codec: Codec) {
    let ssrc = new_pad.ssrc;
    let Some(new_ghost) = self.pads.get(&new_pad).map(|pad| pad.ghost.clone()) else {
      return;
    };
    let Some(stream) = self.streams.get_mut(&ssrc) else {
      return;
    };
    let Some(old_ghost) = self.pads.get(&stream.pad).map(|pad| pad.ghost.clone()) else {
      return;
    };
    let Some(Parts {
      chain: Some(old_chain),
      consumer,
    }) = stream.parts.take()
    else {
      return;
    };
    let generation = self.chain_generations.fetch_add(1, Ordering::SeqCst);
    stream.state = StreamState::Switching;
    stream.chain_generation = generation;
    stream.switch = Some(Switch {
      target: new_pad,
      failure: None,
    });
    info!(
      "stream {} switches from payload type {} to {}",
      stream.id, stream.pad.pt, new_pad.pt
    );

    let request = SwitchRequest {
      params: self.params.clone(),
      stream_id: stream.id,
      generation,
      participant_id: stream.participant_id.clone(),
      media_type: stream.source.media_type,
      new_codec,
      new_pad,
      new_ghost,
      old_ghost,
      old_chain,
      consumer,
    };
    let tx = self.tx.clone();
    tokio::spawn(async move {
      let stream_id = request.stream_id;
      let result = switch(request).await;
      let _ = tx.send(Cmd::Switched {
        stream_id,
        result: Box::new(result),
      });
    });
  }

  fn stream_ssrc(&self, stream_id: StreamId) -> Option<u32> {
    self
      .streams
      .values()
      .find(|stream| stream.id == stream_id)
      .map(|stream| stream.source.ssrc)
  }

  fn pad_failed(&mut self, key: PadKey, flow: FlowError, at: Instant) {
    let Some(stream) = self.streams.get_mut(&key.ssrc) else {
      return;
    };
    match &mut stream.switch {
      // The chain the stream is about to move to. It is not the stream's until the swap is done, but it will be.
      Some(switch) if switch.target == key => {
        switch.failure.get_or_insert((flow, at));
      },
      // The chain that is on its way out.
      Some(_) if stream.pad == key => {},
      _ => {
        if stream.pad == key && at >= stream.linked_at {
          let id = stream.id;
          self.stream_failed(id, FailureOrigin::Flow, format!("downstream returned {flow:?}"));
        }
      },
    }
  }

  fn stream_failed(&mut self, stream_id: StreamId, origin: FailureOrigin, message: String) {
    let Some(ssrc) = self.stream_ssrc(stream_id) else {
      debug!("ignoring a failure of stream {}, which is gone: {}", stream_id, message);
      return;
    };
    let Some(stream) = self.streams.get(&ssrc) else {
      return;
    };
    if let FailureOrigin::Chain(generation) = origin {
      if generation != stream.chain_generation {
        debug!(
          "ignoring a failure of chain {} of stream {}, which has been replaced: {}",
          generation, stream_id, message
        );
        return;
      }
    }
    if stream.state == StreamState::Retiring {
      return;
    }
    warn!("stream {} (ssrc {}) failed: {}", stream_id, ssrc, message);
    // A stream that is switching has its new chain's payload type, not the old one's, in its pending failure.
    let pt = stream.switch.as_ref().map_or(stream.pad.pt, |switch| switch.target.pt);
    if let Some(source) = self.sources.get_mut(&ssrc) {
      source.failed_pts.insert(pt);
    }
    self.begin_retire(
      ssrc,
      Retirement {
        reason: Reason::Failure { origin, message },
        timeout: None,
      },
    );
  }

  fn event(stream: &StreamEntry, kind: StreamEventKind, error: Option<String>) -> EventMsg {
    EventMsg::Event(StreamEvent {
      stream_id: stream.id,
      participant_id: stream.participant_id.clone(),
      ssrc: stream.source.ssrc,
      media_type: stream.source.media_type,
      source_name: stream.source.source_name.clone(),
      kind,
      error,
    })
  }

  fn activated(&mut self, stream_id: StreamId, result: std::result::Result<Activation, ActivationFailure>) {
    let Some(ssrc) = self.stream_ssrc(stream_id) else {
      return;
    };
    match result {
      Ok(activation) => {
        let Some(stream) = self.streams.get_mut(&ssrc) else {
          return;
        };
        stream.parts = Some(activation.parts);
        stream.linked_at = activation.linked_at;
        stream.state = StreamState::Active;
        // A failure during the activation is the activation failing: the consumer never gets an `Added`.
        let failed = stream
          .pending
          .as_ref()
          .is_some_and(|pending| matches!(pending.reason, Reason::Failure { .. }));
        if !failed {
          let _ = self.events.send(Self::event(stream, StreamEventKind::Added, None));
        }
        if let Some(pending) = stream.pending.take() {
          self.begin_retire(ssrc, pending);
        }
        self.reconsider(ssrc);
      },
      Err(failure) => {
        let Some(stream) = self.streams.remove(&ssrc) else {
          return;
        };
        let mut try_another_pad = false;
        match failure {
          ActivationFailure::NoConsumer => {
            debug!("nothing consumes ssrc {} of {}", ssrc, stream.participant_id);
          },
          ActivationFailure::Failed {
            message,
            consumer_created,
          } => {
            warn!("failed to activate stream {} (ssrc {}): {}", stream.id, ssrc, message);
            if let Some(source) = self.sources.get_mut(&ssrc) {
              source.failed_pts.insert(stream.pad.pt);
            }
            if consumer_created {
              let _ = self
                .events
                .send(Self::event(&stream, StreamEventKind::Removed, Some(message)));
            }
            try_another_pad = true;
          },
        }
        let _ = stream.done.send(Some(true));
        // The source may be gone in the meantime, with nobody left to free what rtpbin keeps for it.
        if stream.clear_ssrc && !self.sources.contains_key(&ssrc) {
          self.clear_ssrc(ssrc);
        }
        // What other pads of the source delivered meanwhile was not looked at. Each failure rules out its payload
        // type for the source, so this ends.
        if try_another_pad {
          self.try_activate(ssrc);
        }
      },
    }
  }

  fn switched(&mut self, stream_id: StreamId, result: SwitchResult) {
    let Some(ssrc) = self.stream_ssrc(stream_id) else {
      return;
    };
    let Some(stream) = self.streams.get_mut(&ssrc) else {
      return;
    };
    stream.state = StreamState::Active;
    let incoming_failure = stream.switch.take().and_then(|switch| switch.failure);
    let mut failure = None;
    match result {
      SwitchResult::Switched {
        pad,
        chain,
        consumer,
        linked_at,
      } => {
        stream.pad = pad;
        stream.parts = Some(Parts {
          chain: Some(chain),
          consumer,
        });
        stream.linked_at = linked_at;
        // What the new chain's barrier reported while the chain was being brought up is the chain's now.
        failure = incoming_failure.filter(|(_, at)| *at >= linked_at);
      },
      SwitchResult::Kept {
        rejected_pad,
        old_chain,
        consumer,
        message,
      } => {
        warn!("stream {} stays on its payload type: {}", stream_id, message);
        stream.chain_generation = old_chain.generation;
        stream.parts = Some(Parts {
          chain: Some(old_chain),
          consumer,
        });
        if let Some(source) = self.sources.get_mut(&ssrc) {
          source.failed_pts.insert(rejected_pad.pt);
        }
      },
      SwitchResult::Broken { consumer, message } => {
        stream.parts = Some(Parts { chain: None, consumer });
        self.stream_failed(stream_id, FailureOrigin::Flow, message);
        return;
      },
    }
    if let Some((flow, _)) = failure {
      self.stream_failed(stream_id, FailureOrigin::Flow, format!("downstream returned {flow:?}"));
    }
    if let Some(pending) = self.streams.get_mut(&ssrc).and_then(|stream| stream.pending.take()) {
      self.begin_retire(ssrc, pending);
    }
    self.reconsider(ssrc);
  }

  fn begin_retire(&mut self, ssrc: u32, retirement: Retirement) {
    let Some(stream) = self.streams.get_mut(&ssrc) else {
      return;
    };
    match stream.state {
      StreamState::Retiring => return,
      StreamState::Activating | StreamState::Switching => {
        // A failure is more telling than the others.
        let keep = stream
          .pending
          .as_ref()
          .is_some_and(|pending| matches!(pending.reason, Reason::Failure { .. }));
        if !keep {
          stream.pending = Some(retirement);
        }
        return;
      },
      StreamState::Active => {},
    }
    stream.state = StreamState::Retiring;
    stream.retirement = Some(retirement.clone());
    let parts = stream.parts.take();
    let ghost = self.pads.get(&stream.pad).map(|pad| pad.ghost.clone());
    let stream_id = stream.id;
    debug!("retiring stream {} (ssrc {})", stream_id, ssrc);

    let params = self.params.clone();
    let tx = self.tx.clone();
    tokio::spawn(async move {
      let drained = match parts {
        Some(parts) => wind_down(&params, ghost, parts, &retirement).await,
        None => true,
      };
      let _ = tx.send(Cmd::Retired { stream_id, drained });
    });
  }

  fn retired(&mut self, stream_id: StreamId, drained: bool) {
    let Some(ssrc) = self.stream_ssrc(stream_id) else {
      return;
    };
    let Some(stream) = self.streams.remove(&ssrc) else {
      return;
    };
    let error = match stream.retirement.as_ref().map(|retirement| &retirement.reason) {
      Some(Reason::Failure { message, .. }) => Some(message.clone()),
      _ => None,
    };
    let _ = self.events.send(Self::event(&stream, StreamEventKind::Removed, error));
    let _ = stream.done.send(Some(drained));
    if stream.clear_ssrc && !self.sources.contains_key(&ssrc) {
      self.clear_ssrc(ssrc);
    }
    // The source may have been announced again while the stream wound down (a camera switched off and on).
    self.try_activate(ssrc);
  }
}

struct Activation {
  parts: Parts,
  linked_at: Instant,
}

enum ActivationFailure {
  /// No element, no participant bin and no stream handler: nothing to deliver the stream to.
  NoConsumer,
  Failed {
    message: String,
    /// The sink factory returned an element, which its owner may want to hear about.
    consumer_created: bool,
  },
}

/// Blocks.
fn activate(
  params: &StreamsParams,
  stream_id: StreamId,
  generation: u64,
  source: &RemoteSource,
  participant_id: &str,
  codec: &Codec,
  session_ghost: &GhostPad,
) -> std::result::Result<Activation, ActivationFailure> {
  let failed = |e: anyhow::Error, consumer_created| ActivationFailure::Failed {
    message: format!("{e:#}"),
    consumer_created,
  };

  // No sink factory call if the chain cannot be built: that would leave a consumer nothing ever feeds.
  let chain = chain::build(
    &params.chain_spec(codec, source.media_type, stream_id, generation),
    &StreamsParams::chain_name(stream_id, generation),
  )
  .map_err(|e| failed(e, false))?;

  let element = (params.hooks.sink_factory)(StreamInfo {
    stream_id,
    participant_id: participant_id.to_owned(),
    ssrc: source.ssrc,
    media_type: source.media_type,
    source_name: source.source_name.clone(),
  });
  let consumer_created = element.is_some();
  let consumer = Consumer::resolve(ConsumerRequest {
    pipeline: &params.pipeline,
    stream_id,
    participant_id,
    media_type: source.media_type,
    element,
    has_stream_handler: (params.hooks.has_stream_handler)(),
    owner: params.owner(stream_id, StreamPart::Consumer),
  })
  .map_err(|e| failed(e, consumer_created))?
  .ok_or(ActivationFailure::NoConsumer)?;

  if !consumer.needs_chain() {
    return Ok(Activation {
      parts: Parts { chain: None, consumer },
      linked_at: Instant::now(),
    });
  }

  match attach(
    params,
    stream_id,
    source,
    participant_id,
    &chain,
    &consumer,
    session_ghost,
  ) {
    Ok(linked_at) => Ok(Activation {
      parts: Parts {
        chain: Some(chain),
        consumer,
      },
      linked_at,
    }),
    Err(e) => {
      if session_ghost.peer().as_ref() == Some(&chain.sink) {
        let _ = session_ghost.unlink(&chain.sink);
      }
      consumer.unlink(&chain.src);
      consumer.dispose(&params.pipeline);
      remove_element(&params.pipeline, &chain.bin);
      Err(failed(e, consumer_created))
    },
  }
}

fn attach(
  params: &StreamsParams,
  stream_id: StreamId,
  source: &RemoteSource,
  participant_id: &str,
  chain: &Chain,
  consumer: &Consumer,
  session_ghost: &GhostPad,
) -> Result<Instant> {
  params
    .pipeline
    .add(&chain.bin)
    .context("failed to add the chain to the pipeline")?;
  consumer.link(&chain.src)?;
  // The consumer first, so that it is ready when the chain delivers its first buffer.
  if let Some(element) = consumer.per_stream_element() {
    element
      .sync_state_with_parent()
      .context("the consumer did not reach the pipeline's state")?;
  }
  chain
    .bin
    .sync_state_with_parent()
    .context("the chain did not reach the pipeline's state")?;

  if params.mode == ReceiveMode::Encoded {
    chain::install_rtp_mapping_probe(
      &chain.depayloader_sink,
      stream_id,
      participant_id.to_owned(),
      source.media_type,
      source.ssrc,
      params.hooks.sync_sample.clone(),
    );
  }

  let linked_at = Instant::now();
  session_ghost
    .link(&chain.sink)
    .map_err(|e| anyhow!("failed to link the session to the chain: {:?}", e))?;
  if source.media_type.is_video() {
    chain.sink.push_event(chain::force_key_unit_event());
  }
  Ok(linked_at)
}

/// Returns whether the consumer saw the EOS (trivially true for consumers that are not the stream's alone).
async fn wind_down(params: &StreamsParams, ghost: Option<GhostPad>, parts: Parts, retirement: &Retirement) -> bool {
  let Parts { chain, consumer } = parts;
  if let (Some(ghost), Some(chain)) = (&ghost, &chain) {
    if ghost.peer().as_ref() == Some(&chain.sink) {
      let _ = ghost.unlink(&chain.sink);
    }
  }

  let timeout = retirement.timeout.unwrap_or(params.drain_timeout);
  let drained = match (&consumer, &retirement.reason) {
    // A consumer that failed is not going to deal with an EOS.
    (
      Consumer::PerStream(_),
      Reason::Failure {
        origin: FailureOrigin::Consumer,
        ..
      },
    ) => false,
    // The chain is broken, or gone already (a switch that was cut short): the consumer gets its EOS straight away.
    (Consumer::PerStream(element), Reason::Failure { .. }) => {
      if let Some(chain) = &chain {
        consumer.unlink(&chain.src);
      }
      match consumer.sink_pad() {
        Some(sink) => send_eos_and_wait(sink, element, ERROR_DRAIN_TIMEOUT).await,
        None => false,
      }
    },
    // The EOS goes through the chain, behind everything it still holds.
    (Consumer::PerStream(element), _) => match &chain {
      Some(chain) => send_eos_and_wait(chain.sink.clone(), element, timeout).await,
      None => false,
    },
    _ => true,
  };
  if !drained {
    warn!("a consumer did not see the end of its stream within {:?}", timeout);
  }

  let pipeline = params.pipeline.clone();
  let _ = tokio::task::spawn_blocking(move || {
    // The consumer goes first: the chain cannot stop while its streaming thread is stuck in a push into a consumer
    // that stalls, and stopping the consumer ends that push.
    if let Some(chain) = &chain {
      consumer.unlink(&chain.src);
    }
    consumer.dispose(&pipeline);
    if let Some(chain) = chain {
      remove_element(&pipeline, &chain.bin);
    }
  })
  .await;
  drained
}

/// Sending can block while a stalled consumer holds the stream lock; EOS arriving at the sinks is what counts.
async fn send_eos_and_wait(target: gstreamer::Pad, consumer: &gstreamer::Element, timeout: Duration) -> bool {
  let watch = EosWatch::install(consumer);
  tokio::task::spawn_blocking(move || {
    if !target.send_event(gstreamer::event::Eos::new()) {
      warn!("EOS was not handled by {}", target.name());
    }
  });
  watch.wait(timeout).await
}

struct SwitchRequest {
  params: Arc<StreamsParams>,
  stream_id: StreamId,
  generation: u64,
  participant_id: String,
  media_type: MediaType,
  new_codec: Codec,
  new_pad: PadKey,
  new_ghost: GhostPad,
  old_ghost: GhostPad,
  old_chain: Chain,
  consumer: Consumer,
}

enum SwitchResult {
  Switched {
    pad: PadKey,
    chain: Chain,
    consumer: Consumer,
    linked_at: Instant,
  },
  /// The new chain could not be set up; nothing has changed.
  Kept {
    rejected_pad: PadKey,
    old_chain: Chain,
    consumer: Consumer,
    message: String,
  },
  /// The old chain was already cut off when the new one failed.
  Broken { consumer: Consumer, message: String },
}

/// What swapping the chain behind a consumer needs: the old chain's EOS is caught on its way out and replaced by
/// the new chain's output, so the consumer sees new caps and new data but never an end of stream.
struct Swap {
  done: AtomicBool,
  old_src: gstreamer::Pad,
  new_src: gstreamer::Pad,
  consumer_sink: gstreamer::Pad,
  block: Mutex<Option<PadProbeId>>,
}

impl Swap {
  fn perform(&self) {
    if self.done.swap(true, Ordering::SeqCst) {
      return;
    }
    if self.consumer_sink.peer().as_ref() == Some(&self.old_src) {
      let _ = self.old_src.unlink(&self.consumer_sink);
    }
    if let Err(e) = self.new_src.link(&self.consumer_sink) {
      warn!("failed to link the new chain to the consumer: {:?}", e);
    }
    if let Some(block) = self.block.lock().unwrap_or_else(|e| e.into_inner()).take() {
      self.new_src.remove_probe(block);
    }
  }
}

/// A new chain is built, added, held back by a probe and fed by the new pad. The old pad is cut off, an EOS is
/// pushed into the old chain and, once it reaches the end of it, consumer and new chain are joined.
async fn switch(request: SwitchRequest) -> SwitchResult {
  let SwitchRequest {
    params,
    stream_id,
    generation,
    participant_id,
    media_type,
    new_codec,
    new_pad,
    new_ghost,
    old_ghost,
    old_chain,
    consumer,
  } = request;

  let Some(consumer_sink) = consumer.sink_pad() else {
    return SwitchResult::Kept {
      rejected_pad: new_pad,
      old_chain,
      consumer,
      message: "the consumer has no pad".to_owned(),
    };
  };

  let prepared = {
    let params = params.clone();
    let new_ghost = new_ghost.clone();
    let participant_id = participant_id.clone();
    tokio::task::spawn_blocking(move || {
      prepare_chain(
        &params,
        &params.chain_spec(&new_codec, media_type, stream_id, generation),
        &participant_id,
        new_pad,
        &new_ghost,
      )
    })
    .await
  };
  let (new_chain, block, linked_at) = match prepared {
    Ok(Ok(prepared)) => prepared,
    Ok(Err(e)) => {
      return SwitchResult::Kept {
        rejected_pad: new_pad,
        old_chain,
        consumer,
        message: format!("{e:#}"),
      };
    },
    Err(e) => {
      return SwitchResult::Kept {
        rejected_pad: new_pad,
        old_chain,
        consumer,
        message: e.to_string(),
      };
    },
  };

  // From here on the old chain gets no more data.
  if old_ghost.peer().as_ref() == Some(&old_chain.sink) {
    let _ = old_ghost.unlink(&old_chain.sink);
  }

  let swap = Arc::new(Swap {
    done: AtomicBool::new(false),
    old_src: old_chain.src.clone(),
    new_src: new_chain.src.clone(),
    consumer_sink,
    block: Mutex::new(Some(block)),
  });
  let (eos_seen_tx, eos_seen) = oneshot::channel();
  let eos_seen_tx = Mutex::new(Some(eos_seen_tx));
  old_chain.src.add_probe(PadProbeType::EVENT_DOWNSTREAM, {
    let swap = swap.clone();
    move |_, info| {
      if info
        .event()
        .is_some_and(|event| event.type_() == gstreamer::EventType::Eos)
      {
        swap.perform();
        if let Some(tx) = eos_seen_tx.lock().ok().and_then(|mut tx| tx.take()) {
          let _ = tx.send(());
        }
        return PadProbeReturn::Drop;
      }
      PadProbeReturn::Ok
    }
  });
  {
    let old_sink = old_chain.sink.clone();
    tokio::task::spawn_blocking(move || {
      old_sink.send_event(gstreamer::event::Eos::new());
    });
  }
  if tokio::time::timeout(SWITCH_TIMEOUT, eos_seen).await.is_err() {
    warn!(
      "the old chain of stream {} did not finish within {:?}; swapping anyway",
      stream_id, SWITCH_TIMEOUT
    );
    swap.perform();
  }

  {
    let pipeline = params.pipeline.clone();
    let _ = tokio::task::spawn_blocking(move || remove_element(&pipeline, &old_chain.bin)).await;
  }
  if media_type.is_video() {
    new_chain.sink.push_event(chain::force_key_unit_event());
  }
  if swap.consumer_sink.peer().as_ref() != Some(&new_chain.src) {
    let pipeline = params.pipeline.clone();
    let _ = tokio::task::spawn_blocking(move || remove_element(&pipeline, &new_chain.bin)).await;
    return SwitchResult::Broken {
      consumer,
      message: "the new chain could not be linked to the consumer".to_owned(),
    };
  }
  SwitchResult::Switched {
    pad: new_pad,
    chain: new_chain,
    consumer,
    linked_at,
  }
}

/// Blocks. The new chain is up, held back by a probe on its output, and fed by `new_ghost`.
fn prepare_chain(
  params: &StreamsParams,
  spec: &ChainSpec,
  participant_id: &str,
  pad: PadKey,
  new_ghost: &GhostPad,
) -> Result<(Chain, PadProbeId, Instant)> {
  let chain = chain::build(spec, &StreamsParams::chain_name(spec.stream_id, spec.generation))?;
  let result = (|| -> Result<(PadProbeId, Instant)> {
    params
      .pipeline
      .add(&chain.bin)
      .context("failed to add the new chain to the pipeline")?;
    chain
      .bin
      .sync_state_with_parent()
      .context("the new chain did not reach the pipeline's state")?;
    let block = chain
      .src
      .add_probe(PadProbeType::BLOCK_DOWNSTREAM, |_, _| PadProbeReturn::Ok)
      .context("failed to block the new chain")?;
    if params.mode == ReceiveMode::Encoded {
      chain::install_rtp_mapping_probe(
        &chain.depayloader_sink,
        spec.stream_id,
        participant_id.to_owned(),
        spec.media_type,
        pad.ssrc,
        params.hooks.sync_sample.clone(),
      );
    }
    let linked_at = Instant::now();
    new_ghost
      .link(&chain.sink)
      .map_err(|e| anyhow!("failed to link the session to the new chain: {:?}", e))?;
    Ok((block, linked_at))
  })();
  match result {
    Ok((block, linked_at)) => Ok((chain, block, linked_at)),
    Err(e) => {
      remove_element(&params.pipeline, &chain.bin);
      Err(e)
    },
  }
}
