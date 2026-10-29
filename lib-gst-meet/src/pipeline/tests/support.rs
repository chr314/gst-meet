use std::{
  collections::HashMap,
  net::UdpSocket,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex, Once,
  },
  time::Duration,
};

use gstreamer::prelude::*;
use tokio::sync::mpsc;

use crate::{
  conference::ReceiveMode,
  pipeline::{
    codec::Codec, elements, transport::TransportBins, CodecName, ConferenceMedia, ConnectionState, Fingerprint,
    HashAlgo, MediaConfig, MediaHooks, RemoteIce, RemoteSourceDesc, RtpSession, SessionFailure, SessionOffer,
    StreamEvent, StreamEventKind, StreamId, StreamInfo, SyncSampleSink,
  },
  source::{MediaType, RemoteSource},
};

pub(super) fn init() {
  static INIT: Once = Once::new();
  INIT.call_once(|| {
    gstreamer::init().expect("failed to initialise GStreamer");
    elements::register().expect("failed to register the elements");
  });
}

/// Sets the pipeline to `Null` on drop, also when a test fails: disposing playing elements crashes.
pub(super) struct NullOnDrop(pub(super) gstreamer::Pipeline);

impl Drop for NullOnDrop {
  fn drop(&mut self) {
    let _ = self.0.set_state(gstreamer::State::Null);
  }
}

pub(super) async fn wait_until(what: &str, timeout: Duration, condition: impl Fn() -> bool) {
  let deadline = tokio::time::Instant::now() + timeout;
  while !condition() {
    assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
    tokio::time::sleep(Duration::from_millis(20)).await;
  }
}

/// What reached the sink of a consumer.
#[derive(Clone, Default)]
pub(super) struct Record(Arc<Mutex<RecordInner>>);

#[derive(Default)]
struct RecordInner {
  buffers: usize,
  events: Vec<String>,
}

impl Record {
  pub(super) fn buffers(&self) -> usize {
    self.0.lock().unwrap().buffers
  }

  /// `stream-start`, `caps <caps>`, `segment`, `eos` and so on, in order of arrival.
  pub(super) fn events(&self) -> Vec<String> {
    self.0.lock().unwrap().events.clone()
  }

  pub(super) fn count(&self, kind: &str) -> usize {
    self.events().iter().filter(|event| event.starts_with(kind)).count()
  }

  pub(super) fn has_eos(&self) -> bool {
    self.count("eos") > 0
  }

  fn watch(&self, pad: &gstreamer::Pad) {
    let record = self.0.clone();
    pad.add_probe(
      gstreamer::PadProbeType::BUFFER
        | gstreamer::PadProbeType::BUFFER_LIST
        | gstreamer::PadProbeType::EVENT_DOWNSTREAM,
      move |_, info| {
        let mut record = record.lock().unwrap();
        match &info.data {
          Some(gstreamer::PadProbeData::Buffer(_)) => record.buffers += 1,
          Some(gstreamer::PadProbeData::BufferList(list)) => record.buffers += list.len(),
          Some(gstreamer::PadProbeData::Event(event)) => {
            let description = match event.view() {
              gstreamer::EventView::Caps(caps) => format!("caps {}", caps.caps()),
              gstreamer::EventView::StreamStart(_) => "stream-start".to_owned(),
              gstreamer::EventView::Segment(_) => "segment".to_owned(),
              gstreamer::EventView::Eos(_) => "eos".to_owned(),
              _ => return gstreamer::PadProbeReturn::Ok,
            };
            record.events.push(description);
          },
          _ => {},
        }
        gstreamer::PadProbeReturn::Ok
      },
    );
  }
}

static NEXT_CONSUMER: AtomicUsize = AtomicUsize::new(0);

/// A parentless `queue ! fakesink` as a stream's consumer. The sink does not hold the pipeline's state change
/// back (`async=false`) and does not wait for the clock.
pub(super) fn recording_consumer() -> (gstreamer::Element, Record) {
  let queue = gstreamer::ElementFactory::make("queue").build().unwrap();
  let sink = gstreamer::ElementFactory::make("fakesink")
    .name(format!(
      "consumer_sink_{}",
      NEXT_CONSUMER.fetch_add(1, Ordering::SeqCst)
    ))
    .property("async", false)
    .property("sync", false)
    .build()
    .unwrap();
  let record = Record::default();
  record.watch(&sink.static_pad("sink").unwrap());
  (consumer_bin(&[queue, sink]), record)
}

pub(super) fn consumer_bin(elements: &[gstreamer::Element]) -> gstreamer::Element {
  let bin = gstreamer::Bin::new();
  bin.add_many(elements).unwrap();
  gstreamer::Element::link_many(elements).unwrap();
  let sink = elements[0].static_pad("sink").unwrap();
  bin
    .add_pad(
      &gstreamer::GhostPad::builder_with_target(&sink)
        .unwrap()
        .name("sink")
        .build(),
    )
    .unwrap();
  bin.upcast()
}

type SinkMaker = Box<dyn Fn(&StreamInfo) -> Option<gstreamer::Element> + Send>;
type EventHook = Arc<Mutex<Option<Box<dyn Fn(&StreamEvent) + Send>>>>;

/// What the sink factory is asked and what it hands out.
pub(super) struct Factory {
  pub(super) calls: Mutex<Vec<StreamInfo>>,
  make: Mutex<SinkMaker>,
  pub(super) records: Mutex<HashMap<StreamId, Record>>,
  pub(super) elements: Mutex<HashMap<StreamId, gstreamer::Element>>,
}

impl Factory {
  /// Makes a recording consumer for every stream and keeps its record.
  pub(super) fn recording(self: &Arc<Self>) -> impl Fn(&StreamInfo) -> Option<gstreamer::Element> + Send + 'static {
    let factory = Arc::downgrade(self);
    move |info| {
      let (consumer, record) = recording_consumer();
      factory
        .upgrade()?
        .records
        .lock()
        .unwrap()
        .insert(info.stream_id, record);
      Some(consumer)
    }
  }

  pub(super) fn set(&self, make: impl Fn(&StreamInfo) -> Option<gstreamer::Element> + Send + 'static) {
    *self.make.lock().unwrap() = Box::new(make);
  }

  pub(super) fn record(&self, stream_id: StreamId) -> Record {
    self
      .records
      .lock()
      .unwrap()
      .get(&stream_id)
      .cloned()
      .expect("no consumer was made for the stream")
  }

  pub(super) fn element(&self, stream_id: StreamId) -> gstreamer::Element {
    self
      .elements
      .lock()
      .unwrap()
      .get(&stream_id)
      .cloned()
      .expect("no consumer was made for the stream")
  }
}

#[derive(Default)]
pub(super) struct Observed {
  pub(super) failures: Mutex<Vec<(u64, SessionFailure)>>,
  pub(super) connection_states: Mutex<Vec<(u64, ConnectionState)>>,
  pub(super) fatal: Mutex<Vec<String>>,
  pub(super) participants: Mutex<Vec<String>>,
  /// While it is set and `false`, the application is not done with new participants.
  pub(super) gate: Mutex<Option<tokio::sync::watch::Receiver<bool>>>,
}

pub(super) struct Rig {
  pub(super) media: ConferenceMedia,
  pub(super) events: mpsc::UnboundedReceiver<StreamEvent>,
  pub(super) factory: Arc<Factory>,
  pub(super) observed: Arc<Observed>,
  pub(super) port: u16,
  pub(super) has_stream_handler: Arc<AtomicBool>,
  event_hook: EventHook,
  sender_ports: Mutex<HashMap<u32, u16>>,
  _null_on_drop: NullOnDrop,
}

impl Rig {
  pub(super) fn new(mode: ReceiveMode) -> Self {
    Self::with_video_codecs(mode, &["vp8"])
  }

  pub(super) fn with_video_codecs(mode: ReceiveMode, video_codecs: &[&str]) -> Self {
    init();
    let (event_tx, events) = mpsc::unbounded_channel();
    let factory = Arc::new(Factory {
      calls: Mutex::default(),
      make: Mutex::new(Box::new(|_| None)),
      records: Mutex::default(),
      elements: Mutex::default(),
    });
    factory.set(factory.recording());
    let observed = Arc::new(Observed::default());
    let event_hook = EventHook::default();
    let has_stream_handler = Arc::new(AtomicBool::new(true));

    let hooks = MediaHooks {
      stream_event: {
        let event_hook = event_hook.clone();
        Arc::new(move |event| {
          if let Some(hook) = event_hook.lock().unwrap().as_ref() {
            hook(&event);
          }
          let _ = event_tx.send(event);
        })
      },
      sink_factory: {
        let factory = factory.clone();
        Arc::new(move |info| {
          factory.calls.lock().unwrap().push(info.clone());
          let element = (factory.make.lock().unwrap())(&info);
          if let Some(element) = &element {
            factory.elements.lock().unwrap().insert(info.stream_id, element.clone());
          }
          element
        })
      },
      has_stream_handler: {
        let has_stream_handler = has_stream_handler.clone();
        Arc::new(move || has_stream_handler.load(Ordering::SeqCst))
      },
      participant_ready: {
        let observed = observed.clone();
        Arc::new(move |id| {
          observed.participants.lock().unwrap().push(id.to_owned());
          let gate = observed.gate.lock().unwrap().clone();
          Box::pin(async move {
            if let Some(mut gate) = gate {
              let _ = gate.wait_for(|open| *open).await;
            }
          })
        })
      },
      sync_sample: SyncSampleSink::default(),
      session_failed: {
        let observed = observed.clone();
        Arc::new(move |session, failure| observed.failures.lock().unwrap().push((session, failure)))
      },
      connection_state: {
        let observed = observed.clone();
        Arc::new(move |session, state| observed.connection_states.lock().unwrap().push((session, state)))
      },
      fatal: {
        let observed = observed.clone();
        Arc::new(move |message| observed.fatal.lock().unwrap().push(message))
      },
    };
    let media = ConferenceMedia::new(
      MediaConfig {
        receive_mode: mode,
        buffer_size: 60,
        recv_video_scale_width: 640,
        recv_video_scale_height: 360,
        stream_drain_timeout: Duration::from_secs(3),
        glib_main_context: None,
        tls_insecure: false,
        video_codecs: video_codecs.iter().map(|codec| (*codec).to_owned()).collect(),
        #[cfg(feature = "log-rtp")]
        log_rtp: false,
        #[cfg(feature = "log-rtp")]
        log_rtcp: false,
      },
      hooks,
    )
    .expect("failed to create the conference media");
    let port = free_udp_port();
    Self {
      _null_on_drop: NullOnDrop(media.pipeline()),
      media,
      events,
      factory,
      observed,
      port,
      has_stream_handler,
      event_hook,
      sender_ports: Mutex::default(),
    }
  }

  /// A `udpsink` to the rig. An SSRC always comes from the same source port: rtpsession takes a packet from
  /// another address for a collision and drops it.
  fn udp_sink(&self, ssrc: u32) -> String {
    let source_port = *self
      .sender_ports
      .lock()
      .unwrap()
      .entry(ssrc)
      .or_insert_with(free_udp_port);
    format!(
      "udpsink host=127.0.0.1 port={} bind-address=127.0.0.1 bind-port={source_port} sync=false",
      self.port
    )
  }

  /// Runs `hook` where the application's `stream_event` hook runs, before the event is queued for the test.
  pub(super) fn set_event_hook(&self, hook: impl Fn(&StreamEvent) + Send + 'static) {
    *self.event_hook.lock().unwrap() = Some(Box::new(hook));
  }

  /// A session whose transport is UDP on loopback and whose offer announces `sources`.
  pub(super) async fn start(&self, sources: Vec<RemoteSourceDesc>) -> RtpSession {
    let offer = offer(sources);
    let session = self.media.next_generation();
    self
      .media
      .start_rtp_session(session, &offer, fake_transport(self.port))
      .await
      .expect("failed to start the session")
  }

  pub(super) async fn next_event(&mut self) -> StreamEvent {
    tokio::time::timeout(Duration::from_secs(15), self.events.recv())
      .await
      .expect("no stream event arrived")
      .expect("the event channel closed")
  }

  pub(super) async fn expect(&mut self, kind: StreamEventKind) -> StreamEvent {
    let event = self.next_event().await;
    assert_eq!(event.kind, kind, "unexpected event {event:?}");
    event
  }

  pub(super) async fn expect_no_event(&mut self, quiet: Duration) {
    if let Ok(event) = tokio::time::timeout(quiet, self.events.recv()).await {
      panic!("unexpected event {event:?}");
    }
  }

  /// Playing and no state change under way. (The result of `state()` is not looked at: a bin remembers that a
  /// child posted an error, e.g. a consumer that was torn down, until its next state change.)
  pub(super) fn assert_pipeline_running(&self) {
    let pipeline = self.media.pipeline();
    assert_eq!(pipeline.current_state(), gstreamer::State::Playing);
    assert_eq!(pipeline.pending_state(), gstreamer::State::VoidPending);
    assert!(
      self.observed.fatal.lock().unwrap().is_empty(),
      "{:?}",
      self.observed.fatal
    );
  }
}

/// The source pads, in the pipeline and its bins, that last returned something other than OK to a push. A
/// barrier's own output is left out: what it is for is to take the failures of what follows.
pub(super) fn pads_with_failed_flow(pipeline: &gstreamer::Pipeline) -> Vec<String> {
  let mut failed = Vec::new();
  let mut elements = pipeline.iterate_recurse();
  while let Ok(Some(element)) = elements.next() {
    if element
      .factory()
      .is_some_and(|factory| factory.name() == "gstmeetbarrier")
    {
      continue;
    }
    for pad in element.src_pads() {
      if pad.is::<gstreamer::GhostPad>() && pad.name().starts_with("stream_") {
        continue;
      }
      if let Err(flow) = pad.last_flow_result() {
        failed.push(format!("{}:{} {:?}", element.name(), pad.name(), flow));
      }
    }
  }
  failed
}

/// The fakesinks that are neither a test consumer's sink nor the test transport's.
pub(super) fn parking_fakesinks(pipeline: &gstreamer::Pipeline) -> Vec<String> {
  let mut found = Vec::new();
  let mut elements = pipeline.iterate_recurse();
  while let Ok(Some(element)) = elements.next() {
    let name = element.name().to_string();
    if element.factory().is_some_and(|factory| factory.name() == "fakesink")
      && !name.starts_with("consumer_sink")
      && !name.starts_with("transport_")
    {
      found.push(name);
    }
  }
  found
}

pub(super) fn free_udp_port() -> u16 {
  UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// The transport side of a session without ICE and DTLS: RTP arrives on a UDP port, and what we send goes
/// nowhere.
pub(super) fn fake_transport(port: u16) -> TransportBins {
  let make = |factory: &str| gstreamer::ElementFactory::make(factory).build().unwrap();
  let rtp_in = gstreamer::ElementFactory::make("udpsrc")
    .property("address", "127.0.0.1")
    .property("port", port as i32)
    .property("caps", gstreamer::Caps::builder("application/x-rtp").build())
    .build()
    .unwrap();
  // Nothing ever arrives as RTCP.
  let rtcp_in = make("queue");
  let sink = |name: &str| {
    gstreamer::ElementFactory::make("fakesink")
      .name(name)
      .property("async", false)
      .property("sync", false)
      .build()
      .unwrap()
  };
  let (rtp_out, rtcp_out) = (sink("transport_rtp_out"), sink("transport_rtcp_out"));

  let ghost = |bin: &gstreamer::Bin, name: &str, element: &gstreamer::Element, pad: &str| {
    bin
      .add_pad(
        &gstreamer::GhostPad::builder_with_target(&element.static_pad(pad).unwrap())
          .unwrap()
          .name(name)
          .build(),
      )
      .unwrap();
  };
  let (receive, send) = (gstreamer::Bin::new(), gstreamer::Bin::new());
  receive.add_many([&rtp_in, &rtcp_in]).unwrap();
  send.add_many([&rtp_out, &rtcp_out]).unwrap();
  ghost(&receive, "rtp_recv_src", &rtp_in, "src");
  ghost(&receive, "rtcp_recv_src", &rtcp_in, "src");
  ghost(&send, "rtp_send_sink", &rtp_out, "sink");
  ghost(&send, "rtcp_send_sink", &rtcp_out, "sink");
  TransportBins { receive, send }
}

/// A pipeline that sends RTP to the rig's UDP port.
pub(super) struct Sender {
  pipeline: gstreamer::Pipeline,
}

impl Sender {
  fn launch(description: &str) -> Self {
    let pipeline = gstreamer::parse::launch(description)
      .expect("invalid sender pipeline")
      .downcast::<gstreamer::Pipeline>()
      .unwrap();
    pipeline.set_state(gstreamer::State::Playing).unwrap();
    Self { pipeline }
  }

  pub(super) fn opus(rig: &Rig, ssrc: u32, pt: u32) -> Self {
    Self::launch(&format!(
      "audiotestsrc is-live=true ! audioconvert ! audioresample ! opusenc ! \
       rtpopuspay name=pay pt={pt} ssrc={ssrc} ! {}",
      rig.udp_sink(ssrc)
    ))
  }

  pub(super) fn vp8(rig: &Rig, ssrc: u32, pt: u32) -> Self {
    Self::launch(&format!(
      "videotestsrc is-live=true ! video/x-raw,width=160,height=120,framerate=15/1 ! \
       vp8enc deadline=1 cpu-used=8 keyframe-max-dist=10 ! \
       rtpvp8pay name=pay pt={pt} ssrc={ssrc} ! {}",
      rig.udp_sink(ssrc)
    ))
  }

  /// VP9 continuing the RTP stream of an earlier sender of the same SSRC, as a client that switches codec does.
  pub(super) fn vp9_after(rig: &Rig, earlier: &Sender, ssrc: u32, pt: u32) -> Self {
    Self::launch(&format!(
      "videotestsrc is-live=true ! video/x-raw,width=160,height=120,framerate=15/1 ! \
       vp9enc deadline=1 cpu-used=8 keyframe-max-dist=10 ! \
       rtpvp9pay name=pay pt={pt} ssrc={ssrc} seqnum-offset={} timestamp-offset={} ! {}",
      earlier.last_seqnum() + 1,
      earlier.last_timestamp().wrapping_add(6000),
      rig.udp_sink(ssrc)
    ))
  }

  fn last_seqnum(&self) -> u32 {
    self.pipeline.by_name("pay").unwrap().property::<u32>("seqnum")
  }

  fn last_timestamp(&self) -> u32 {
    self.pipeline.by_name("pay").unwrap().property::<u32>("timestamp")
  }

  pub(super) fn set_pt(&self, pt: u32) {
    self.pipeline.by_name("pay").unwrap().set_property("pt", pt);
  }

  pub(super) fn stop(self) {
    let _ = self.pipeline.set_state(gstreamer::State::Null);
  }
}

impl Drop for Sender {
  fn drop(&mut self) {
    let _ = self.pipeline.set_state(gstreamer::State::Null);
  }
}

pub(super) fn offer(sources: Vec<RemoteSourceDesc>) -> SessionOffer {
  let codec = |name, pt, rtx_pt| Codec {
    name,
    pt,
    rtx_pt,
    rtcp_fbs: vec![],
  };
  SessionOffer {
    sid: "test".to_owned(),
    bridge_session_id: None,
    codecs: vec![
      codec(CodecName::Opus, 111, None),
      codec(CodecName::Opus, 112, None),
      codec(CodecName::Vp8, 96, Some(97)),
      codec(CodecName::Vp9, 98, Some(99)),
    ],
    hdrexts: Default::default(),
    sources,
    ice: RemoteIce {
      ufrag: "ufrag".to_owned(),
      pwd: "pwd".to_owned(),
      candidates: vec![],
      fingerprint: Fingerprint {
        algo: HashAlgo::Sha256,
        value: vec![0; 32],
      },
      setup: xmpp_parsers::jingle_dtls_srtp::Setup::Actpass,
    },
  }
}

pub(super) fn source(ssrc: u32, participant: &str, media_type: MediaType) -> RemoteSourceDesc {
  RemoteSourceDesc {
    source: RemoteSource {
      participant_id: Some(participant.to_owned()),
      ssrc,
      media_type,
      source_name: Some(format!(
        "{participant}-{}0",
        if media_type.is_audio() { 'a' } else { 'v' }
      )),
    },
    rtx_ssrc: None,
  }
}
