use std::{
  net::SocketAddr,
  sync::{Arc, Mutex},
  time::Duration,
};

use anyhow::{bail, Result};
use async_trait::async_trait;
use jitsi_xmpp_parsers::jingle_ice_udp::Transport as IceUdpTransport;
use tokio::sync::mpsc;
use xmpp_parsers::jingle_dtls_srtp::Setup;

use crate::{
  backend::{Media, Session},
  pipeline::{LocalIceCandidate, LocalSsrcs, LocalTransport, MediaHooks, RemoteSourceDesc, SessionOffer},
  xmpp::extdisco::Service,
};

pub(super) struct FakeMedia {
  pub(super) hooks: MediaHooks,
  pipeline: gstreamer::Pipeline,
  state: Arc<Mutex<FakeState>>,
}

#[derive(Default)]
struct FakeState {
  generations: u64,
  log: Vec<String>,
  fail_next_start: Option<String>,
  late_candidate: Option<mpsc::UnboundedSender<LocalIceCandidate>>,
}

impl FakeMedia {
  pub(super) fn new(hooks: MediaHooks) -> Arc<Self> {
    gstreamer::init().expect("failed to initialise GStreamer");
    Arc::new(Self {
      hooks,
      pipeline: gstreamer::Pipeline::new(),
      state: Arc::default(),
    })
  }

  fn record(&self, entry: String) {
    self.state.lock().unwrap().log.push(entry);
  }

  pub(super) fn log(&self) -> Vec<String> {
    self.state.lock().unwrap().log.clone()
  }

  pub(super) fn count(&self, entry: &str) -> usize {
    self.log().iter().filter(|logged| logged.as_str() == entry).count()
  }

  pub(super) async fn wait_for_log(&self, entry: &str) {
    let found = async {
      while !self.log().iter().any(|logged| logged == entry) {
        tokio::time::sleep(Duration::from_millis(5)).await;
      }
    };
    if tokio::time::timeout(Duration::from_secs(5), found).await.is_err() {
      panic!("timed out waiting for {entry:?} in {:#?}", self.log());
    }
  }

  pub(super) fn fail_next_start(&self, message: &str) {
    self.state.lock().unwrap().fail_next_start = Some(message.to_owned());
  }

  /// Makes a candidate that is found after the accept.
  pub(super) fn late_candidate(&self) {
    let sender = self.state.lock().unwrap().late_candidate.clone().expect("a session gathers candidates");
    sender.send(candidate(10001)).unwrap();
  }
}

fn candidate(port: u16) -> LocalIceCandidate {
  LocalIceCandidate {
    component_id: 1,
    foundation: "1".to_owned(),
    addr: SocketAddr::from(([192, 168, 1, 5], port)),
    priority: 2130706431,
    candidate_type: nice_gst_meet::CandidateType::Host,
    rel_addr: None,
  }
}

fn ssrcs(sources: &[RemoteSourceDesc]) -> Vec<u32> {
  sources.iter().map(|desc| desc.source.ssrc).collect()
}

#[async_trait]
impl Media for FakeMedia {
  fn pipeline(&self) -> gstreamer::Pipeline {
    self.pipeline.clone()
  }

  fn local_ssrcs(&self) -> LocalSsrcs {
    LocalSsrcs {
      audio: 100,
      video: 200,
      video_rtx: 201,
    }
  }

  fn receivable_video_codecs(&self) -> Vec<String> {
    vec!["vp8".to_owned()]
  }

  fn audio_sink_element(&self) -> Option<gstreamer::Element> {
    None
  }

  fn video_sink_element(&self) -> Option<gstreamer::Element> {
    None
  }

  fn reevaluate_streams(&self) {
    self.record("reevaluate".to_owned());
  }

  async fn start_session(&self, offer: &SessionOffer, ice_servers: &[Service]) -> Result<Box<dyn Session>> {
    let generation = {
      let mut state = self.state.lock().unwrap();
      if let Some(message) = state.fail_next_start.take() {
        bail!("{message}");
      }
      state.generations += 1;
      state.generations
    };
    let (late_tx, late_rx) = mpsc::unbounded_channel();
    self.state.lock().unwrap().late_candidate = Some(late_tx);
    self.record(format!(
      "session {generation}: start sid={} sources={:?} ice_servers={} fingerprint={:?} candidates={}",
      offer.sid,
      ssrcs(&offer.sources),
      ice_servers.len(),
      offer.ice.fingerprint.algo,
      offer.ice.candidates.len(),
    ));
    Ok(Box::new(FakeSession {
      state: self.state.clone(),
      generation,
      late: Some(late_rx),
    }))
  }

  async fn shutdown(&self) {
    self.record("media: shutdown".to_owned());
  }
}

struct FakeSession {
  state: Arc<Mutex<FakeState>>,
  generation: u64,
  late: Option<mpsc::UnboundedReceiver<LocalIceCandidate>>,
}

impl FakeSession {
  fn record(&self, entry: String) {
    self
      .state
      .lock()
      .unwrap()
      .log
      .push(format!("session {}: {entry}", self.generation));
  }
}

#[async_trait]
impl Session for FakeSession {
  fn generation(&self) -> u64 {
    self.generation
  }

  fn local_transport(&self) -> LocalTransport {
    LocalTransport {
      ufrag: "localufrag".to_owned(),
      pwd: "localpassword0123456789".to_owned(),
      fingerprint: vec![0xCD; 32],
      setup: Setup::Active,
    }
  }

  async fn gather_candidates(
    &mut self,
    _timeout: Duration,
  ) -> (Vec<LocalIceCandidate>, Option<mpsc::UnboundedReceiver<LocalIceCandidate>>) {
    (vec![candidate(10000)], self.late.take())
  }

  fn add_remote_candidates(&self, transport: &IceUdpTransport) -> Result<()> {
    self.record(format!("remote candidates {}", transport.candidates.len()));
    Ok(())
  }

  async fn sources_added(&self, sources: Vec<RemoteSourceDesc>) {
    self.record(format!("sources added {:?}", ssrcs(&sources)));
  }

  async fn sources_removed(&self, ssrcs: Vec<u32>) {
    self.record(format!("sources removed {ssrcs:?}"));
  }

  async fn participant_left(&self, participant_id: &str) {
    self.record(format!("participant left {participant_id}"));
  }

  async fn drain(&self, timeout: Duration) -> Result<()> {
    self.record(format!("drain {timeout:?}"));
    Ok(())
  }

  async fn shutdown(self: Box<Self>, _drain_timeout: Duration) {
    self.record("shutdown".to_owned());
  }

  fn rtp_source_stats(&self) -> Vec<gstreamer::Structure> {
    vec![]
  }
}
