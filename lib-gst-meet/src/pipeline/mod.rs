//! The media layer: one GStreamer pipeline per conference ([`ConferenceMedia`]) that outlives the Jingle
//! sessions, and one bin per session ([`MediaSession`]) with its transport, rtpbin and per-stream barriers.
//!
//! ```text
//! pipeline
//!  ├─ send path (payloaders → rtpfunnel → send barrier), for the whole conference
//!  ├─ session_<n> bin: transport bin, rtpbin, barriers, ghost pads
//!  ├─ chain_<id> bins (depayloader …) and per-stream consumers, linked to the session's ghost pads
//!  └─ whatever the application adds
//! ```

use std::{
  fmt,
  sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, Weak,
  },
  time::Duration,
};

use anyhow::{Context, Result};
use gstreamer::prelude::*;
use jitsi_xmpp_parsers::jingle_ice_udp::Transport as IceUdpTransport;
use nice_gst_meet as nice;
use rand::RngExt as _;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, warn};
use xmpp_parsers::jingle_dtls_srtp::Setup;

use crate::{conference::ReceiveMode, xmpp::extdisco::Service};

mod bus;
mod chain;
mod codec;
mod consumer;
mod dtls;
mod elements;
mod hdrext;
mod ice;
mod main_loop;
mod offer;
mod owner;
mod rtp;
mod rtp_session;
mod send;
mod streams;
mod transport;
mod turn_tls;
mod types;
mod vp9_descriptor;

#[cfg(test)]
mod tests;

pub(crate) use codec::{parse_rtp_description, Codec, CodecName, RTP_HDREXT_SSRC_AUDIO_LEVEL, RTP_HDREXT_TRANSPORT_CC};
pub(crate) use dtls::pick_fingerprint;
pub(crate) use ice::LocalIceCandidate;
#[cfg(test)]
pub(crate) use offer::HashAlgo;
pub(crate) use offer::{participant_id_for_owner, Fingerprint, HdrextIds, RemoteIce, RemoteSourceDesc, SessionOffer};
pub(crate) use rtp_session::RtpSession;
pub(crate) use types::SyncSampleSink;
pub use types::{ConnectionState, StreamEvent, StreamEventKind, StreamId, StreamInfo, SyncSample};

use self::{
  bus::{BusHooks, SessionRegistry},
  dtls::DtlsIdentity,
  ice::IceTransport,
  main_loop::MainContext,
  rtp_session::RtpSessionParams,
  send::SendPath,
  streams::{StreamHooks, StreamsHandle},
  transport::{EncoderRelease, TransportBins, TransportParams},
};

/// Why a session has to be replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionFailure {
  /// ICE failed or lost consent.
  Ice,
  /// The DTLS handshake or the SRTP layer failed.
  Dtls(String),
  /// The peer's certificate does not match the fingerprint it signaled.
  Security(String),
  /// An element of the transport failed.
  Transport(String),
}

/// What the media layer needs from the conference. None of these may block: they are called from streaming
/// threads, from GLib's main context and from the stream controller.
pub(crate) struct MediaHooks {
  pub(crate) stream_event: Arc<dyn Fn(StreamEvent) + Send + Sync>,
  pub(crate) sink_factory: Arc<dyn Fn(StreamInfo) -> Option<gstreamer::Element> + Send + Sync>,
  pub(crate) has_stream_handler: Arc<dyn Fn() -> bool + Send + Sync>,
  /// Called when a source of a participant the session has not seen yet is announced. The future completes when the
  /// application is done with the participant (it may have to create the bin that the participant's streams are
  /// linked to); the participant's streams are not activated before, for a couple of seconds at most.
  pub(crate) participant_ready: streams::ParticipantReady,
  pub(crate) sync_sample: SyncSampleSink,
  /// The session generation, and why that session has to go.
  pub(crate) session_failed: Arc<dyn Fn(u64, SessionFailure) + Send + Sync>,
  pub(crate) connection_state: Arc<dyn Fn(u64, ConnectionState) + Send + Sync>,
  /// An error that belongs to no session or stream; the pipeline has been set to `Null`.
  pub(crate) fatal: Arc<dyn Fn(String) + Send + Sync>,
}

pub(crate) struct MediaConfig {
  pub(crate) receive_mode: ReceiveMode,
  pub(crate) buffer_size: u32,
  pub(crate) recv_video_scale_width: u16,
  pub(crate) recv_video_scale_height: u16,
  pub(crate) stream_drain_timeout: Duration,
  /// `None`: libnice runs on a context of ours, iterated by a thread of ours.
  pub(crate) glib_main_context: Option<glib::MainContext>,
  pub(crate) tls_insecure: bool,
  pub(crate) video_codecs: Vec<String>,
  #[cfg(feature = "log-rtp")]
  pub(crate) log_rtp: bool,
  #[cfg(feature = "log-rtp")]
  pub(crate) log_rtcp: bool,
}

/// The SSRCs we send with, fixed for the conference so that they survive a change of session.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalSsrcs {
  pub(crate) audio: u32,
  pub(crate) video: u32,
  pub(crate) video_rtx: u32,
}

#[derive(Clone)]
pub(crate) struct LocalTransport {
  pub(crate) ufrag: String,
  pub(crate) pwd: String,
  /// SHA-256 of our certificate.
  pub(crate) fingerprint: Vec<u8>,
  pub(crate) setup: Setup,
}

#[derive(Clone)]
pub(crate) struct ConferenceMedia {
  inner: Arc<Inner>,
}

struct Inner {
  config: MediaConfig,
  hooks: MediaHooks,
  pipeline: gstreamer::Pipeline,
  main_context: Mutex<MainContext>,
  local_ssrcs: LocalSsrcs,
  stream_ids: Arc<AtomicU64>,
  generations: AtomicU64,
  send: Mutex<Option<Arc<SendPath>>>,
  sessions: SessionRegistry,
  /// Of the sessions that may still start their DTLS encoder; [`ConferenceMedia::shutdown`] closes them.
  encoder_releases: Mutex<Vec<Weak<EncoderRelease>>>,
  bus_task: Mutex<Option<JoinHandle<()>>>,
  shut_down: AtomicBool,
}

impl fmt::Debug for ConferenceMedia {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ConferenceMedia").finish()
  }
}

impl ConferenceMedia {
  /// Must be called from within a tokio runtime, which runs the bus monitor.
  pub(crate) fn new(config: MediaConfig, hooks: MediaHooks) -> Result<Self> {
    elements::register()?;
    let main_context = MainContext::new(config.glib_main_context.clone())?;
    let pipeline = gstreamer::Pipeline::new();
    let sessions = SessionRegistry::default();
    let bus_task = bus::spawn(
      &pipeline,
      sessions.clone(),
      BusHooks {
        session_failed: hooks.session_failed.clone(),
        fatal: hooks.fatal.clone(),
      },
    )
    .context("the pipeline has no bus")?;

    let mut rng = rand::rng();
    let local_ssrcs = LocalSsrcs {
      audio: rng.random(),
      video: rng.random(),
      video_rtx: rng.random(),
    };
    debug!("local SSRCs: {:?}", local_ssrcs);

    Ok(Self {
      inner: Arc::new(Inner {
        config,
        hooks,
        pipeline,
        main_context: Mutex::new(main_context),
        local_ssrcs,
        stream_ids: Arc::new(AtomicU64::new(1)),
        generations: AtomicU64::new(0),
        send: Mutex::new(None),
        sessions,
        encoder_releases: Mutex::new(Vec::new()),
        bus_task: Mutex::new(Some(bus_task)),
        shut_down: AtomicBool::new(false),
      }),
    })
  }

  pub(crate) fn pipeline(&self) -> gstreamer::Pipeline {
    self.inner.pipeline.clone()
  }

  pub(crate) fn local_ssrcs(&self) -> LocalSsrcs {
    self.inner.local_ssrcs
  }

  /// `video_codecs` of the configuration that this machine can receive, in lower case.
  pub(crate) fn receivable_video_codecs(&self) -> Vec<String> {
    self
      .inner
      .config
      .video_codecs
      .iter()
      .filter(|name| {
        CodecName::parse_video(name).is_some_and(|codec| codec.receive_supported(self.inner.config.receive_mode))
      })
      .map(|name| name.to_lowercase())
      .collect()
  }

  /// `None` until the first session has been started.
  pub(crate) fn audio_sink_element(&self) -> Option<gstreamer::Element> {
    self.send_path().map(|send| send.audio_sink_element())
  }

  /// `None` until the first session has been started.
  pub(crate) fn video_sink_element(&self) -> Option<gstreamer::Element> {
    self.send_path().map(|send| send.video_sink_element())
  }

  fn send_path(&self) -> Option<Arc<SendPath>> {
    self.inner.send.lock().unwrap_or_else(|e| e.into_inner()).clone()
  }

  /// Sets up ICE and the transport for `offer` and puts the session into the pipeline, which is started if it
  /// is not running yet. Candidate gathering starts with [`MediaSession::gather_candidates`], the DTLS handshake
  /// when ICE connects.
  pub(crate) async fn start_session(&self, offer: &SessionOffer, ice_servers: &[Service]) -> Result<MediaSession> {
    let session = self.next_generation();
    debug!(
      "starting media session {} for Jingle session {} (bridge session {:?})",
      session, offer.sid, offer.bridge_session_id
    );
    let hooks = &self.inner.hooks;
    let identity = DtlsIdentity::generate()?;
    let main_context = self
      .inner
      .main_context
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .context()
      .clone();
    let mut ice = IceTransport::new(&main_context, ice_servers, &offer.ice, self.inner.config.tls_insecure).await?;
    let (ufrag, pwd) = ice.local_credentials()?;

    let setup = local_setup(&offer.ice.setup);
    let transport = transport::build_transport_bin(TransportParams {
      agent: ice.agent(),
      stream_id: ice.stream_id(),
      component_id: ice.component_id(),
      pem: &identity.pem,
      is_client: setup == Setup::Active,
      peer_fingerprint: offer.ice.fingerprint.clone(),
      on_fingerprint_mismatch: {
        let session_failed = hooks.session_failed.clone();
        Box::new(move |reason| session_failed(session, SessionFailure::Security(reason)))
      },
      #[cfg(feature = "log-rtp")]
      log_rtp: self.inner.config.log_rtp,
      #[cfg(feature = "log-rtp")]
      log_rtcp: self.inner.config.log_rtcp,
    })?;

    let rtp = self.start_rtp_session(session, offer, transport.bins).await?;

    let encoder_release = transport
      .locked_encoder
      .as_ref()
      .map(|encoder| Arc::new(EncoderRelease::new(encoder)));
    if let Some(release) = &encoder_release {
      let mut releases = self.inner.encoder_releases.lock().unwrap_or_else(|e| e.into_inner());
      releases.retain(|release| release.strong_count() > 0);
      releases.push(Arc::downgrade(release));
    }
    let connection_state = hooks.connection_state.clone();
    let session_failed = hooks.session_failed.clone();
    ice.on_component_state({
      let encoder_release = encoder_release.clone();
      move |state| match state {
        nice::ComponentState::Connecting => connection_state(session, ConnectionState::Checking),
        nice::ComponentState::Connected | nice::ComponentState::Ready => {
          connection_state(session, ConnectionState::Connected);
          if let Some(release) = &encoder_release {
            release.release();
          }
        },
        nice::ComponentState::Failed => {
          connection_state(session, ConnectionState::Failed);
          session_failed(session, SessionFailure::Ice);
        },
        _ => {},
      }
    });

    Ok(MediaSession {
      media: self.clone(),
      rtp,
      ice,
      encoder_release,
      local: LocalTransport {
        ufrag,
        pwd,
        fingerprint: identity.fingerprint,
        setup,
      },
    })
  }

  fn next_generation(&self) -> u64 {
    self.inner.generations.fetch_add(1, Ordering::SeqCst) + 1
  }

  /// Everything of a session but ICE and DTLS.
  async fn start_rtp_session(
    &self,
    session: u64,
    offer: &SessionOffer,
    transport: TransportBins,
  ) -> Result<RtpSession> {
    let inner = &self.inner;
    let send = {
      let mut send = inner.send.lock().unwrap_or_else(|e| e.into_inner());
      match send.as_ref() {
        Some(send) => {
          send.update(offer, &inner.config.video_codecs);
          send.clone()
        },
        None => {
          let created = Arc::new(SendPath::new(
            &inner.pipeline,
            offer,
            &inner.config.video_codecs,
            inner.local_ssrcs,
          )?);
          *send = Some(created.clone());
          created
        },
      }
    };

    let hooks = &inner.hooks;
    let rtp = RtpSession::new(RtpSessionParams {
      session,
      pipeline: &inner.pipeline,
      offer,
      config: &inner.config,
      local_ssrcs: inner.local_ssrcs,
      stream_ids: inner.stream_ids.clone(),
      hooks: StreamHooks {
        stream_event: hooks.stream_event.clone(),
        sink_factory: hooks.sink_factory.clone(),
        has_stream_handler: hooks.has_stream_handler.clone(),
        participant_ready: hooks.participant_ready.clone(),
        sync_sample: hooks.sync_sample.clone(),
      },
      transport,
    })?;

    inner.pipeline.add(rtp.bin())?;
    let started = (|| -> Result<()> {
      send.attach(&rtp.send_sink_pad()?)?;
      if inner.pipeline.current_state() == gstreamer::State::Playing {
        rtp.bin().sync_state_with_parent()?;
      }
      else {
        inner
          .pipeline
          .set_state(gstreamer::State::Playing)
          .context("failed to start the pipeline")?;
      }
      Ok(())
    })();
    if let Err(e) = started {
      if let Ok(sink) = rtp.send_sink_pad() {
        send.detach(&sink);
      }
      consumer::remove_element(&inner.pipeline, rtp.bin());
      rtp.retire_streams(Duration::ZERO).await;
      return Err(e);
    }

    inner
      .sessions
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .insert(session, rtp.streams());
    Ok(rtp)
  }

  /// Retires the streams of the session (draining their consumers for at most `drain_timeout`), disconnects the
  /// send path and removes the session's bin from the pipeline, which keeps running.
  async fn stop_rtp_session(&self, rtp: RtpSession, drain_timeout: Duration) {
    rtp.retire_streams(drain_timeout).await;
    self
      .inner
      .sessions
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .remove(&rtp.generation());

    if let (Some(send), Ok(sink)) = (self.send_path(), rtp.send_sink_pad()) {
      send.detach(&sink);
    }
    let pipeline = self.inner.pipeline.clone();
    let bin = rtp.bin().clone();
    if let Err(e) = tokio::task::spawn_blocking(move || consumer::remove_element(&pipeline, &bin)).await {
      warn!("failed to remove session {}: {:?}", rtp.generation(), e);
    }
  }

  /// Tries again to activate the streams that found no consumer, as one may exist now: a sink factory or a stream
  /// handler was registered, or the application added a bin.
  pub(crate) fn reevaluate_streams(&self) {
    for streams in self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner()).values() {
      streams.reevaluate();
    }
  }

  /// Stops the sessions that are left, the pipeline, the bus monitor and the GLib thread, if we started one. The
  /// pipeline reaches `Null` before this returns: its state change is synchronous, and the bus never delivers the
  /// message that announces it, so nothing waits for one.
  pub(crate) async fn shutdown(&self) {
    if self.inner.shut_down.swap(true, Ordering::SeqCst) {
      return;
    }
    let remaining: Vec<StreamsHandle> = self
      .inner
      .sessions
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .drain()
      .map(|(_, handle)| handle)
      .collect();
    for handle in remaining {
      handle.shutdown(self.inner.config.stream_drain_timeout).await;
    }

    if let Some(bus_task) = self.inner.bus_task.lock().unwrap_or_else(|e| e.into_inner()).take() {
      bus_task.abort();
    }
    // Sessions that were not shut down may still see ICE connect while the pipeline goes to `Null`.
    for release in self
      .inner
      .encoder_releases
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .drain(..)
      .filter_map(|release| release.upgrade())
    {
      release.close();
    }
    let pipeline = self.inner.pipeline.clone();
    match tokio::task::spawn_blocking(move || pipeline.set_state(gstreamer::State::Null)).await {
      Ok(Ok(_)) => {},
      Ok(Err(e)) => warn!("failed to set the pipeline to Null: {:?}", e),
      Err(e) => warn!("failed to set the pipeline to Null: {:?}", e),
    }
    let inner = self.inner.clone();
    let _ = tokio::task::spawn_blocking(move || {
      inner.main_context.lock().unwrap_or_else(|e| e.into_inner()).stop();
    })
    .await;
  }
}

/// We take the role the peer does not: against `active` we are the DTLS server, otherwise the client.
fn local_setup(remote: &Setup) -> Setup {
  match remote {
    Setup::Active => Setup::Passive,
    _ => Setup::Active,
  }
}

/// A Jingle session's media: ICE, DTLS, rtpbin and the streams. Dropped without [`shutdown`](Self::shutdown) it
/// leaves its bin in the pipeline.
pub(crate) struct MediaSession {
  media: ConferenceMedia,
  rtp: RtpSession,
  ice: IceTransport,
  encoder_release: Option<Arc<EncoderRelease>>,
  local: LocalTransport,
}

impl MediaSession {
  pub(crate) fn generation(&self) -> u64 {
    self.rtp.generation()
  }

  pub(crate) fn local_transport(&self) -> LocalTransport {
    self.local.clone()
  }

  /// Starts gathering and waits for the candidates, at most `timeout`. The receiver, if any, yields those that
  /// come later.
  pub(crate) async fn gather_candidates(
    &mut self,
    timeout: Duration,
  ) -> (
    Vec<LocalIceCandidate>,
    Option<mpsc::UnboundedReceiver<LocalIceCandidate>>,
  ) {
    self.ice.gather(timeout).await
  }

  pub(crate) fn add_remote_candidates(&self, transport: &IceUdpTransport) -> Result<()> {
    self.ice.add_remote_candidates(transport)
  }

  pub(crate) async fn sources_added(&self, sources: Vec<RemoteSourceDesc>) {
    self.rtp.sources_added(sources).await;
  }

  pub(crate) async fn sources_removed(&self, ssrcs: Vec<u32>) {
    self.rtp.sources_removed(ssrcs).await;
  }

  pub(crate) async fn participant_left(&self, participant_id: &str) {
    self.rtp.participant_left(participant_id).await;
  }

  /// Sends EOS through every stream with a consumer of its own and waits for it to arrive, at most `timeout`.
  /// Fails if some stream could not be drained. No stream is activated afterwards.
  pub(crate) async fn drain(&self, timeout: Duration) -> Result<()> {
    self.rtp.drain(timeout).await
  }

  /// Takes the session apart: the streams are retired, the bin leaves the pipeline, and the ICE agent is closed,
  /// which gives the TURN allocations back (for [`ice::CLOSE_TIMEOUT`] at most).
  pub(crate) async fn shutdown(self, drain_timeout: Duration) {
    let Self {
      media,
      rtp,
      ice,
      encoder_release,
      ..
    } = self;
    // ICE may connect at any moment until the agent is closed: no more starting of elements that are going away.
    if let Some(release) = &encoder_release {
      release.close();
    }
    media.stop_rtp_session(rtp, drain_timeout).await;
    ice.close(self::ice::CLOSE_TIMEOUT).await;
  }

  pub(crate) fn rtp_source_stats(&self) -> Vec<gstreamer::Structure> {
    self.rtp.rtp_source_stats()
  }
}
