use std::{
  collections::HashMap,
  convert::TryFrom,
  fmt,
  future::Future,
  pin::Pin,
  sync::{atomic::AtomicBool, Arc, Mutex, OnceLock, Weak},
  time::Duration,
};

use anyhow::{Context, Result};
use bon::Builder;
use colibri::{ColibriMessage, Constraints, ReceiverVideoConstraints, VideoType};
use gstreamer::prelude::{ElementExt as _, ElementExtManual as _, GstBinExt as _};
use jid::{BareJid, FullJid, Jid};
use minidom::Element;
use rand::RngExt as _;
use serde::Serialize;
use tokio::{
  sync::{mpsc, oneshot},
  time::sleep,
};
use tracing::{debug, warn};
use uuid::Uuid;
use xmpp_parsers::{
  iq::Iq,
  message::{Id as MessageId, Message, MessageType},
  presence::{self, Presence},
};

use crate::{
  backend::Media,
  dispatcher::EventDispatcher,
  error::{ConferenceLeftReason, JoinError, MediaSessionEvent, SourceEvent},
  focus::{self, Answer, MAX_ERROR_RETRY_DELAY},
  jingle_actor::{self, Command},
  pipeline::{ConferenceMedia, MediaConfig, MediaHooks, StreamEvent, StreamInfo, SyncSample, SyncSampleSink},
  presence::PresenceState,
  receiver::{BridgeConstraints, ReceiverState, VideoReceivePolicy},
  shared::{current, Callbacks, Phase, Shared, State},
  signaling::Signaling,
  source::{MediaType, RemoteSource},
  sources::SourceMap,
  util::{generate_id, Backoff},
  xmpp::{
    self,
    connection::Connection,
    iq::RequestError,
  },
};

const CONFERENCE_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// The focus may have to ask for the room again if it sets it up while we are joining.
const ROOM_CREATION_RETRIES: u32 = 3;

#[derive(Builder, Debug, Clone)]
pub struct JitsiConferenceConfig {
  pub muc: BareJid,
  pub focus: Jid,
  pub nick: String,
  pub region: Option<String>,
  /// The video codecs to use, in order of preference: "av1", "vp9", "vp8" or "h264", in lower case.
  #[builder(default = vec!["vp8".to_owned()])]
  pub video_codecs: Vec<String>,
  #[builder(default)]
  pub extra_muc_features: Vec<String>,
  #[builder(default = 800)]
  pub start_bitrate: u32,
  #[builder(default)]
  pub stereo: bool,
  #[builder(default = 1280)]
  pub recv_video_scale_width: u16,
  #[builder(default = 720)]
  pub recv_video_scale_height: u16,
  #[builder(default = 200)]
  pub buffer_size: u32,
  #[builder(default)]
  pub receive_mode: ReceiveMode,
  /// The context libnice runs on. `None`: the library iterates a context of its own on a thread of its own.
  pub glib_main_context: Option<glib::MainContext>,
  /// How long [`JitsiConference::join`] may take in all, from the request to the focus to the acknowledgement of
  /// the first media session.
  #[builder(default = Duration::from_secs(60))]
  pub join_timeout: Duration,
  /// How long to wait for local ICE candidates before the session is accepted; later ones are trickled.
  #[builder(default = Duration::from_secs(5))]
  pub ice_gathering_timeout: Duration,
  /// How long a stream's consumer gets to see the end of its stream before the stream is torn down.
  #[builder(default = Duration::from_secs(3))]
  pub stream_drain_timeout: Duration,
  /// How long the conference waits for a new media session after one has ended, before it is given up with
  /// [`ConferenceLeftReason::NoMediaSession`].
  #[builder(default = Duration::from_secs(60))]
  pub media_session_timeout: Duration,
  /// The max resolution we are sending, broadcast to other participants in stats so that their clients can display it.
  /// Purely informational: lib-gst-meet does not encode video itself.
  pub send_resolution: Option<i32>,
  #[cfg(feature = "log-rtp")]
  pub log_rtp: bool,
  #[cfg(feature = "log-rtp")]
  pub log_rtcp: bool,
}

/// What the consumer of each remote stream receives.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReceiveMode {
  /// Depayloaded and decoded: `audio/x-raw` / `video/x-raw`.
  #[default]
  Decoded,
  /// Depayloaded and parsed only (H264 as `avc`/`au`, AV1 as `obu-stream`/`tu`). Adds keyframe requests on
  /// packet loss and [`SyncSample`]s. A per-stream consumer gets EOS through the stream before teardown, and
  /// its `Removed` event fires once that has completed.
  Encoded,
}

/// A joined conference. Cloning gives another handle to the same conference.
///
/// Callbacks run one at a time, in the order of the events, on a task of the library that nothing else waits for: a
/// callback may take as long as it needs (a slow one delays the ones after it) and may call any method of the
/// conference. The exceptions are the sink factory and the sync sample callback, which run synchronously on the
/// library's threads and must not call back into the conference.
#[derive(Clone)]
pub struct JitsiConference {
  pub(crate) shared: Arc<Shared>,
}

impl fmt::Debug for JitsiConference {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("JitsiConference")
      .field("jid", &self.shared.jid)
      .field("config", &self.shared.config)
      .finish()
  }
}

#[derive(Debug, Clone)]
pub struct Participant {
  pub jid: Option<FullJid>,
  pub muc_jid: FullJid,
  pub nick: Option<String>,
}

pub type BoxedResultFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

type SharedCell = Arc<OnceLock<Weak<Shared>>>;

impl JitsiConference {
  /// Resolves when the focus has accepted the first media session of the conference.
  ///
  /// When it fails, the error is a [`JoinError`] (`error.downcast_ref::<JoinError>()`) unless it is about
  /// something that precedes the join, such as a connection that has no JID.
  ///
  /// Must be called from within a tokio runtime.
  pub async fn join(connection: Connection, config: JitsiConferenceConfig) -> Result<Self> {
    Self::join_with(connection, config, |config, tls_insecure, hooks| {
      let media = ConferenceMedia::new(
        MediaConfig {
          receive_mode: config.receive_mode,
          buffer_size: config.buffer_size,
          recv_video_scale_width: config.recv_video_scale_width,
          recv_video_scale_height: config.recv_video_scale_height,
          stream_drain_timeout: config.stream_drain_timeout,
          glib_main_context: config.glib_main_context.clone(),
          tls_insecure,
          video_codecs: config.video_codecs.clone(),
          #[cfg(feature = "log-rtp")]
          log_rtp: config.log_rtp,
          #[cfg(feature = "log-rtp")]
          log_rtcp: config.log_rtcp,
        },
        hooks,
      )?;
      Ok(Arc::new(media))
    })
    .await
  }

  pub(crate) async fn join_with(
    connection: Connection,
    mut config: JitsiConferenceConfig,
    make_media: impl FnOnce(&JitsiConferenceConfig, bool, MediaHooks) -> Result<Arc<dyn Media>>,
  ) -> Result<Self> {
    config.video_codecs = config.video_codecs.iter().map(|codec| codec.to_lowercase()).collect();

    let jid = connection.jid().await.context("not connected (no JID)")?;
    let endpoint_id = endpoint_id_for_jid(&jid)?.to_owned();
    let muc_jid = config.muc.clone().with_resource_str(&endpoint_id)?;
    let focus_jid = config.muc.clone().with_resource_str("focus")?;

    let cell = SharedCell::default();
    let sync_sample_sink = SyncSampleSink::default();
    let media = make_media(
      &config,
      connection.tls_insecure,
      media_hooks(&cell, sync_sample_sink.clone()),
    )?;

    let (jingle_tx, jingle_rx) = mpsc::unbounded_channel();
    let presence = PresenceState::new(
      endpoint_id.clone(),
      config.nick.clone(),
      config.region.clone(),
      media.receivable_video_codecs(),
      config.extra_muc_features.clone(),
    );
    let shared = Arc::new(Shared {
      jid,
      endpoint_id,
      muc: config.muc.clone(),
      muc_jid,
      focus_jid,
      xmpp_tx: connection.tx.clone(),
      connection: connection.clone(),
      media,
      sync_sample_sink,
      dispatcher: EventDispatcher::spawn(),
      callbacks: Callbacks::default(),
      state: Mutex::new(State {
        phase: Phase::Joining,
        in_room: false,
        participants: HashMap::new(),
        on_participant: None,
        presence,
        known_sid: None,
        sources: SourceMap::default(),
        receiver: ReceiverState::default(),
        bridge_constraints: BridgeConstraints::default(),
        video_type: VideoType::Camera,
        muc_waiter: None,
        session_waiter: None,
        left: None,
      }),
      jingle: jingle_tx,
      colibri: Mutex::new(None),
      filter_id: OnceLock::new(),
      runtime: tokio::runtime::Handle::current(),
      torn_down: tokio::sync::Mutex::new(false),
      released: AtomicBool::new(false),
      constraints_gate: tokio::sync::Mutex::new(()),
      config,
    });
    let _ = cell.set(Arc::downgrade(&shared));
    jingle_actor::spawn(shared.clone(), jingle_rx);
    let filter_id = connection.add_stanza_filter(Signaling(shared.clone())).await;
    let _ = shared.filter_id.set(filter_id);

    let joined = tokio::time::timeout(shared.config.join_timeout, shared.run_join()).await;
    match joined {
      Ok(Ok(())) => Ok(Self { shared }),
      Ok(Err(error)) => {
        shared.tear_down().await;
        Err(error.into())
      },
      Err(_) => {
        shared.tear_down().await;
        Err(JoinError::Timeout.into())
      },
    }
  }

  /// Leaves the conference: ends the media session, tells the room, stops the pipeline and the tasks of the
  /// library. Streams that have a consumer of their own get their EOS first. The `on_conference_left` callback is
  /// called with [`ConferenceLeftReason::Left`], unless the conference was over already.
  ///
  /// Once the last event has been delivered the callbacks are dropped, and later ones are not kept: a callback that
  /// holds a handle on the conference (as one that calls back into it must) would keep it alive otherwise.
  ///
  /// Does nothing if the conference has been left.
  pub async fn leave(self) -> Result<()> {
    self.shared.tear_down().await;
    Ok(())
  }

  /// Our nickname in the room: the ID that other participants know us by, and that the names of our sources start
  /// with (`<endpoint id>-a0` and `<endpoint id>-v0`).
  pub fn endpoint_id(&self) -> &str {
    &self.shared.endpoint_id
  }

  /// Mutes or unmutes our audio or video in the presence the other participants see. The pipeline is the
  /// application's to control.
  pub async fn set_muted(&self, media_type: MediaType, muted: bool) -> Result<()> {
    let payloads = {
      let mut state = self.shared.lock();
      state.presence.set_muted(media_type, muted);
      state.presence.payloads(false)
    };
    self.shared.send_presence_payloads(payloads).await
  }

  /// The pipeline of the conference, which exists from the start and goes on after the media sessions that come
  /// and go in it. It is `Playing` from the first media session on.
  pub fn pipeline(&self) -> gstreamer::Pipeline {
    self.shared.media.pipeline()
  }

  /// Adds `bin` to the pipeline and brings it to the state of the pipeline. A stream that waited for a bin of
  /// this kind (`participant_<id>`) is given another chance.
  pub async fn add_bin(&self, bin: &gstreamer::Bin) -> Result<()> {
    let pipeline = self.pipeline();
    pipeline.add(bin)?;
    bin.sync_state_with_parent()?;
    self.shared.media.reevaluate_streams();
    Ok(())
  }

  pub async fn set_pipeline_state(&self, state: gstreamer::State) -> Result<()> {
    self.pipeline().call_async(move |pipeline| {
      if let Err(e) = pipeline.set_state(state) {
        tracing::error!("pipeline set_state: {:?}", e);
      }
    });
    Ok(())
  }

  /// Called for every remote stream that is added or removed.
  pub async fn on_remote_stream(&self, f: impl Fn(StreamEvent) -> BoxedResultFuture + Send + Sync + 'static) {
    self.shared.set_callback(&self.shared.callbacks.on_remote_stream, Arc::new(f));
    // A stream that has no other consumer is delivered to the handler.
    self.shared.media.reevaluate_streams();
  }

  /// Called synchronously from streaming threads: must be cheap and must not block.
  pub async fn on_sync_sample(&self, f: impl Fn(SyncSample) + Send + Sync + 'static) {
    self.shared.set_sync_sample_callback(Arc::new(f));
  }

  /// Pushes EOS through every stream with a per-stream consumer and waits at most `timeout` for it to reach the
  /// consumers' sinks. The streams are torn down, `Removed` fires for each, and no new stream is activated
  /// afterwards. Fails if a stream could not be drained in time.
  ///
  /// It never waits for a callback, so a callback may call it.
  pub async fn drain_streams(&self, timeout: Duration) -> Result<()> {
    let (reply, drained) = oneshot::channel();
    self.shared.jingle.send(Command::Drain { timeout, reply }).ok().context("the conference is over")?;
    drained.await.context("the conference is over")?
  }

  /// Register a factory that provides a downstream sink element for each new stream.
  /// Called synchronously before the stream's upstream pad is connected, so the lib
  /// can link downstream before data starts flowing. It must not call back into the conference.
  ///
  /// Return `Some(element)` to route the stream there. For a global per-media-type sink (e.g. `audiomixer`)
  /// the element should already be in a bin (added via `add_bin`); the lib will use a ghost pad to bridge the
  /// bin boundary. For a per-stream element (e.g. `appsink`) leave it parentless and the lib will add it
  /// directly to its internal pipeline. Return `None` to leave the stream to the participant's bin or, if
  /// there is none, to `on_remote_stream` alone.
  ///
  /// Streams that found no consumer before are tried again.
  pub async fn set_stream_sink_factory(
    &self,
    f: impl Fn(StreamInfo) -> Option<gstreamer::Element> + Send + Sync + 'static,
  ) {
    self.shared.set_callback(&self.shared.callbacks.sink_factory, Arc::new(f));
    self.shared.media.reevaluate_streams();
  }

  /// The element that sends audio, which exists once the first media session has been set up: that is, always
  /// after `join`.
  pub async fn audio_sink_element(&self) -> Result<gstreamer::Element> {
    self
      .shared
      .media
      .audio_sink_element()
      .context("there has been no media session yet")
  }

  pub async fn video_sink_element(&self) -> Result<gstreamer::Element> {
    self
      .shared
      .media
      .video_sink_element()
      .context("there has been no media session yet")
  }

  /// Sends a message on the bridge channel. It is not kept: if the channel is down, the message is not sent.
  pub async fn send_colibri_message(&self, message: ColibriMessage) -> Result<()> {
    self
      .shared
      .colibri_sender()
      .context("there is no bridge channel")?
      .send(message)
      .await
  }

  /// Replaces what we ask the bridge to send us. The library keeps it and sends it again whenever the bridge
  /// channel is connected again and for every new media session. What the previous state set and this one does not
  /// is given back to the bridge (the bridge keeps whatever a message leaves out): the number of videos goes back to
  /// unlimited, the lists and the per-source constraints to empty, the assumed bandwidth to none, and the default
  /// constraints to the 180 pixels the bridge starts with.
  pub async fn set_receiver_video_constraints(&self, constraints: ReceiverVideoConstraints) -> Result<()> {
    self.shared.lock().receiver.replace(constraints);
    self.shared.push_receiver_constraints(false).await;
    Ok(())
  }

  async fn update_receiver_constraints(&self, update: ReceiverVideoConstraints) -> Result<()> {
    self.shared.lock().receiver.merge(update);
    self.shared.push_receiver_constraints(false).await;
    Ok(())
  }

  /// Set the maximum number of video streams to receive. Pass `-1` for unlimited.
  pub async fn set_last_n(&self, n: i32) -> Result<()> {
    self
      .update_receiver_constraints(ReceiverVideoConstraints {
        last_n: Some(n),
        ..Default::default()
      })
      .await
  }

  /// Set the maximum height we want to receive video at. JVB will downscale above this. It applies to the sources
  /// that have no constraints of their own.
  pub async fn set_max_receive_height(&self, height: u16) -> Result<()> {
    self
      .update_receiver_constraints(ReceiverVideoConstraints {
        default_constraints: Some(Constraints::height(height.into())),
        ..Default::default()
      })
      .await
  }

  /// Select (prioritise) specific sources, by name, for video delivery. JVB will prefer to send video from them
  /// even when `last_n` would normally exclude them.
  pub async fn set_selected_sources(&self, sources: Vec<String>) -> Result<()> {
    self
      .update_receiver_constraints(ReceiverVideoConstraints {
        selected_sources: Some(sources),
        ..Default::default()
      })
      .await
  }

  /// Puts sources, by name, on stage: they are given the bandwidth first.
  pub async fn set_on_stage_sources(&self, sources: Vec<String>) -> Result<()> {
    self
      .update_receiver_constraints(ReceiverVideoConstraints {
        on_stage_sources: Some(sources),
        ..Default::default()
      })
      .await
  }

  /// The bandwidth the bridge is to assume we have until it has an estimate of its own.
  /// `None` takes the assumption back.
  pub async fn set_assumed_bandwidth(&self, bps: Option<u64>) -> Result<()> {
    self.shared.lock().receiver.set_assumed_bandwidth(bps);
    self.shared.push_receiver_constraints(false).await;
    Ok(())
  }

  /// Sets or removes the policy that works out how much video to receive from the sources that exist, by the type
  /// of each. See [`VideoReceivePolicy`].
  pub async fn set_video_receive_policy(&self, policy: Option<VideoReceivePolicy>) -> Result<()> {
    self.shared.lock().receiver.set_policy(policy);
    self.shared.push_receiver_constraints(false).await;
    Ok(())
  }

  /// Sets the type of the video we send, which the bridge is told. The default is `Camera`.
  pub async fn set_video_type(&self, video_type: VideoType) -> Result<()> {
    let source_name = format!("{}-v0", self.shared.endpoint_id);
    self.shared.lock().video_type = video_type;
    if let Some(sender) = self.shared.colibri_sender() {
      if let Err(e) = sender
        .send(ColibriMessage::SourceVideoTypeMessage { source_name, video_type })
        .await
      {
        debug!("the video type is sent when the bridge channel connects: {:#}", e);
      }
    }
    Ok(())
  }

  pub async fn send_json_message<T: Serialize>(&self, payload: &T) -> Result<()> {
    let message = Message {
      from: Some(self.shared.jid.clone().into()),
      to: Some(self.shared.muc.clone().into()),
      id: Some(MessageId(Uuid::new_v4().to_string())),
      type_: MessageType::Groupchat,
      bodies: Default::default(),
      subjects: Default::default(),
      thread: None,
      payloads: vec![Element::try_from(xmpp::jitsi::JsonMessage {
        payload: serde_json::to_value(payload)?,
      })?],
    };
    self.shared.xmpp_tx.send(message.into()).await?;
    Ok(())
  }

  /// The media sources of the other participants, as announced by the focus.
  pub async fn remote_sources(&self) -> Vec<RemoteSource> {
    self.shared.lock().sources.remote_sources()
  }

  /// Register a callback for the participants of the room, other than the focus. It is called for the ones that
  /// are there now, and then for every one that arrives. Streams of a participant are not delivered before the
  /// callback has returned for it (for two seconds at most), so that it can create the `participant_<id>` bin
  /// they go to.
  pub async fn on_participant(&self, f: impl (Fn(Participant) -> BoxedResultFuture) + Send + Sync + 'static) {
    self.shared.set_on_participant(Arc::new(f));
  }

  pub async fn on_participant_left(&self, f: impl (Fn(Participant) -> BoxedResultFuture) + Send + Sync + 'static) {
    self.shared.set_callback(&self.shared.callbacks.on_participant_left, Arc::new(f));
  }

  /// Register a callback for the messages from the bridge channel that the library does not answer itself.
  pub async fn on_colibri_message(&self, f: impl (Fn(ColibriMessage) -> BoxedResultFuture) + Send + Sync + 'static) {
    self.shared.set_callback(&self.shared.callbacks.on_colibri_message, Arc::new(f));
  }

  /// Register a callback for the end of the conference, from outside or inside: we were kicked, the room was
  /// destroyed, the connection was lost, no media session came, something fatal happened, or we left. It is called
  /// once, and the media sessions are down by then; call [`leave`](Self::leave) to release the rest.
  pub async fn on_conference_left(
    &self,
    f: impl Fn(ConferenceLeftReason) -> BoxedResultFuture + Send + Sync + 'static,
  ) {
    self.shared.set_callback(&self.shared.callbacks.on_conference_left, Arc::new(f));
  }

  /// Register a callback for the media sessions with the focus: they start, end, and change the state of their
  /// connection. The conference stays joined while they come and go.
  pub async fn on_media_session(&self, f: impl Fn(MediaSessionEvent) -> BoxedResultFuture + Send + Sync + 'static) {
    self.shared.set_callback(&self.shared.callbacks.on_media_session, Arc::new(f));
  }

  /// Register a callback for the media sources that are announced and withdrawn.
  pub async fn on_source(&self, f: impl Fn(SourceEvent) -> BoxedResultFuture + Send + Sync + 'static) {
    self.shared.set_callback(&self.shared.callbacks.on_source, Arc::new(f));
  }
}

impl Shared {
  pub(crate) async fn send_presence_payloads(&self, payloads: Vec<Element>) -> Result<()> {
    let mut presence = Presence::new(presence::Type::None).with_to(self.muc_jid.clone());
    presence.payloads = payloads;
    self.xmpp_tx.send(presence.into()).await?;
    Ok(())
  }

  pub(crate) async fn run_join(self: &Arc<Self>) -> Result<(), JoinError> {
    let (session_tx, session_rx) = oneshot::channel();
    self.lock().session_waiter = Some(session_tx);

    let mut retries = 0;
    loop {
      self.request_conference().await?;
      match self.join_room().await {
        Err(JoinError::RoomCreationRestricted) if retries < ROOM_CREATION_RETRIES => {
          retries += 1;
          let delay = room_creation_retry_delay(retries);
          warn!("the room does not exist yet, asking the focus again in {:?}", delay);
          sleep(delay).await;
        },
        result => break result?,
      }
    }

    session_rx.await.map_err(|_| JoinError::ConnectionClosed)?
  }

  async fn request_conference(&self) -> Result<(), JoinError> {
    let machine_uid = Uuid::new_v4().to_string();
    let mut not_ready = Backoff::new(Duration::from_secs(1), Duration::from_secs(30));
    let mut errors = Backoff::new(Duration::from_secs(1), Duration::from_secs(120));
    loop {
      let iq = Iq::from_set(
        generate_id(),
        xmpp::jitsi::Conference {
          machine_uid: machine_uid.clone(),
          room: self.config.muc.to_string(),
          properties: HashMap::from([
            ("stereo".to_owned(), self.config.stereo.to_string()),
            ("startBitrate".to_owned(), self.config.start_bitrate.to_string()),
          ]),
        },
      )
      .with_to(self.config.focus.clone())
      .with_from(self.jid.clone().into());
      let response = self.connection.request(iq, CONFERENCE_REQUEST_TIMEOUT).await;

      let delay = match response {
        Err(RequestError::ConnectionClosed) => return Err(JoinError::ConnectionClosed),
        Err(RequestError::Timeout) => {
          warn!("the focus did not answer the conference request");
          not_ready.next_delay()
        },
        Ok(response) => match focus::interpret(&response) {
          Answer::Ready => return Ok(()),
          Answer::NotReady => {
            let delay = not_ready.next_delay();
            debug!("the focus is not ready, asking again in {:?}", delay);
            delay
          },
          Answer::Retry(error) => {
            let delay = errors.next_delay();
            if delay >= MAX_ERROR_RETRY_DELAY {
              return Err(error);
            }
            warn!("the focus refused the conference request ({}), asking again in {:?}", error, delay);
            delay
          },
          Answer::Refused(error) => return Err(error),
        },
      };
      sleep(delay).await;
    }
  }

  async fn join_room(&self) -> Result<(), JoinError> {
    let (tx, confirmed) = oneshot::channel();
    let payloads = {
      let mut state = self.lock();
      state.phase = Phase::Joining;
      state.muc_waiter = Some(tx);
      // The room may take the presence up as soon as it is sent, whether or not we hear back in time.
      state.in_room = true;
      state.presence.payloads(true)
    };
    self
      .send_presence_payloads(payloads)
      .await
      .map_err(|_| JoinError::ConnectionClosed)?;
    confirmed.await.map_err(|_| JoinError::ConnectionClosed)?
  }
}

/// ljm's `getJitterDelay(retry, 500, 1.5)`: a random delay of at least half a second, which grows with the number of
/// retries.
fn room_creation_retry_delay(retry: u32) -> Duration {
  let max = Duration::from_secs_f64(1.5f64.powi(retry as i32)).max(Duration::from_millis(501));
  Duration::from_millis(rand::rng().random_range(500..max.as_millis() as u64))
}

fn endpoint_id_for_jid(jid: &FullJid) -> Result<&str> {
  jid
    .node()
    .map(|n| n.as_str())
    .context("invalid jid")?
    .split('-')
    .next()
    .context("invalid jid")
}

/// What the media layer calls. These only enqueue work: streaming threads and the stream controller call them.
fn media_hooks(cell: &SharedCell, sync_sample: SyncSampleSink) -> MediaHooks {
  fn conference(cell: &SharedCell) -> Option<Arc<Shared>> {
    cell.get().and_then(Weak::upgrade)
  }

  MediaHooks {
    stream_event: {
      let cell = cell.clone();
      Arc::new(move |event| {
        if let Some(shared) = conference(&cell) {
          shared.dispatch_stream_event(event);
        }
      })
    },
    sink_factory: {
      let cell = cell.clone();
      Arc::new(move |info| {
        let factory = conference(&cell).and_then(|shared| current(&shared.callbacks.sink_factory));
        factory.and_then(|factory| factory(info))
      })
    },
    has_stream_handler: {
      let cell = cell.clone();
      Arc::new(move || {
        conference(&cell).is_some_and(|shared| current(&shared.callbacks.on_remote_stream).is_some())
      })
    },
    participant_ready: {
      let cell = cell.clone();
      Arc::new(move |id| match conference(&cell) {
        Some(shared) => shared.participant_ready(id),
        None => Box::pin(async {}),
      })
    },
    sync_sample,
    session_failed: {
      let cell = cell.clone();
      Arc::new(move |generation, failure| {
        if let Some(shared) = conference(&cell) {
          shared.command(Command::SessionFailed { generation, failure });
        }
      })
    },
    connection_state: {
      let cell = cell.clone();
      Arc::new(move |generation, state| {
        if let Some(shared) = conference(&cell) {
          shared.command(Command::ConnectionState { generation, state });
        }
      })
    },
    fatal: {
      let cell = cell.clone();
      Arc::new(move |message| {
        tracing::error!("fatal media error: {}", message);
        if let Some(shared) = conference(&cell) {
          shared.conference_left(ConferenceLeftReason::Fatal(message));
        }
      })
    },
  }
}
