//! The Jingle actor: one task per conference that carries out what the focus asks of the media session, one command
//! at a time. The stanza filter only parses, acknowledges and queues, so that reading stanzas never waits for ICE,
//! a WebSocket or the pipeline.

use std::{
  collections::VecDeque,
  sync::Arc,
  time::{Duration, Instant},
};

use anyhow::{Context, Result};
use jitsi_xmpp_parsers::jingle::{Action, Jingle, Transport};
use minidom::Element;
use tokio::{
  select,
  sync::{mpsc, oneshot},
  task::JoinHandle,
  time::{interval, sleep_until, MissedTickBehavior},
};
use tracing::{debug, info, warn};
use xmpp_parsers::{iq::Iq, jingle::Reason, ns};

use crate::{
  backend::Session,
  colibri::{ColibriChannel, ColibriParams},
  error::{ConferenceLeftReason, JoinError, MediaSessionEvent, SourceEvent},
  jingle::{self, AnswerParams, OfferConfig, OfferParts, SessionRef},
  muc::error_condition,
  pipeline::{ConnectionState, LocalIceCandidate, LocalTransport, SessionFailure},
  shared::Shared,
  stats::{self, StatsContext},
  util::{generate_id, AbortOnDrop},
  xmpp::iq::IqResponse,
};

const ACCEPT_TIMEOUT: Duration = Duration::from_secs(15);

const REPLY_TIMEOUT: Duration = Duration::from_secs(15);

const STATS_INTERVAL: Duration = Duration::from_secs(10);

/// Asking for a new session more often than this many times within the window is a sign that it is not going to
/// work.
const MAX_RESTARTS: usize = 5;
const RESTART_WINDOW: Duration = Duration::from_secs(5 * 60);

pub(crate) enum Command {
  Initiate(Jingle),
  SourceAdd(Jingle),
  SourceRemove(Jingle),
  TransportInfo(Jingle),
  Terminate(Jingle),
  TransportReplace(Jingle),
  ParticipantLeft(String),
  /// From the media layer, for the session of the generation.
  SessionFailed {
    generation: u64,
    failure: SessionFailure,
  },
  ConnectionState {
    generation: u64,
    state: ConnectionState,
  },
  /// The answer of the focus to our session-accept or transport-accept.
  Accepted {
    generation: u64,
    result: Result<(), String>,
  },
  /// The focus did not take a Jingle message of ours: it refused it, or gave no answer.
  NotTaken {
    sid: String,
    purpose: Purpose,
    reply: Refusal,
  },
  /// Send EOS through the streams of the session and wait for it to arrive; no stream is activated afterwards.
  Drain {
    timeout: Duration,
    reply: oneshot::Sender<Result<()>>,
  },
  /// Tell the focus, end the session and stop.
  Leave(oneshot::Sender<()>),
  /// End the session and stop.
  Shutdown(oneshot::Sender<()>),
}

/// What a Jingle message of ours was for, which is what its refusal means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Purpose {
  /// A session-terminate that asks for a new session.
  Restart,
  /// The session-info that tells the focus ICE failed, which it answers with a new session.
  IceFailed,
  Other(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
  /// An error answer: the condition of the error, and a description of it.
  Error { condition: String, description: String },
  /// No answer: the request timed out or the connection ended.
  Unanswered(String),
}

#[derive(PartialEq, Eq)]
enum Flow {
  Continue,
  Stop,
}

pub(crate) fn spawn(shared: Arc<Shared>, commands: mpsc::UnboundedReceiver<Command>) -> JoinHandle<()> {
  tokio::spawn(Actor::new(shared).run(commands))
}

struct ActiveSession {
  media: Box<dyn Session>,
  generation: u64,
  /// The focus has acknowledged our accept.
  accepted: bool,
  local: LocalTransport,
  /// Candidates found after the accept was sent; they are trickled once the focus has it.
  late_candidates: Option<mpsc::UnboundedReceiver<LocalIceCandidate>>,
  _trickle: Option<AbortOnDrop>,
  colibri: Option<ColibriChannel>,
}

struct Actor {
  shared: Arc<Shared>,
  session: Option<ActiveSession>,
  /// The Jingle session the focus has with us, until one of us ends it: it may have no media session in the
  /// meantime.
  reference: Option<SessionRef>,
  /// The offer of that session, which a transport-replace builds on.
  offer: Option<OfferParts>,
  restarts: RestartLimiter,
  /// The application has drained the streams: no stream is activated any more, in this session or in a later one.
  drained: bool,
  /// When the conference is given up for want of a media session.
  session_deadline: Option<tokio::time::Instant>,
  /// The conference is over: nothing but the end is of interest.
  over: bool,
}

impl Actor {
  fn new(shared: Arc<Shared>) -> Self {
    Self {
      shared,
      session: None,
      reference: None,
      offer: None,
      restarts: RestartLimiter::default(),
      drained: false,
      session_deadline: None,
      over: false,
    }
  }

  async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
    let mut stats = interval(STATS_INTERVAL);
    stats.set_missed_tick_behavior(MissedTickBehavior::Skip);
    stats.tick().await;
    loop {
      let deadline = self.session_deadline;
      select! {
        command = commands.recv() => match command {
          Some(command) => {
            if self.handle(command).await == Flow::Stop {
              break;
            }
          },
          None => break,
        },
        _ = stats.tick() => self.send_stats().await,
        _ = async { sleep_until(deadline.expect("guarded")).await }, if deadline.is_some() => {
          self.session_deadline = None;
          warn!("no new media session within {:?}", self.shared.config.media_session_timeout);
          self.give_up(ConferenceLeftReason::NoMediaSession).await;
        },
      }
    }
    self.end_session("the Jingle actor stopped", false).await;
    debug!("the Jingle actor stopped");
  }

  async fn handle(&mut self, command: Command) -> Flow {
    match command {
      Command::Leave(reply) => {
        self.leave().await;
        let _ = reply.send(());
        return Flow::Stop;
      },
      Command::Shutdown(reply) => {
        self.end_session("the conference is over", false).await;
        let _ = reply.send(());
        return Flow::Stop;
      },
      _ if self.over => debug!("ignoring a command, the conference is over"),
      Command::Initiate(jingle) => self.initiate(jingle).await,
      Command::SourceAdd(jingle) => self.source_add(jingle).await,
      Command::SourceRemove(jingle) => self.source_remove(&jingle).await,
      Command::TransportInfo(jingle) => self.transport_info(&jingle),
      Command::Terminate(jingle) => self.terminated(&jingle).await,
      Command::TransportReplace(jingle) => self.transport_replace(jingle).await,
      Command::ParticipantLeft(participant_id) => self.participant_left(&participant_id).await,
      Command::SessionFailed { generation, failure } => {
        if self.is_current(generation) {
          self.session_failed(failure).await;
        }
      },
      Command::Drain { timeout, reply } => {
        self.drained = true;
        let result = match &self.session {
          Some(session) => session.media.drain(timeout).await,
          None => Ok(()),
        };
        let _ = reply.send(result);
      },
      Command::ConnectionState { generation, state } => {
        if self.is_current(generation) {
          self
            .shared
            .dispatch_media_session(MediaSessionEvent::ConnectionStateChanged(state));
        }
      },
      Command::NotTaken { sid, purpose, reply } => self.not_taken(&sid, purpose, reply).await,
      Command::Accepted { generation, result } => {
        if self.is_current(generation) {
          match result {
            Ok(()) => self.session_accepted().await,
            Err(reason) => self.session_rejected(reason).await,
          }
        }
      },
    }
    Flow::Continue
  }

  fn is_current(&self, generation: u64) -> bool {
    self.session.as_ref().is_some_and(|session| session.generation == generation)
  }

  async fn initiate(&mut self, jingle: Jingle) {
    self.session_deadline = None;
    self.end_session("replaced by a new session", true).await;
    self.offer = None;
    self.reference = Some(SessionRef::of(&jingle));

    let parsed = jingle::parse_offer(&jingle, &self.offer_config(), None);
    let result = match parsed {
      Ok(offer) => {
        info!("Received Jingle session-initiate for {}", offer.sid);
        let events = self.shared.lock().sources.replace(offer.sources.clone());
        self.sources_changed(events).await;
        self.start(offer, Action::SessionAccept).await
      },
      Err(e) => Err(e),
    };
    if let Err(e) = result {
      self.start_failed(e).await;
    }
  }

  async fn source_add(&mut self, jingle: Jingle) {
    let sources = match jingle::sources_from_jingle(&jingle) {
      Ok(sources) => sources,
      Err(e) => {
        warn!("ignoring a source-add that does not parse: {:#}", e);
        return;
      },
    };
    debug!("source-add: {:?}", sources);
    let events = self.shared.lock().sources.add(sources.clone());
    if let Some(session) = &self.session {
      session.media.sources_added(sources).await;
    }
    self.sources_changed(events).await;
  }

  async fn source_remove(&mut self, jingle: &Jingle) {
    let ssrcs = jingle::removed_ssrcs(jingle);
    debug!("source-remove: {:?}", ssrcs);
    let events = self.shared.lock().sources.remove(&ssrcs);
    if let Some(session) = &self.session {
      session.media.sources_removed(ssrcs).await;
    }
    self.sources_changed(events).await;
  }

  fn transport_info(&self, jingle: &Jingle) {
    let Some(session) = &self.session else {
      debug!("ignoring a transport-info, there is no media session");
      return;
    };
    for content in &jingle.contents {
      if let Some(Transport::IceUdp(transport)) = &content.transport {
        if let Err(e) = session.media.add_remote_candidates(transport) {
          warn!("transport-info handling failed: {:#}", e);
        }
      }
    }
  }

  /// The focus has ended the session, normally to start another one.
  async fn terminated(&mut self, jingle: &Jingle) {
    let reason = jingle
      .reason
      .as_ref()
      .map_or_else(|| "terminated by the focus".to_owned(), ToString::to_string);
    if let Some(current) = self.reference.as_ref().filter(|current| current.sid != jingle.sid.0) {
      debug!("ignoring the session-terminate for {}, the session is {}", jingle.sid.0, current.sid);
      return;
    }
    info!("Received Jingle session-terminate: {}", reason);
    self.reference = None;
    self.offer = None;
    self.end_session(&reason, true).await;
    self.expect_session();
  }

  async fn transport_replace(&mut self, jingle: Jingle) {
    let Some(prior) = self.offer.clone().filter(|offer| offer.sid == jingle.sid.0) else {
      warn!("rejecting a transport-replace for a session without an offer");
      self
        .send_watched(jingle::transport_reject(&jingle), Purpose::Other("transport-reject"))
        .await;
      return;
    };
    info!("Received Jingle transport-replace for {}", prior.sid);
    self.session_deadline = None;
    // The new session reaches the same consumers as the old one did: the old one has to be gone first.
    self.end_session("the transport was replaced", true).await;
    // The replace names the allocation on the bridge that takes over. The focus only takes what we say about the
    // session (ICE failed, terminate) if it names that bridge session, not the one that has been replaced.
    self.reference = Some(SessionRef::from(&prior).replaced(&jingle));

    let result = match jingle::parse_offer(&jingle, &self.offer_config(), Some(&prior)) {
      Ok(offer) => {
        if !offer.sources.is_empty() {
          let events = self.shared.lock().sources.replace(offer.sources.clone());
          self.sources_changed(events).await;
        }
        self.start(offer, Action::TransportAccept).await
      },
      Err(e) => Err(e),
    };
    if let Err(e) = result {
      warn!("failed to replace the transport: {:#}", e);
      self
        .send_watched(jingle::transport_reject(&jingle), Purpose::Other("transport-reject"))
        .await;
      self.restart(Reason::FailedApplication, format!("{e:#}")).await;
    }
  }

  async fn participant_left(&mut self, participant_id: &str) {
    let events = self.shared.lock().sources.remove_participant(participant_id);
    if let Some(session) = &self.session {
      session.media.participant_left(participant_id).await;
    }
    self.sources_changed(events).await;
  }

  async fn sources_changed(&self, events: Vec<SourceEvent>) {
    if events.is_empty() {
      return;
    }
    self.shared.dispatch_sources(events);
    self.shared.push_receiver_constraints(true).await;
  }

  fn offer_config(&self) -> OfferConfig<'_> {
    OfferConfig {
      video_codecs: &self.shared.config.video_codecs,
      receive_mode: self.shared.config.receive_mode,
    }
  }

  /// Sets up the media for `offer` and answers it.
  async fn start(&mut self, offer: OfferParts, action: Action) -> Result<()> {
    let shared = self.shared.clone();
    self.offer = Some(offer.clone());

    let services = shared.connection.external_services_fresh().await;
    let descriptions = shared.lock().sources.descriptions();
    let mut media = shared
      .media
      .start_session(&offer.session_offer(descriptions), &services)
      .await
      .context("failed to start the media session")?;
    let generation = media.generation();
    if self.drained {
      // Before anything can come in: the application wants no more streams.
      if let Err(e) = media.drain(Duration::ZERO).await {
        warn!("failed to stop the new media session from activating streams: {:#}", e);
      }
    }
    let local = media.local_transport();
    let (candidates, late_candidates) = media.gather_candidates(shared.config.ice_gathering_timeout).await;
    debug!("local candidates: {:?}", candidates);

    let answer = jingle::answer(AnswerParams {
      action,
      offer: &offer,
      responder: &shared.jid,
      endpoint_id: &shared.endpoint_id,
      video_codecs: &shared.config.video_codecs,
      local_ssrcs: shared.media.local_ssrcs(),
      local: &local,
      candidates: &candidates,
    });
    let pending = match answer {
      Ok(answer) => {
        let iq = Iq::from_set(generate_id(), answer)
          .with_to(shared.focus_jid.clone().into())
          .with_from(shared.jid.clone().into());
        shared.connection.send_request(iq).await.map_err(anyhow::Error::from)
      },
      Err(e) => Err(e),
    };
    let pending = match pending {
      Ok(pending) => pending,
      Err(e) => {
        media.shutdown(Duration::ZERO).await;
        return Err(e);
      },
    };

    let commands = shared.jingle.clone();
    tokio::spawn(async move {
      let result = match pending.wait(ACCEPT_TIMEOUT).await {
        Ok(IqResponse::Result(_)) => Ok(()),
        Ok(IqResponse::Error(error)) => Err(describe_error(&error)),
        Err(e) => Err(e.to_string()),
      };
      let _ = commands.send(Command::Accepted { generation, result });
    });
    self.session = Some(ActiveSession {
      media,
      generation,
      accepted: false,
      local,
      late_candidates,
      _trickle: None,
      colibri: None,
    });
    Ok(())
  }

  async fn session_accepted(&mut self) {
    let shared = self.shared.clone();
    let (Some(session), Some(offer)) = (self.session.as_mut(), self.offer.as_ref()) else {
      return;
    };
    debug!("Focus acknowledged the accept of media session {}", session.generation);
    session.accepted = true;
    shared.dispatch_media_session(MediaSessionEvent::Started {
      bridge_session_id: offer.bridge_session_id.clone(),
    });

    shared.bridge_endpoint_created();
    match &offer.ice.colibri_ws_url {
      Some(url) => {
        info!("Connecting Colibri WebSocket to {}", url);
        let channel = ColibriChannel::spawn(ColibriParams {
          url: url.clone(),
          tls_insecure: shared.connection.tls_insecure,
          endpoint_id: shared.endpoint_id.clone(),
          on_message: {
            let shared = shared.clone();
            Arc::new(move |message| shared.dispatch_colibri(message))
          },
          on_connected: {
            let shared = shared.clone();
            Arc::new(move || shared.colibri_greeting())
          },
          reconnect: ColibriParams::default_reconnect(),
        });
        shared.set_colibri_sender(Some(channel.sender()));
        session.colibri = Some(channel);
      },
      None => warn!("the offer has no Colibri WebSocket: the bridge channel is not available"),
    }

    if let Some(mut candidates) = session.late_candidates.take() {
      let (shared, offer, local) = (shared.clone(), offer.clone(), session.local.clone());
      session._trickle = Some(AbortOnDrop(tokio::spawn(async move {
        while let Some(candidate) = candidates.recv().await {
          let jingle = jingle::trickle_candidate(&offer, &local, &candidate);
          let iq = Iq::from_set(generate_id(), jingle)
            .with_to(shared.focus_jid.clone().into())
            .with_from(shared.jid.clone().into());
          if shared.xmpp_tx.send(iq.into()).await.is_err() {
            break;
          }
        }
      })));
    }

    self.shared.complete_join(Ok(()));
  }

  async fn session_rejected(&mut self, reason: String) {
    warn!("the focus rejected the media session: {}", reason);
    if self.shared.join_pending() {
      self.terminate_session(Reason::FailedApplication, Some(&reason), false).await;
      self.end_session(&reason, false).await;
      self.shared.complete_join(Err(JoinError::SessionRejected(reason)));
    }
    else {
      self.restart(Reason::FailedApplication, reason).await;
    }
  }

  async fn start_failed(&mut self, error: anyhow::Error) {
    let text = format!("{error:#}");
    warn!("failed to set up the media session: {}", text);
    if self.shared.join_pending() {
      self.terminate_session(Reason::FailedApplication, Some(&text), false).await;
      self.shared.complete_join(Err(JoinError::SessionFailed(text)));
    }
    else {
      self.restart(Reason::FailedApplication, text).await;
    }
  }

  async fn session_failed(&mut self, failure: SessionFailure) {
    warn!("the media session failed: {:?}", failure);
    match failure {
      SessionFailure::Ice => {
        if !self.restarts.allow(Instant::now()) {
          return self.give_up_after_restarts("ICE failed").await;
        }
        // Jicofo answers with a new session, by whichever way it is set to re-invite. If it has ended the session
        // already, it does not want to hear about it.
        let reference = self
          .reference
          .as_ref()
          .filter(|reference| self.shared.session_is_known(&reference.sid));
        if let Some(reference) = reference {
          self.send_watched(jingle::ice_failed(reference), Purpose::IceFailed).await;
        }
        self.end_session("ICE failed", true).await;
        self.expect_session();
      },
      SessionFailure::Dtls(message) | SessionFailure::Transport(message) => {
        self.restart(Reason::ConnectivityError, message).await;
      },
      SessionFailure::Security(message) => {
        self
          .terminate_session(Reason::SecurityError, Some(&message), false)
          .await;
        self.give_up(ConferenceLeftReason::Fatal(message)).await;
      },
    }
  }

  async fn restart(&mut self, reason: Reason, text: String) {
    if !self.restarts.allow(Instant::now()) {
      return self.give_up_after_restarts(&text).await;
    }
    warn!("requesting a new media session: {}", text);
    self.terminate_session(reason, Some(&text), true).await;
    self.end_session(&text, true).await;
    self.offer = None;
    self.expect_session();
  }

  async fn give_up_after_restarts(&mut self, last_reason: &str) {
    self
      .give_up(ConferenceLeftReason::Fatal(format!(
        "more than {MAX_RESTARTS} media session restarts within {} minutes, the last because: {last_reason}",
        RESTART_WINDOW.as_secs() / 60
      )))
      .await;
  }

  async fn give_up(&mut self, reason: ConferenceLeftReason) {
    self.end_session("the conference is over", false).await;
    self.over = true;
    self.shared.conference_left(reason);
  }

  /// A new session is due: if none comes within `media_session_timeout`, the conference is given up.
  fn expect_session(&mut self) {
    self.session_deadline = Some(tokio::time::Instant::now() + self.shared.config.media_session_timeout);
  }

  /// The Jingle session to tell the focus we are done with, if the focus still has one. It was ended by the focus
  /// already if the stanza filter has seen that, which it may have done while this was waiting in the queue.
  fn session_to_end(&mut self) -> Option<SessionRef> {
    let reference = self.reference.take()?;
    if self.shared.session_ended(&reference.sid) {
      Some(reference)
    }
    else {
      debug!("the focus has ended the session {} already", reference.sid);
      None
    }
  }

  /// Tells the focus we are done with the Jingle session, if there still is one. `restart`: and that we want
  /// another.
  async fn terminate_session(&mut self, reason: Reason, text: Option<&str>, restart: bool) {
    if let Some(reference) = self.session_to_end() {
      let purpose = if restart {
        Purpose::Restart
      }
      else {
        Purpose::Other("session-terminate")
      };
      self
        .send_watched(jingle::session_terminate(&reference, reason, text, restart), purpose)
        .await;
    }
  }

  async fn leave(&mut self) {
    if let Some(reference) = self.session_to_end() {
      self
        .send_jingle(jingle::session_terminate(&reference, Reason::Success, None, false))
        .await;
    }
    self.end_session("left the conference", false).await;
  }

  /// Shuts down the media session, if there is one. Its streams are retired before the application hears that it
  /// has ended.
  async fn end_session(&mut self, reason: &str, restarting: bool) {
    let Some(session) = self.session.take() else {
      return;
    };
    self.shared.set_colibri_sender(None);
    let ActiveSession {
      media,
      accepted,
      colibri,
      _trickle,
      ..
    } = session;
    drop(colibri);
    drop(_trickle);
    media.shutdown(self.shared.config.stream_drain_timeout).await;
    if accepted {
      self.shared.dispatch_media_session(MediaSessionEvent::Ended {
        reason: reason.to_owned(),
        restarting,
      });
    }
  }

  async fn send_jingle(&self, jingle: Jingle) {
    let iq = Iq::from_set(generate_id(), jingle)
      .with_to(self.shared.focus_jid.clone().into())
      .with_from(self.shared.jid.clone().into());
    if self.shared.xmpp_tx.send(iq.into()).await.is_err() {
      debug!("the connection is closed, the Jingle message was not sent");
    }
  }

  /// Sends a Jingle message and looks at the answer: a refusal comes back as [`Command::NotTaken`].
  async fn send_watched(&self, jingle: Jingle, purpose: Purpose) {
    let sid = jingle.sid.0.clone();
    let iq = Iq::from_set(generate_id(), jingle)
      .with_to(self.shared.focus_jid.clone().into())
      .with_from(self.shared.jid.clone().into());
    let pending = match self.shared.connection.send_request(iq).await {
      Ok(pending) => pending,
      Err(e) => {
        debug!("the Jingle message was not sent: {}", e);
        return;
      },
    };
    let commands = self.shared.jingle.clone();
    tokio::spawn(async move {
      let reply = match pending.wait(REPLY_TIMEOUT).await {
        Ok(IqResponse::Result(_)) => return,
        Ok(IqResponse::Error(error)) => Refusal::Error {
          condition: error_condition(&error).unwrap_or_default(),
          description: describe_error(&error),
        },
        Err(e) => Refusal::Unanswered(e.to_string()),
      };
      let _ = commands.send(Command::NotTaken { sid, purpose, reply });
    });
  }

  async fn not_taken(&mut self, sid: &str, purpose: Purpose, reply: Refusal) {
    let description = match reply {
      Refusal::Unanswered(reason) => {
        debug!("no answer to the {:?} for the session {}: {}", purpose, sid, reason);
        return;
      },
      Refusal::Error { condition, description } => {
        warn!("the focus refused the {:?} for the session {}: {}", purpose, sid, description);
        if condition != "resource-constraint" {
          // Not a rate limit. The focus does not know the session or the bridge session we named, because it has
          // moved on without telling us yet or because it thinks differently. Whether a new session comes is
          // for the time limit of `media_session_timeout` to decide.
          return;
        }
        description
      },
    };
    // The rate limit of restarts: the focus has ended the session all the same, and it starts no other. There is
    // no session left to ask again about, so the conference cannot go on.
    let waiting = self.session.is_none() && self.reference.is_none() && self.session_deadline.is_some();
    if purpose == Purpose::Restart && waiting {
      self
        .give_up(ConferenceLeftReason::Fatal(format!(
          "the focus rate-limited our request for a new media session ({description}) and ended the session without \
           starting another"
        )))
        .await;
    }
  }

  async fn send_stats(&self) {
    let Some(session) = self.session.as_ref().filter(|session| session.accepted) else {
      return;
    };
    let Some(sender) = session.colibri.as_ref().map(ColibriChannel::sender) else {
      return;
    };
    if !sender.is_connected() {
      return;
    }
    let remote_sources = self.shared.lock().sources.remote_sources();
    let message = stats::endpoint_stats(
      &session.media.rtp_source_stats(),
      &StatsContext {
        remote_sources: &remote_sources,
        local_ssrcs: self.shared.media.local_ssrcs(),
        region: self.shared.config.region.as_deref(),
        max_enabled_resolution: self.shared.config.send_resolution,
      },
    );
    if let Err(e) = sender.send(message).await {
      warn!("failed to send stats: {:#}", e);
    }
  }
}

fn describe_error(error: &Element) -> String {
  let condition = error_condition(error).unwrap_or_else(|| "unknown error".to_owned());
  match error.get_child("text", ns::XMPP_STANZAS).map(Element::text) {
    Some(text) if !text.is_empty() => format!("{condition}: {text}"),
    _ => condition,
  }
}

#[derive(Debug, Default)]
pub(crate) struct RestartLimiter {
  requests: VecDeque<Instant>,
}

impl RestartLimiter {
  /// Records a restart at `now`. Returns false if it is one too many.
  pub(crate) fn allow(&mut self, now: Instant) -> bool {
    while self
      .requests
      .front()
      .is_some_and(|request| now.duration_since(*request) > RESTART_WINDOW)
    {
      self.requests.pop_front();
    }
    if self.requests.len() >= MAX_RESTARTS {
      return false;
    }
    self.requests.push_back(now);
    true
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn the_sixth_restart_within_five_minutes_is_one_too_many() {
    let start = Instant::now();
    let mut limiter = RestartLimiter::default();
    for i in 0..MAX_RESTARTS {
      assert!(limiter.allow(start + Duration::from_secs(i as u64 * 10)));
    }
    assert!(!limiter.allow(start + Duration::from_secs(60)));
    // The refused one does not count against the later ones: once the first has aged out, there is room again.
    assert!(limiter.allow(start + RESTART_WINDOW + Duration::from_secs(1)));
    assert!(!limiter.allow(start + RESTART_WINDOW + Duration::from_secs(2)));
  }

  #[test]
  fn describes_an_error() {
    let error: Element = "<error xmlns='jabber:client' type='cancel'><gone xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
      <text xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'>session has been replaced</text></error>"
      .parse()
      .unwrap();
    assert_eq!(describe_error(&error), "gone: session has been replaced");
  }
}
