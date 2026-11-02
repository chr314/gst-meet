//! The state of a conference that the public handle, the stanza filter, the Jingle actor and the media hooks share.
//!
//! The state is behind a plain mutex that is never held across an await and never while a callback is called:
//! callbacks only ever run on the [dispatcher](crate::dispatcher), which is fed through a queue.

use std::{
  collections::{hash_map::Entry, HashMap},
  future::Future,
  pin::Pin,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, MutexGuard, OnceLock, RwLock,
  },
  time::Duration,
};

use colibri::{ColibriMessage, VideoType};
use jid::{BareJid, FullJid, ResourcePart};
use tokio::{
  runtime::Handle,
  sync::{mpsc, oneshot, watch},
};
use tracing::{debug, error, warn};
use xmpp_parsers::presence::{self, Presence};

use crate::{
  backend::Media,
  colibri::ColibriSender,
  conference::{JitsiConferenceConfig, Participant},
  dispatcher::{Callback, EventDispatcher},
  error::{ConferenceLeftReason, JoinError, MediaSessionEvent, SourceEvent},
  jingle_actor::Command,
  pipeline::{StreamEvent, StreamInfo, SyncSampleSink},
  presence::PresenceState,
  receiver::{BridgeConstraints, ReceiverState},
  sources::SourceMap,
  stanza_filter::StanzaFilterId,
  xmpp::connection::Connection,
};

const ACTOR_STOP_TIMEOUT: Duration = Duration::from_secs(30);

const CALLBACKS_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) type JoinWaiter = oneshot::Sender<Result<(), JoinError>>;
pub(crate) type SinkFactory = Arc<dyn Fn(StreamInfo) -> Option<gstreamer::Element> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
  /// The room has not confirmed our presence yet.
  Joining,
  Joined,
  Left,
}

pub(crate) struct ParticipantEntry {
  pub(crate) participant: Participant,
  /// Turns `true` once the application is done with the participant: `on_participant` has returned.
  pub(crate) ready: Arc<watch::Sender<bool>>,
}

pub(crate) struct State {
  pub(crate) phase: Phase,
  /// We may be in the room, as far as the room is concerned: from the moment the joining presence is sent until the
  /// room refuses it or throws us out. An unavailable presence is due when we leave. The room may have accepted the
  /// presence long before it tells us, so this does not wait for the confirmation.
  pub(crate) in_room: bool,
  /// By the resource of the occupant JID, which is the endpoint ID.
  pub(crate) participants: HashMap<String, ParticipantEntry>,
  pub(crate) on_participant: Option<Callback<Participant>>,
  pub(crate) presence: PresenceState,
  /// The Jingle session the focus has with us, as far as the stanza filter knows: it is set when the focus offers a
  /// session and cleared when one of us ends that session. It runs ahead of the Jingle actor, which still works its
  /// way through the commands queued before, so whoever clears it must name the session it means to end (see
  /// [`Shared::session_ended`]).
  pub(crate) known_sid: Option<String>,
  pub(crate) sources: SourceMap,
  pub(crate) receiver: ReceiverState,
  pub(crate) bridge_constraints: BridgeConstraints,
  pub(crate) video_type: VideoType,
  /// Waits for our presence to be confirmed.
  pub(crate) muc_waiter: Option<JoinWaiter>,
  /// Waits for the first media session to be accepted.
  pub(crate) session_waiter: Option<JoinWaiter>,
  pub(crate) left: Option<ConferenceLeftReason>,
}

/// The callbacks that are stored as they are: they have no state to replay when they are set.
#[derive(Default)]
pub(crate) struct Callbacks {
  pub(crate) on_participant_left: RwLock<Option<Callback<Participant>>>,
  pub(crate) on_colibri_message: RwLock<Option<Callback<ColibriMessage>>>,
  pub(crate) on_remote_stream: RwLock<Option<Callback<StreamEvent>>>,
  pub(crate) sink_factory: RwLock<Option<SinkFactory>>,
  pub(crate) on_conference_left: RwLock<Option<Callback<ConferenceLeftReason>>>,
  pub(crate) on_media_session: RwLock<Option<Callback<MediaSessionEvent>>>,
  pub(crate) on_source: RwLock<Option<Callback<SourceEvent>>>,
}

pub(crate) fn current<T: Clone>(slot: &RwLock<Option<T>>) -> Option<T> {
  slot.read().unwrap_or_else(|e| e.into_inner()).clone()
}

pub(crate) fn store<T>(slot: &RwLock<Option<T>>, value: T) {
  *slot.write().unwrap_or_else(|e| e.into_inner()) = Some(value);
}

fn clear<T>(slot: &RwLock<Option<T>>) {
  // Dropped after the lock is released: the callback may own the last handle on the conference.
  let old = slot.write().unwrap_or_else(|e| e.into_inner()).take();
  drop(old);
}

pub(crate) struct Shared {
  pub(crate) config: JitsiConferenceConfig,
  pub(crate) jid: FullJid,
  pub(crate) endpoint_id: String,
  pub(crate) muc: BareJid,
  pub(crate) muc_jid: FullJid,
  /// The focus as an occupant of the room, which is where the Jingle IQs come from.
  pub(crate) focus_jid: FullJid,
  pub(crate) connection: Connection,
  pub(crate) xmpp_tx: mpsc::Sender<minidom::Element>,
  pub(crate) media: Arc<dyn Media>,
  pub(crate) sync_sample_sink: SyncSampleSink,
  pub(crate) dispatcher: EventDispatcher,
  pub(crate) callbacks: Callbacks,
  pub(crate) state: Mutex<State>,
  pub(crate) jingle: mpsc::UnboundedSender<Command>,
  pub(crate) colibri: Mutex<Option<ColibriSender>>,
  pub(crate) filter_id: OnceLock<StanzaFilterId>,
  pub(crate) runtime: Handle,
  /// Whether `tear_down` has run to the end.
  pub(crate) torn_down: tokio::sync::Mutex<bool>,
  /// The callbacks have been dropped for good, and no new ones are taken.
  pub(crate) released: AtomicBool,
  /// Held from working out a receiver constraints message to having it queued, so that the messages reach the
  /// bridge in the order of the states they were worked out from.
  pub(crate) constraints_gate: tokio::sync::Mutex<()>,
}

impl Shared {
  pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
    self.state.lock().unwrap_or_else(|e| e.into_inner())
  }

  pub(crate) fn colibri_sender(&self) -> Option<ColibriSender> {
    self.colibri.lock().unwrap_or_else(|e| e.into_inner()).clone()
  }

  pub(crate) fn set_colibri_sender(&self, sender: Option<ColibriSender>) {
    *self.colibri.lock().unwrap_or_else(|e| e.into_inner()) = sender;
  }

  pub(crate) fn command(&self, command: Command) {
    // The actor has stopped: the conference is over.
    let _ = self.jingle.send(command);
  }

  /// A participant that the room announces. Known ones keep their entry; their nickname and JID may change.
  pub(crate) fn participant_present(&self, id: &str, jid: Option<FullJid>, nick: Option<String>) {
    let Ok(resource) = ResourcePart::new(id) else {
      warn!("ignoring an occupant with an invalid nickname: {:?}", id);
      return;
    };
    let mut state = self.lock();
    let callback = state.on_participant.clone();
    match state.participants.entry(id.to_owned()) {
      Entry::Occupied(mut entry) => {
        let participant = &mut entry.get_mut().participant;
        if jid.is_some() {
          participant.jid = jid;
        }
        if nick.is_some() {
          participant.nick = nick;
        }
      },
      Entry::Vacant(entry) => {
        let participant = Participant {
          jid,
          muc_jid: self.muc.clone().with_resource(&resource),
          nick,
        };
        let ready = Arc::new(watch::channel(false).0);
        entry.insert(ParticipantEntry {
          participant: participant.clone(),
          ready: ready.clone(),
        });
        debug!("new participant: {:?}", participant);
        self.announce_participant(callback, participant, ready);
      },
    }
  }

  /// Hands a participant to `on_participant` and, whatever the callback does, marks it ready afterwards. With no
  /// callback there is nothing to wait for.
  fn announce_participant(
    &self,
    callback: Option<Callback<Participant>>,
    participant: Participant,
    ready: Arc<watch::Sender<bool>>,
  ) {
    match callback {
      Some(callback) => {
        let media = self.media.clone();
        self.dispatcher.dispatch_then("on_participant", callback, participant, move || {
          ready.send_replace(true);
          // The callback may have built what the streams of the participant go to.
          media.reevaluate_streams();
        });
      },
      None => {
        ready.send_replace(true);
      },
    }
  }

  /// Sets `on_participant`, and hands it the participants that are there already.
  pub(crate) fn set_on_participant(&self, callback: Callback<Participant>) {
    if self.released.load(Ordering::Acquire) {
      return;
    }
    let mut state = self.lock();
    state.on_participant = Some(callback.clone());
    for entry in state.participants.values() {
      self.announce_participant(Some(callback.clone()), entry.participant.clone(), entry.ready.clone());
    }
  }

  pub(crate) fn participant_gone(&self, id: &str) {
    let removed = self.lock().participants.remove(id);
    if let Some(entry) = removed {
      debug!("participant left: {:?}", entry.participant);
      if let Some(callback) = current(&self.callbacks.on_participant_left) {
        self.dispatcher.dispatch("on_participant_left", callback, entry.participant);
      }
      self.command(Command::ParticipantLeft(id.to_owned()));
    }
  }

  /// The media hook for a source of a participant that shows up. Completes when the application has had its say about
  /// the participant, if the room has announced it. It never makes a participant: the participant may have left by
  /// then, and one made now would never leave again.
  pub(crate) fn participant_ready(&self, id: &str) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    let ready = self.lock().participants.get(id).map(|entry| entry.ready.subscribe());
    Box::pin(async move {
      if let Some(mut ready) = ready {
        // The sender goes away with the participant.
        let _ = ready.wait_for(|ready| *ready).await;
      }
    })
  }

  pub(crate) fn dispatch_sources(&self, events: Vec<SourceEvent>) {
    if let Some(callback) = current(&self.callbacks.on_source) {
      for event in events {
        self.dispatcher.dispatch("on_source", callback.clone(), event);
      }
    }
  }

  pub(crate) fn dispatch_media_session(&self, event: MediaSessionEvent) {
    if let Some(callback) = current(&self.callbacks.on_media_session) {
      self.dispatcher.dispatch("on_media_session", callback, event);
    }
  }

  pub(crate) fn dispatch_stream_event(&self, event: StreamEvent) {
    if let Some(callback) = current(&self.callbacks.on_remote_stream) {
      self.dispatcher.dispatch("on_remote_stream", callback, event);
    }
  }

  pub(crate) fn dispatch_colibri(&self, message: ColibriMessage) {
    if let Some(callback) = current(&self.callbacks.on_colibri_message) {
      self.dispatcher.dispatch("on_colibri_message", callback, message);
    }
  }

  /// Sends the receiver constraints, unless the bridge channel is down: they are sent when it comes up.
  /// `only_changes` leaves out what the bridge has been told already.
  pub(crate) async fn push_receiver_constraints(&self, only_changes: bool) {
    let _gate = self.constraints_gate.lock().await;
    let Some(sender) = self.colibri_sender().filter(ColibriSender::is_connected) else {
      return;
    };
    let constraints = {
      let mut state = self.lock();
      let wanted = state.receiver.effective(&state.sources.remote_sources());
      match state.bridge_constraints.update(wanted, only_changes) {
        Some(constraints) => constraints,
        None => return,
      }
    };
    if let Err(e) = sender.send(ColibriMessage::ReceiverVideoConstraints(constraints)).await {
      warn!("failed to send the receiver video constraints: {:#}", e);
      self.lock().bridge_constraints.lose_track();
    }
  }

  /// A media session has a new endpoint on the bridge, which holds no constraints of ours.
  pub(crate) fn bridge_endpoint_created(&self) {
    self.lock().bridge_constraints.clear();
  }

  /// What a bridge channel starts with, whether it is new or has just reconnected. It is worked out after the channel
  /// has been marked as connected: a constraint set from then on is queued behind these, and one set before is in
  /// them, so none is lost.
  pub(crate) fn colibri_greeting(&self) -> Vec<ColibriMessage> {
    let mut state = self.lock();
    let wanted = state.receiver.effective(&state.sources.remote_sources());
    let mut messages = vec![];
    if let Some(constraints) = state.bridge_constraints.update(wanted, false) {
      messages.push(ColibriMessage::ReceiverVideoConstraints(constraints));
    }
    messages.push(ColibriMessage::SourceVideoTypeMessage {
      source_name: format!("{}-v0", self.endpoint_id),
      video_type: state.video_type,
    });
    messages
  }

  /// Hands the result of the join to whoever waits for it; only the first result counts.
  pub(crate) fn complete_join(&self, result: Result<(), JoinError>) {
    let waiter = self.lock().session_waiter.take();
    if let Some(waiter) = waiter {
      let _ = waiter.send(result);
    }
  }

  pub(crate) fn join_pending(&self) -> bool {
    self.lock().session_waiter.is_some()
  }

  /// Records that the conference is over and fails the join if it is still going on. Returns false if the
  /// conference was over already.
  fn mark_left(&self, reason: &ConferenceLeftReason) -> bool {
    let waiters = {
      let mut state = self.lock();
      if state.left.is_some() {
        return false;
      }
      state.left = Some(reason.clone());
      state.phase = Phase::Left;
      if matches!(
        reason,
        ConferenceLeftReason::Kicked { .. } | ConferenceLeftReason::RoomDestroyed { .. }
      ) {
        state.in_room = false;
      }
      [state.muc_waiter.take(), state.session_waiter.take()]
    };
    let join_error = match reason {
      ConferenceLeftReason::Fatal(message) => JoinError::SessionFailed(message.clone()),
      ConferenceLeftReason::Kicked { reason } => JoinError::NotAuthorized(reason.clone()),
      ConferenceLeftReason::NoMediaSession => JoinError::Timeout,
      ConferenceLeftReason::Left
      | ConferenceLeftReason::ConnectionLost
      | ConferenceLeftReason::RoomDestroyed { .. } => JoinError::ConnectionClosed,
    };
    for waiter in waiters.into_iter().flatten() {
      let _ = waiter.send(Err(join_error.clone()));
    }
    true
  }

  fn notify_conference_left(&self, reason: ConferenceLeftReason) {
    match current(&self.callbacks.on_conference_left) {
      Some(callback) => self.dispatcher.dispatch("on_conference_left", callback, reason),
      None => debug!("the conference is over ({:?}) and nobody listens", reason),
    }
  }

  /// The conference is over for the reason given. The media session is shut down first, then the application is
  /// told. Does nothing the second time.
  pub(crate) fn conference_left(self: &Arc<Self>, reason: ConferenceLeftReason) {
    if !self.mark_left(&reason) {
      return;
    }
    let this = self.clone();
    self.runtime.spawn(async move {
      this.stop_session_actor(false).await;
      this.notify_conference_left(reason);
    });
  }

  /// Ends the media session and stops the actor. `leave`: tell the focus, too.
  pub(crate) async fn stop_session_actor(&self, leave: bool) {
    let (reply, stopped) = oneshot::channel();
    let command = if leave {
      Command::Leave(reply)
    }
    else {
      Command::Shutdown(reply)
    };
    if self.jingle.send(command).is_err() {
      return;
    }
    if tokio::time::timeout(ACTOR_STOP_TIMEOUT, stopped).await.is_err() {
      error!("the media session did not end within {:?}", ACTOR_STOP_TIMEOUT);
    }
  }

  /// Everything a conference that has been joined, or has failed to, needs to be rid of. Safe to call again, also
  /// from a callback: a second call returns as soon as the first has stopped the session, the pipeline and the
  /// stanza filter.
  pub(crate) async fn tear_down(self: &Arc<Self>) {
    {
      let mut torn_down = self.torn_down.lock().await;
      if *torn_down {
        return;
      }
      *torn_down = true;

      let first = self.mark_left(&ConferenceLeftReason::Left);
      self.stop_session_actor(true).await;
      if first {
        self.notify_conference_left(ConferenceLeftReason::Left);
      }

      let in_room = std::mem::take(&mut self.lock().in_room);
      if in_room {
        let presence = Presence::new(presence::Type::Unavailable).with_to(self.muc_jid.clone());
        if self.xmpp_tx.send(presence.into()).await.is_err() {
          debug!("the connection is closed, no unavailable presence was sent");
        }
      }

      self.media.shutdown().await;
      if let Some(id) = self.filter_id.get() {
        self.connection.remove_stanza_filter(*id).await;
      }
    }
    // Outside the lock: a callback that leaves, too, must not wait for us while we wait for it.
    self.dispatcher.shutdown(CALLBACKS_TIMEOUT).await;
    self.release_callbacks();
  }

  /// Drops the callbacks of the application. They are called no more, and one that holds a handle on the conference
  /// (as a callback that wants to call back into it must) would keep the conference alive through the cycle. Called
  /// once the last event has been queued: the queued ones own their callbacks.
  fn release_callbacks(&self) {
    self.released.store(true, Ordering::Release);
    let on_participant = self.lock().on_participant.take();
    drop(on_participant);
    let callbacks = &self.callbacks;
    clear(&callbacks.on_participant_left);
    clear(&callbacks.on_colibri_message);
    clear(&callbacks.on_remote_stream);
    clear(&callbacks.sink_factory);
    clear(&callbacks.on_conference_left);
    clear(&callbacks.on_media_session);
    clear(&callbacks.on_source);
    // There is no way to take this one out.
    self.sync_sample_sink.set(Arc::new(|_| {}));
  }

  /// Keeps `value` as the callback of `slot`, unless the callbacks have been released.
  pub(crate) fn set_callback<T>(&self, slot: &RwLock<Option<T>>, value: T) {
    if self.released.load(Ordering::Acquire) {
      debug!("the conference is left: the callback is not kept");
      return;
    }
    store(slot, value);
  }

  /// The sync sample callback, unless the callbacks have been released.
  pub(crate) fn set_sync_sample_callback(&self, callback: Arc<dyn Fn(crate::SyncSample) + Send + Sync>) {
    if self.released.load(Ordering::Acquire) {
      return;
    }
    self.sync_sample_sink.set(callback);
  }

  /// A Jingle session is over: the focus has ended it or we are ending it. Forgets `sid` as the session the focus has
  /// with us, unless it is another one by now (a session the focus has offered since). Returns whether `sid` was the
  /// session it knew.
  pub(crate) fn session_ended(&self, sid: &str) -> bool {
    let mut state = self.lock();
    let known = state.known_sid.as_deref() == Some(sid);
    if known {
      state.known_sid = None;
    }
    known
  }
}
