use std::fmt;

use crate::{pipeline::ConnectionState, source::RemoteSource};

/// Why [`Connection::connect`](crate::Connection::connect) failed, or why a connection ended before it was set up. It
/// is the source of the `anyhow::Error` that `connect` returns: use `downcast_ref::<ConnectionError>()` to find out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionError {
  /// The server refused the authentication (a SASL failure), for instance because the token has expired. The
  /// condition is the one the server names, such as `not-authorized`.
  AuthenticationFailed { condition: String, text: Option<String> },
  /// The server reported an error on the stream, such as `host-unknown`, and closed it.
  StreamError { condition: String, text: Option<String> },
  BindFailed(String),
  DiscoveryFailed(String),
  /// The connection ended: the socket failed, or the server closed it.
  Closed(String),
  /// The server sent something that makes no sense in the stage the setup was in.
  Protocol(String),
  Timeout,
}

impl fmt::Display for ConnectionError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fn with_text(f: &mut fmt::Formatter<'_>, what: &str, condition: &str, text: &Option<String>) -> fmt::Result {
      match text {
        Some(text) => write!(f, "{what} ({condition}): {text}"),
        None => write!(f, "{what} ({condition})"),
      }
    }
    match self {
      ConnectionError::AuthenticationFailed { condition, text } => {
        with_text(f, "the server refused the authentication", condition, text)
      },
      ConnectionError::StreamError { condition, text } => {
        with_text(f, "the server reported a stream error", condition, text)
      },
      ConnectionError::BindFailed(reason) => write!(f, "the server did not bind a resource: {reason}"),
      ConnectionError::DiscoveryFailed(reason) => {
        write!(f, "the server did not answer the service discovery: {reason}")
      },
      ConnectionError::Closed(reason) => write!(f, "the XMPP connection ended: {reason}"),
      ConnectionError::Protocol(reason) => write!(f, "unexpected XMPP stanza while connecting: {reason}"),
      ConnectionError::Timeout => f.write_str("timed out setting up the XMPP connection"),
    }
  }
}

impl std::error::Error for ConnectionError {}

/// Why [`JitsiConference::join`](crate::JitsiConference::join) failed. It is the source of the `anyhow::Error` the
/// join returns: use `downcast_ref::<JoinError>()` to find out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinError {
  /// The deployment only lets authenticated users create a conference or join it.
  AuthenticationRequired,
  /// The server or the focus refused the user, with the reason it gave, if any.
  NotAuthorized(Option<String>),
  PasswordRequired,
  /// The room is for members only. Behind a lobby, `lobby_room` is the lobby's JID. `waiting_for_host`: the room
  /// does not exist yet and only its host can open it.
  MembersOnly {
    lobby_room: Option<String>,
    waiting_for_host: bool,
  },
  Banned,
  RoomFull,
  NicknameConflict,
  /// The room does not exist and the server does not let this user create it. The focus normally does that on the
  /// first request, so this is a race with a focus that has just left; the join retries it a few times.
  RoomCreationRestricted,
  TimeLimitReached,
  DisplayNameRequired,
  /// The server does not allow this user to join, for the reason given: `no-main-participants`,
  /// `promotion-not-allowed`, `no-visitors-lobby`, or the text of the error.
  NotAllowed(String),
  /// The server refused the join for a reason of its own: the error condition.
  RoomJoinError(String),
  /// The focus (Jicofo) refused to open the conference.
  FocusError { condition: String, text: Option<String> },
  /// The focus answered our session-accept with an error.
  SessionRejected(String),
  SessionFailed(String),
  /// The conference was not joined within [`join_timeout`](crate::JitsiConferenceConfig::join_timeout).
  Timeout,
  ConnectionClosed,
}

impl fmt::Display for JoinError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      JoinError::AuthenticationRequired => f.write_str("authentication is required to join this conference"),
      JoinError::NotAuthorized(Some(reason)) => write!(f, "not authorized to join this conference: {reason}"),
      JoinError::NotAuthorized(None) => f.write_str("not authorized to join this conference"),
      JoinError::PasswordRequired => f.write_str("the room requires a password"),
      JoinError::MembersOnly {
        lobby_room: Some(lobby),
        ..
      } => write!(f, "the room is for members only; its lobby is {lobby}"),
      JoinError::MembersOnly {
        waiting_for_host: true,
        ..
      } => f.write_str("the room is for members only and waits for its host"),
      JoinError::MembersOnly { .. } => f.write_str("the room is for members only"),
      JoinError::Banned => f.write_str("banned from the room"),
      JoinError::RoomFull => f.write_str("the room is full"),
      JoinError::NicknameConflict => f.write_str("the nickname is in use in the room"),
      JoinError::RoomCreationRestricted => f.write_str("room creation is restricted"),
      JoinError::TimeLimitReached => f.write_str("the conference has reached its time limit"),
      JoinError::DisplayNameRequired => f.write_str("the room requires a display name"),
      JoinError::NotAllowed(reason) => write!(f, "not allowed to join the room: {reason}"),
      JoinError::RoomJoinError(condition) => write!(f, "the room refused the join ({condition})"),
      JoinError::FocusError {
        condition,
        text: Some(text),
      } => write!(f, "the focus refused the conference ({condition}): {text}"),
      JoinError::FocusError { condition, text: None } => write!(f, "the focus refused the conference ({condition})"),
      JoinError::SessionRejected(reason) => write!(f, "the focus rejected the media session: {reason}"),
      JoinError::SessionFailed(reason) => write!(f, "the media session failed to start: {reason}"),
      JoinError::Timeout => f.write_str("timed out joining the conference"),
      JoinError::ConnectionClosed => f.write_str("the XMPP connection closed while joining the conference"),
    }
  }
}

impl std::error::Error for JoinError {}

/// Why a conference that was joined is over. See [`on_conference_left`](crate::JitsiConference::on_conference_left).
/// The media sessions are shut down by then; call [`leave`](crate::JitsiConference::leave) to release the rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConferenceLeftReason {
  Left,
  Kicked { reason: Option<String> },
  RoomDestroyed { reason: Option<String> },
  ConnectionLost,
  /// The focus did not offer a new media session in time after the last one ended.
  NoMediaSession,
  /// Something that cannot be recovered from happened: a pipeline error, a security failure, too many restarts.
  Fatal(String),
}

/// What happens to the Jingle session with the focus. After the first `Started`, the conference stays joined while
/// sessions come and go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaSessionEvent {
  /// The focus has accepted our session. `bridge_session_id` is Jicofo's name for the allocation on the bridge.
  Started { bridge_session_id: Option<String> },
  /// The session is gone and its streams are retired. `restarting`: a new session is expected (we asked for one, or
  /// the focus is moving us); if it does not come in time the conference is left with
  /// [`ConferenceLeftReason::NoMediaSession`].
  Ended { reason: String, restarting: bool },
  ConnectionStateChanged(ConnectionState),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceEvent {
  Added(RemoteSource),
  Removed(RemoteSource),
}
