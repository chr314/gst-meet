// `async_trait` puts `#[must_use]` on the futures of the methods it expands.
#![allow(clippy::double_must_use)]

mod backend;
mod colibri;
mod conference;
mod disco;
mod dispatcher;
mod error;
mod focus;
mod jingle;
mod jingle_actor;
mod muc;
mod pinger;
mod pipeline;
mod presence;
mod receiver;
mod shared;
mod signaling;
mod source;
mod sources;
mod stanza_filter;
mod stats;
mod tls;
mod util;
mod xmpp;

#[cfg(test)]
mod tests;

pub use xmpp_parsers;

pub use crate::{
  conference::{BoxedResultFuture, JitsiConference, JitsiConferenceConfig, Participant, ReceiveMode},
  error::{ConferenceLeftReason, ConnectionError, JoinError, MediaSessionEvent, SourceEvent},
  pipeline::{ConnectionState, StreamEvent, StreamEventKind, StreamId, StreamInfo, SyncSample},
  receiver::VideoReceivePolicy,
  source::{MediaType, RemoteSource},
  stanza_filter::{StanzaFilter, StanzaFilterId},
  xmpp::connection::{Authentication, Connection},
};

#[cfg(feature = "tracing-subscriber")]
pub fn init_tracing(level: tracing::Level) {
  tracing_subscriber::fmt()
    .with_max_level(level)
    .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
    .with_target(false)
    .init();
}
