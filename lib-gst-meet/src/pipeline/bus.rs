use std::{
  collections::HashMap,
  sync::{Arc, Mutex},
};

use futures::StreamExt as _;
use gstreamer::prelude::*;
use tracing::{debug, error, warn};

use super::{
  owner::{self, Owner, StreamPart},
  streams::{FailureOrigin, StreamsHandle},
  types::StreamId,
  SessionFailure,
};

pub(super) type SessionRegistry = Arc<Mutex<HashMap<u64, StreamsHandle>>>;

pub(super) struct BusHooks {
  pub(super) session_failed: Arc<dyn Fn(u64, SessionFailure) + Send + Sync>,
  pub(super) fatal: Arc<dyn Fn(String) + Send + Sync>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ErrorTarget {
  Stream {
    session: u64,
    stream_id: StreamId,
    origin: FailureOrigin,
  },
  Session {
    session: u64,
    /// The element is part of the DTLS handshake.
    dtls: bool,
  },
  /// The element has left the pipeline: it is being, or has been, torn down.
  Detached,
  /// Not an element the library owns, e.g. one of the application's consumers.
  Shared,
}

pub(super) fn classify_error(pipeline: &gstreamer::Pipeline, source: &gstreamer::Object) -> ErrorTarget {
  match owner::classify(source) {
    Some(Owner::Stream {
      session,
      stream_id,
      part,
    }) => ErrorTarget::Stream {
      session,
      stream_id,
      origin: match part {
        StreamPart::Chain(generation) => FailureOrigin::Chain(generation),
        StreamPart::Consumer => FailureOrigin::Consumer,
      },
    },
    Some(Owner::Session { session }) => ErrorTarget::Session {
      session,
      dtls: ancestry(source).iter().any(|object| {
        object
          .downcast_ref::<gstreamer::Element>()
          .and_then(|element| element.factory())
          .is_some_and(|factory| factory.name().starts_with("dtlssrtp"))
      }),
    },
    None if ancestry(source).last() == Some(pipeline.upcast_ref::<gstreamer::Object>()) => ErrorTarget::Shared,
    None => ErrorTarget::Detached,
  }
}

fn ancestry(source: &gstreamer::Object) -> Vec<gstreamer::Object> {
  let mut ancestry = vec![source.clone()];
  while let Some(parent) = ancestry.last().and_then(|object| object.parent()) {
    ancestry.push(parent);
  }
  ancestry
}

pub(super) fn spawn(
  pipeline: &gstreamer::Pipeline,
  sessions: SessionRegistry,
  hooks: BusHooks,
) -> Option<tokio::task::JoinHandle<()>> {
  let bus = pipeline.bus()?;
  let pipeline = pipeline.downgrade();
  Some(tokio::spawn(async move {
    let mut messages = bus.stream();
    while let Some(message) = messages.next().await {
      match message.view() {
        gstreamer::MessageView::Error(e) => {
          let Some(source) = message.src() else {
            continue;
          };
          let Some(pipeline) = pipeline.upgrade() else {
            break;
          };
          let text = match e.debug() {
            Some(debug) => format!("{}: {} ({})", source.name(), e.error(), debug),
            None => format!("{}: {}", source.name(), e.error()),
          };
          error!("GStreamer error from {}", text);
          match classify_error(&pipeline, source) {
            ErrorTarget::Stream {
              session,
              stream_id,
              origin,
            } => {
              let handle = sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&session)
                .cloned();
              match handle {
                Some(handle) => handle.stream_failed(stream_id, origin, text),
                None => debug!(
                  "ignoring an error of stream {} of the ended session {}",
                  stream_id, session
                ),
              }
            },
            ErrorTarget::Session { session, dtls } => {
              if sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&session)
              {
                (hooks.session_failed)(
                  session,
                  if dtls {
                    SessionFailure::Dtls(text)
                  }
                  else {
                    SessionFailure::Transport(text)
                  },
                );
              }
              else {
                debug!("ignoring an error of the ended session {}", session);
              }
            },
            ErrorTarget::Detached => debug!("ignoring an error of {}: it is no longer in the pipeline", text),
            ErrorTarget::Shared => {
              let pipeline_for_null = pipeline.clone();
              let _ = tokio::task::spawn_blocking(move || pipeline_for_null.set_state(gstreamer::State::Null)).await;
              (hooks.fatal)(text);
              break;
            },
          }
        },
        gstreamer::MessageView::Warning(w) => {
          warn!(
            "GStreamer warning from {}: {} ({:?})",
            message.src().map(|s| s.name().to_string()).unwrap_or_default(),
            w.error(),
            w.debug()
          );
        },
        gstreamer::MessageView::Latency(_) => {
          if let Some(pipeline) = pipeline.upgrade() {
            if let Err(e) = pipeline.recalculate_latency() {
              warn!("recalculate_latency failed: {:?}", e);
            }
          }
        },
        _ => {},
      }
    }
  }))
}
