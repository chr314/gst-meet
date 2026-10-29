use std::{sync::Mutex, time::Duration};

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Bin, ClockTime, PadProbeId, PadProbeReturn, PadProbeType};
use tokio::sync::oneshot;
use tracing::{debug, warn};

use super::{
  owner::{self, Owner},
  types::StreamId,
};
use crate::source::MediaType;

/// How long we block waiting for an element's state transition to settle. `set_state(Null)` can return `Async`;
/// an element that is dropped while its children are still playing makes GStreamer log critical warnings.
const STATE_WAIT: ClockTime = ClockTime::from_mseconds(500);

pub(super) enum Consumer {
  /// A parentless element from the sink factory, added to the pipeline for this stream alone (e.g. an appsink).
  PerStream(gstreamer::Element),
  /// A request pad of an element inside a bin that already exists (e.g. an audiomixer), reached through a ghost
  /// pad on that bin, which is a sibling of the chain at the pipeline level.
  GhostPad {
    element: gstreamer::Element,
    request_pad: gstreamer::Pad,
    bin: Bin,
    ghost: gstreamer::GhostPad,
  },
  /// A static pad of the application's `participant_<id>` bin.
  ParticipantBin(gstreamer::Pad),
  /// Only the stream events are wanted: no chain exists at all.
  Events,
}

pub(super) struct ConsumerRequest<'a> {
  pub(super) pipeline: &'a gstreamer::Pipeline,
  pub(super) stream_id: StreamId,
  pub(super) participant_id: &'a str,
  pub(super) media_type: MediaType,
  /// What the sink factory returned.
  pub(super) element: Option<gstreamer::Element>,
  pub(super) has_stream_handler: bool,
  pub(super) owner: Owner,
}

impl Consumer {
  /// `None` when there is nothing to consume the stream: no element, no participant bin and no stream handler.
  pub(super) fn resolve(request: ConsumerRequest) -> Result<Option<Self>> {
    let ConsumerRequest {
      pipeline,
      stream_id,
      participant_id,
      media_type,
      element,
      has_stream_handler,
      owner,
    } = request;

    if let Some(element) = element {
      let Some(parent) = element.parent() else {
        owner::mark(&element, owner);
        pipeline
          .add(&element)
          .context("failed to add the consumer to the pipeline")?;
        return Ok(Some(Self::PerStream(element)));
      };
      let bin: Bin = parent
        .downcast()
        .map_err(|_| anyhow!("the parent of the sink element is not a bin"))?;
      let request_pad = element
        .request_pad_simple("sink_%u")
        .context("the sink element has no requestable sink pads")?;
      let ghost = (|| -> Result<gstreamer::GhostPad> {
        let ghost = gstreamer::GhostPad::builder_with_target(&request_pad)?
          .name(format!("stream_{stream_id}"))
          .build();
        bin.add_pad(&ghost)?;
        Ok(ghost)
      })();
      let ghost = match ghost {
        Ok(ghost) => ghost,
        Err(e) => {
          element.release_request_pad(&request_pad);
          return Err(e.context("failed to expose the sink pad outside its bin"));
        },
      };
      return Ok(Some(Self::GhostPad {
        element,
        request_pad,
        bin,
        ghost,
      }));
    }

    let participant_bin = pipeline
      .by_name(&format!("participant_{participant_id}"))
      .and_then(|element| element.downcast::<Bin>().ok());
    if let Some(participant_bin) = participant_bin {
      let pad_name = match media_type {
        MediaType::Audio => "audio",
        MediaType::Video => "video",
        MediaType::AudioScreenshare => "audioscreenshare",
        MediaType::VideoScreenshare => "videoscreenshare",
      };
      match participant_bin.static_pad(pad_name) {
        Some(pad) => return Ok(Some(Self::ParticipantBin(pad))),
        None => warn!("no {} sink pad on the bin of participant {}", pad_name, participant_id),
      }
    }

    Ok(has_stream_handler.then_some(Self::Events))
  }

  pub(super) fn needs_chain(&self) -> bool {
    !matches!(self, Self::Events)
  }

  pub(super) fn sink_pad(&self) -> Option<gstreamer::Pad> {
    match self {
      Self::PerStream(element) => element
        .static_pad("sink")
        .or_else(|| element.sink_pads().into_iter().next()),
      Self::GhostPad { ghost, .. } => Some(ghost.clone().upcast()),
      Self::ParticipantBin(pad) => Some(pad.clone()),
      Self::Events => None,
    }
  }

  /// The element to bring to the pipeline's state before the chain is linked to it.
  pub(super) fn per_stream_element(&self) -> Option<&gstreamer::Element> {
    match self {
      Self::PerStream(element) => Some(element),
      _ => None,
    }
  }

  /// Links `chain_src` to the consumer, replacing whatever the consumer was linked to.
  pub(super) fn link(&self, chain_src: &gstreamer::Pad) -> Result<()> {
    let Some(sink) = self.sink_pad() else {
      return Ok(());
    };
    if let Some(old_peer) = sink.peer() {
      if old_peer != *chain_src {
        old_peer
          .unlink(&sink)
          .context("failed to unlink the consumer from its previous stream")?;
      }
    }
    if chain_src.peer().as_ref() != Some(&sink) {
      chain_src
        .link(&sink)
        .map_err(|e| anyhow!("failed to link the chain to the consumer: {:?}", e))?;
    }
    Ok(())
  }

  pub(super) fn unlink(&self, chain_src: &gstreamer::Pad) {
    if let Some(sink) = self.sink_pad() {
      if sink.peer().as_ref() == Some(chain_src) {
        let _ = chain_src.unlink(&sink);
      }
    }
  }

  /// Blocks. Whoever owns the consumer's elements is responsible for them after this; a `ParticipantBin` or the
  /// bin of a `GhostPad` is left alone.
  pub(super) fn dispose(self, pipeline: &gstreamer::Pipeline) {
    match self {
      Self::PerStream(element) => remove_element(pipeline, &element),
      Self::GhostPad {
        element,
        request_pad,
        bin,
        ghost,
      } => {
        if let Err(e) = ghost.set_active(false) {
          warn!("failed to deactivate ghost pad {}: {:?}", ghost.name(), e);
        }
        if let Err(e) = bin.remove_pad(&ghost) {
          warn!("failed to remove ghost pad {}: {:?}", ghost.name(), e);
        }
        element.release_request_pad(&request_pad);
      },
      Self::ParticipantBin(_) | Self::Events => {},
    }
  }
}

/// Transitions `element` to `Null`, waits for it, then removes it from `parent`. Errors are logged.
pub(super) fn remove_element(
  parent: &impl glib::prelude::IsA<Bin>,
  element: &impl glib::prelude::IsA<gstreamer::Element>,
) {
  let element = element.as_ref();
  if let Err(e) = element.set_state(gstreamer::State::Null) {
    warn!("set_state(Null) failed for {}: {:?}", element.name(), e);
  }
  let (result, current, pending) = element.state(Some(STATE_WAIT));
  if let Err(e) = result {
    warn!(
      "{} did not reach Null: {:?} (current={:?}, pending={:?})",
      element.name(),
      e,
      current,
      pending
    );
  }
  if element.parent().is_some() {
    if let Err(e) = parent.remove(element) {
      warn!(
        "failed to remove {} from {}: {:?}",
        element.name(),
        parent.as_ref().name(),
        e
      );
    }
  }
}

/// The elements of `element` that end the data flow: itself, or the sinks inside a bin, recursively.
pub(super) fn collect_sink_elements(element: &gstreamer::Element) -> Vec<gstreamer::Element> {
  let Some(bin) = element.downcast_ref::<Bin>() else {
    return vec![element.clone()];
  };
  'resync: loop {
    let mut sinks = Vec::new();
    let mut iter = bin.iterate_sinks();
    loop {
      match iter.next() {
        Ok(Some(child)) => sinks.extend(collect_sink_elements(&child)),
        Ok(None) => return sinks,
        Err(gstreamer::IteratorError::Resync) => {
          iter.resync();
          continue 'resync;
        },
        Err(gstreamer::IteratorError::Error) => {
          warn!("failed to iterate the sinks of {}", bin.name());
          return sinks;
        },
      }
    }
  }
}

/// Resolves when EOS has arrived at every sink pad of a consumer. The probes are removed on drop.
pub(super) struct EosWatch {
  probes: Vec<(gstreamer::Pad, PadProbeId)>,
  arrivals: Vec<oneshot::Receiver<()>>,
}

impl EosWatch {
  pub(super) fn install(consumer: &gstreamer::Element) -> Self {
    let mut watch = Self {
      probes: Vec::new(),
      arrivals: Vec::new(),
    };
    for sink in collect_sink_elements(consumer) {
      for pad in sink.sink_pads() {
        let (tx, rx) = oneshot::channel();
        let tx = Mutex::new(Some(tx));
        let probe = pad.add_probe(PadProbeType::EVENT_DOWNSTREAM, move |_, info| {
          // The probe runs before the sink handles the event. By then everything before the EOS has been
          // rendered, e.g. a muxer has finished writing; the sink only closes its file on the way to Null.
          if info
            .event()
            .is_some_and(|event| event.type_() == gstreamer::EventType::Eos)
          {
            if let Some(tx) = tx.lock().ok().and_then(|mut tx| tx.take()) {
              let _ = tx.send(());
            }
          }
          PadProbeReturn::Ok
        });
        if let Some(probe) = probe {
          watch.probes.push((pad, probe));
          watch.arrivals.push(rx);
        }
      }
    }
    watch
  }

  pub(super) async fn wait(mut self, timeout: Duration) -> bool {
    let arrivals = std::mem::take(&mut self.arrivals);
    if arrivals.is_empty() {
      debug!("no sink pads to wait for EOS on");
    }
    let all_arrived = async {
      let mut all = true;
      for arrival in arrivals {
        all &= arrival.await.is_ok();
      }
      all
    };
    tokio::time::timeout(timeout, all_arrived).await.unwrap_or(false)
  }
}

impl Drop for EosWatch {
  fn drop(&mut self) {
    for (pad, probe) in self.probes.drain(..) {
      pad.remove_probe(probe);
    }
  }
}
