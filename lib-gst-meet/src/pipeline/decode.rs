use std::{
  collections::HashMap,
  sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context, Result};
use glib::prelude::{Cast as _, ToValue as _};
use gstreamer::{
  prelude::{
    ElementExt as _, GstBinExt as _, GstBinExtManual as _, GstObjectExt as _, ObjectExt as _, PadExt as _,
    PadExtManual as _,
  },
  Bin,
};
use gstreamer_rtp::{prelude::RTPHeaderExtensionExt as _, RTPHeaderExtension};
use tracing::{debug, error, warn};

use crate::{
  conference::{JitsiConference, StreamEvent, StreamEventKind, StreamInfo},
  source::{MediaType, Source},
};

use super::{
  decode_bin::{build_decode_bin, link_decode_bin_to_participant_bin, link_decode_bin_via_ghost_pad},
  Codec,
};

/// `rtpbin` exposes pads per `(session, ssrc, payload type)`. We need to track the
/// actual pad instance, not just the SSRC, because the same SSRC can legitimately
/// appear on multiple pads over time as payload types change.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct PadKey {
  pub(crate) session: u32,
  pub(crate) ssrc: u32,
  pub(crate) pt: u8,
}

/// State machine for a single `rtpbin` receive pad instance.
#[derive(Debug)]
pub(crate) enum PadState {
  /// This pad is currently absorbed by a fakesink. That can mean:
  /// - the RTP pad arrived before `source-add`;
  /// - the SSRC belongs to the bridge (no participant);
  /// - there is no consumer for the decoded stream yet;
  /// - the codec is unsupported locally.
  Parked {
    rtpbin_pad: gstreamer::Pad,
    participant_id: Option<String>,
    media_type: Option<MediaType>,
    fakesink: gstreamer::Element,
  },

  /// A decode bin is live and linked downstream.
  Active {
    decode_bin: Bin,
    participant_id: String,
    media_type: MediaType,
    consumer: StreamConsumer,
    /// Attached when another pad instance of the same (participant, media_type)
    /// takes over the consumer slot. Drains decoded frames until pad-removed fires.
    parked_fakesink: Option<gstreamer::Element>,
  },
}

/// Describes how a decode bin's src pad is wired downstream.
/// Stored in `PadState::Active` so `cleanup_pad` can perform correct teardown
/// for each consumer type.
#[derive(Debug)]
pub(crate) enum StreamConsumer {
  /// Linked via a named ghost pad on a sub-bin (e.g. audiomixer inside an audio recv bin).
  /// Teardown: deactivate + remove ghost pad from `ext_bin`, release `request_pad`.
  GhostPad {
    pad_name: String,
    ext_bin: gstreamer::Bin,
    request_pad: gstreamer::Pad,
  },
  /// Per-stream element added directly to the pipeline (e.g. appsink).
  /// Teardown: set to Null, remove from pipeline.
  PerStream(gstreamer::Element),
  /// Fallback fakesink — no real consumer registered but `on_remote_stream` is set.
  /// Teardown: set to Null, remove from pipeline.
  Fallback(gstreamer::Element),
  /// Linked directly to a named static pad on a participant bin.
  /// Teardown: just unlink (the bin manages its own lifetime).
  ParticipantBin,
}

fn parse_pad_name(name: &str) -> Option<PadKey> {
  let name = name.strip_prefix("recv_rtp_src_")?;
  let mut parts = name.split('_');
  Some(PadKey {
    session: parts.next()?.parse().ok()?,
    ssrc: parts.next()?.parse().ok()?,
    pt: parts.next()?.parse().ok()?,
  })
}

pub(super) fn make_fakesink(pipeline: &gstreamer::Pipeline, pad: &gstreamer::Pad) -> Result<gstreamer::Element> {
  let fakesink = gstreamer::ElementFactory::make("fakesink")
    .property("sync", false)
    .build()?;
  pipeline.add(&fakesink).context("failed to add fakesink to pipeline")?;
  fakesink.sync_state_with_parent()?;
  let sink_pad = fakesink.static_pad("sink").context("fakesink has no sink pad")?;
  pad
    .link(&sink_pad)
    .map_err(|e| anyhow!("failed to link pad to fakesink: {:?}", e))?;
  Ok(fakesink)
}

pub(super) fn park_pad(
  pipeline: &gstreamer::Pipeline,
  pad: &gstreamer::Pad,
  participant_id: Option<String>,
  media_type: Option<MediaType>,
) -> Result<PadState> {
  let fakesink = make_fakesink(pipeline, pad)?;
  Ok(PadState::Parked {
    rtpbin_pad: pad.clone(),
    participant_id,
    media_type,
    fakesink,
  })
}

/// Attach a `sync=false` fakesink to the decode bin's src pad (after unlinking any
/// existing downstream peer) so that decoded frames are silently discarded while the
/// bin waits to be torn down.
fn attach_parked_fakesink(pipeline: &gstreamer::Pipeline, decode_bin: &Bin) -> Result<gstreamer::Element> {
  let src_pad = decode_bin
    .static_pad("src")
    .context("decode bin has no src ghost pad")?;
  if let Some(peer) = src_pad.peer() {
    let _ = src_pad.unlink(&peer);
  }
  let fakesink = gstreamer::ElementFactory::make("fakesink")
    .property("sync", false)
    .build()?;
  pipeline
    .add(&fakesink)
    .context("failed to add parked fakesink to pipeline")?;
  fakesink.sync_state_with_parent()?;
  let sink_pad = fakesink.static_pad("sink").context("parked fakesink has no sink pad")?;
  src_pad
    .link(&sink_pad)
    .map_err(|e| anyhow!("failed to link decode bin src to parked fakesink: {:?}", e))?;
  Ok(fakesink)
}

/// If there is already an `Active` entry for `(participant_id, media_type)` other than
/// `new_pad`, attach a parked fakesink to it so decoded frames are absorbed until the
/// old bin is torn down. Must be called while holding the registry lock.
pub(super) fn park_old_active(
  registry: &mut HashMap<PadKey, PadState>,
  pipeline: &gstreamer::Pipeline,
  participant_id: &str,
  media_type: MediaType,
  new_pad: PadKey,
) {
  let old_pad = registry
    .iter()
    .find(|(pad_key, state)| {
      **pad_key != new_pad
        && matches!(state,
          PadState::Active { participant_id: pid, media_type: mt, parked_fakesink: None, .. }
          if pid == participant_id && *mt == media_type
        )
    })
    .map(|(pad_key, _)| *pad_key);

  if let Some(old_pad) = old_pad {
    if let Some(PadState::Active {
      decode_bin,
      parked_fakesink,
      ..
    }) = registry.get_mut(&old_pad)
    {
      debug!(
        "parking old active pad {:?} (same {:?} as new pad {:?})",
        old_pad, media_type, new_pad
      );
      match attach_parked_fakesink(pipeline, decode_bin) {
        Ok(sink) => *parked_fakesink = Some(sink),
        Err(e) => warn!("failed to park old active pad {:?}: {:?}", old_pad, e),
      }
    }
  }
}

pub(super) fn connect_request_extension(element: &gstreamer::Element, description: &str) {
  let desc = description.to_owned();
  element.connect("request-extension", false, move |values| {
    let f = || {
      let ext_id: u32 = values[1].get()?;
      let ext_uri: String = values[2].get()?;
      debug!("{} requested extension: {} {}", desc, ext_id, ext_uri);
      let hdrext = RTPHeaderExtension::create_from_uri(&ext_uri).context("failed to create hdrext")?;
      hdrext.set_id(ext_id);
      Ok::<_, anyhow::Error>(hdrext)
    };
    match f() {
      Ok(hdrext) => Some(hdrext.to_value()),
      Err(e) => {
        warn!("request-extension: {:?}", e);
        None
      },
    }
  });
}

fn teardown_decode_bin(pipeline: &gstreamer::Pipeline, decode_bin: &Bin, consumer: &StreamConsumer, ssrc: u32) {
  debug!("tearing down decode bin for ssrc {}", ssrc);

  if let Some(sink_pad) = decode_bin.static_pad("sink") {
    if let Some(peer) = sink_pad.peer() {
      if let Err(e) = peer.unlink(&sink_pad) {
        warn!("teardown ssrc {}: failed to unlink upstream: {:?}", ssrc, e);
      }
    }
  }

  if let Some(src_pad) = decode_bin.static_pad("src") {
    if let Some(peer) = src_pad.peer() {
      if let Err(e) = src_pad.unlink(&peer) {
        warn!("teardown ssrc {}: failed to unlink downstream: {:?}", ssrc, e);
      }
    }
  }

  match consumer {
    StreamConsumer::GhostPad {
      pad_name,
      ext_bin,
      request_pad,
    } => {
      if let Some(pad) = ext_bin.static_pad(pad_name) {
        if let Err(e) = pad.set_active(false) {
          warn!("teardown ssrc {}: failed to deactivate ghost pad: {:?}", ssrc, e);
        }
        if let Err(e) = ext_bin.remove_pad(&pad) {
          warn!("teardown ssrc {}: failed to remove ghost pad: {:?}", ssrc, e);
        }
      }
      if let Some(element) = request_pad.parent_element() {
        element.release_request_pad(request_pad);
      }
    },
    StreamConsumer::PerStream(el) | StreamConsumer::Fallback(el) => {
      if let Err(e) = el.set_state(gstreamer::State::Null) {
        warn!("teardown ssrc {}: failed to set consumer element Null: {:?}", ssrc, e);
      }
      if let Err(e) = pipeline.remove(el) {
        warn!("teardown ssrc {}: failed to remove consumer element: {:?}", ssrc, e);
      }
    },
    StreamConsumer::ParticipantBin => {
      // Upstream and downstream already unlinked above. The participant bin
      // manages its own lifetime via remove_participant().
    },
  }

  if let Err(e) = decode_bin.set_state(gstreamer::State::Null) {
    warn!("failed to set decode bin state to Null: {:?}", e);
  }
  if let Err(e) = pipeline.remove(decode_bin) {
    warn!("failed to remove decode bin from pipeline: {:?}", e);
  }
}

pub(super) fn cleanup_pad(pipeline: &gstreamer::Pipeline, state: PadState, pad_key: PadKey) {
  match state {
    PadState::Parked {
      rtpbin_pad, fakesink, ..
    } => {
      debug!("cleaning up parked pad {:?}", pad_key);
      if let Some(sink_pad) = fakesink.static_pad("sink") {
        if rtpbin_pad.peer().as_ref() == Some(&sink_pad) {
          let _ = rtpbin_pad.unlink(&sink_pad);
        }
      }
      let _ = fakesink.set_state(gstreamer::State::Null);
      let _ = pipeline.remove(&fakesink);
    },
    PadState::Active {
      decode_bin,
      consumer,
      parked_fakesink,
      ..
    } => {
      teardown_decode_bin(pipeline, &decode_bin, &consumer, pad_key.ssrc);

      if let Some(parked) = parked_fakesink {
        let _ = parked.set_state(gstreamer::State::Null);
        let _ = pipeline.remove(&parked);
      }
    },
  }
}

pub(super) async fn activate_pad(
  pipeline: &gstreamer::Pipeline,
  conference: &JitsiConference,
  codecs: &[Codec],
  pad_key: PadKey,
  media_type: MediaType,
  participant_id: &str,
  rtpbin_pad: &gstreamer::Pad,
) -> Result<PadState> {
  conference.ensure_participant(participant_id).await?;

  let has_stream_handler = conference.has_stream_handler().await;
  let maybe_participant_bin = pipeline.by_name(&format!("participant_{}", participant_id));
  let factory_element = conference
    .call_sink_factory(StreamInfo {
      participant_id: participant_id.to_owned(),
      ssrc: pad_key.ssrc,
      media_type,
    })
    .await;

  if factory_element.is_none() && maybe_participant_bin.is_none() && !has_stream_handler {
    debug!(
      "no consumer for {}/{:?} on pad {:?} — linking fakesink",
      participant_id, media_type, pad_key
    );
    return park_pad(pipeline, rtpbin_pad, Some(participant_id.to_owned()), Some(media_type));
  }

  let decode_bin = match build_decode_bin(
    codecs,
    media_type,
    pad_key.pt,
    pad_key.ssrc,
    conference.config.recv_video_scale_width,
    conference.config.recv_video_scale_height,
  ) {
    Ok(bin) => bin,
    Err(e) => {
      warn!(
        "cannot decode ssrc {} (pt {:?} {:?}): {:?} — linking fakesink",
        pad_key.ssrc, pad_key.pt, media_type, e
      );
      return park_pad(pipeline, rtpbin_pad, Some(participant_id.to_owned()), Some(media_type));
    },
  };

  pipeline
    .add(&decode_bin)
    .context("failed to add decode bin to pipeline")?;

  let result: Result<StreamConsumer> = (|| {
    let decode_sink = decode_bin
      .static_pad("sink")
      .context("decode bin has no sink ghost pad")?;
    let decode_src = decode_bin
      .static_pad("src")
      .context("decode bin has no src ghost pad")?;

    let consumer = if let Some(ref el) = factory_element {
      if el.parent().is_some() {
        let (pad_name, ext_bin, request_pad) =
          link_decode_bin_via_ghost_pad(&decode_bin, media_type, participant_id, el)?;
        StreamConsumer::GhostPad {
          pad_name,
          ext_bin,
          request_pad,
        }
      } else {
        pipeline.add(el).context("failed to add per-stream sink to pipeline")?;
        el.sync_state_with_parent()?;
        let sink_pad = el
          .static_pad("sink")
          .context("per-stream sink element has no sink pad")?;
        decode_src
          .link(&sink_pad)
          .map_err(|e| anyhow!("failed to link decode bin to per-stream sink: {:?}", e))?;
        StreamConsumer::PerStream(el.clone())
      }
    } else {
      let maybe_participant_bin: Option<Bin> = maybe_participant_bin.and_then(|e| e.downcast().ok());
      if let Some(participant_bin) = maybe_participant_bin {
        link_decode_bin_to_participant_bin(&decode_bin, media_type, participant_id, &participant_bin)?;
        StreamConsumer::ParticipantBin
      } else {
        let fakesink = gstreamer::ElementFactory::make("fakesink")
          .property("sync", false)
          .build()
          .context("failed to create fallback fakesink")?;
        pipeline
          .add(&fakesink)
          .context("failed to add fallback fakesink to pipeline")?;
        fakesink.sync_state_with_parent()?;
        let sink_pad = fakesink
          .static_pad("sink")
          .context("fallback fakesink has no sink pad")?;
        decode_src
          .link(&sink_pad)
          .map_err(|e| anyhow!("failed to link decode bin to fallback fakesink: {:?}", e))?;
        StreamConsumer::Fallback(fakesink)
      }
    };

    decode_bin.sync_state_with_parent()?;

    rtpbin_pad
      .link(&decode_sink)
      .map_err(|e| anyhow!("failed to link rtpbin pad to decode bin: {:?}", e))?;

    Ok(consumer)
  })();

  let consumer = match result {
    Ok(c) => c,
    Err(e) => {
      let _ = decode_bin.set_state(gstreamer::State::Null);
      let _ = pipeline.remove(&decode_bin);
      return Err(e);
    },
  };

  debug!(
    "activated decode bin decode_{} for {}/{:?} on pad {:?} — upstream and downstream linked",
    pad_key.ssrc, participant_id, media_type, pad_key
  );

  conference
    .fire_stream_event(StreamEvent {
      participant_id: participant_id.to_owned(),
      ssrc: pad_key.ssrc,
      media_type,
      kind: StreamEventKind::Added,
    })
    .await;

  pipeline.debug_to_dot_file(
    gstreamer::DebugGraphDetails::ALL,
    &format!("pad-added-{}-{}", pad_key.ssrc, pad_key.pt),
  );

  if let Err(e) = pipeline.recalculate_latency() {
    warn!("recalculate_latency failed after adding decode bin: {:?}", e);
  }

  if media_type.is_video() {
    let fku = gstreamer::Structure::builder("GstForceKeyUnit")
      .field("running-time", gstreamer::ClockTime::NONE)
      .field("all-headers", true)
      .field("count", 0u32)
      .build();
    let event = gstreamer::event::CustomUpstream::builder(fku).build();
    if let Some(sink) = decode_bin.static_pad("sink") {
      if !sink.push_event(event) {
        warn!("force-key-unit event not handled for pad {:?}", pad_key);
      }
    }
  }

  Ok(PadState::Active {
    decode_bin,
    participant_id: participant_id.to_owned(),
    media_type,
    consumer,
    parked_fakesink: None,
  })
}

pub(super) fn connect_pad_added(
  rtpbin: &gstreamer::Element,
  pipeline: &gstreamer::Pipeline,
  conference: &JitsiConference,
  codecs: &[Codec],
  source_registry: &Arc<Mutex<HashMap<u32, Source>>>,
  pad_registry: &Arc<Mutex<HashMap<PadKey, PadState>>>,
) {
  let pipeline = pipeline.clone();
  let conference = conference.clone();
  let codecs = codecs.to_vec();
  let source_registry = source_registry.clone();
  let pad_registry = pad_registry.clone();
  let handle = tokio::runtime::Handle::current();
  rtpbin.connect("pad-added", false, move |values| {
    let pad: gstreamer::Pad = match values[1].get() {
      Ok(p) => p,
      Err(e) => {
        error!("pad-added: failed to get pad: {:?}", e);
        return None;
      },
    };
    let pad_name: String = pad.property("name");
    debug!("pad added: {}", pad_name);
    let Some(pad_key) = parse_pad_name(&pad_name) else {
      return None;
    };

    let f = || -> Result<()> {
      let replaced = {
        let mut registry = pad_registry
          .lock()
          .map_err(|e| anyhow!("pad_registry lock poisoned: {}", e))?;
        registry.remove(&pad_key)
      }; // registry lock dropped
      if let Some(old_state) = replaced {
        warn!("pad-added replacing existing state for pad {:?}", pad_key);
        cleanup_pad(&pipeline, old_state, pad_key);
      }

      let source = source_registry
        .lock()
        .map_err(|e| anyhow!("source_registry lock poisoned: {}", e))?
        .get(&pad_key.ssrc)
        .cloned();

      let new_state = match source {
        None => {
          debug!("pad-added for unknown pad {:?} — parking", pad_key);
          park_pad(&pipeline, &pad, None, None)?
        },
        Some(Source {
          participant_id: None,
          media_type,
          ..
        }) => {
          debug!("pad-added for bridge pad {:?} — parking", pad_key);
          park_pad(&pipeline, &pad, None, Some(media_type))?
        },
        Some(Source {
          participant_id: Some(participant_id),
          media_type,
          ..
        }) => {
          let state = match handle.block_on(activate_pad(
            &pipeline,
            &conference,
            &codecs,
            pad_key,
            media_type,
            &participant_id,
            &pad,
          )) {
            Ok(state) => state,
            Err(e) => {
              error!("pad-added: failed to activate pad {:?}: {:?} — parking", pad_key, e);
              match park_pad(&pipeline, &pad, Some(participant_id.clone()), Some(media_type)) {
                Ok(state) => state,
                Err(e2) => {
                  error!("pad-added: cannot park pad {:?}: {:?}", pad_key, e2);
                  return Ok(());
                },
              }
            },
          };

          if matches!(state, PadState::Active { .. }) {
            let mut registry = pad_registry
              .lock()
              .map_err(|e| anyhow!("pad_registry lock poisoned: {}", e))?;
            park_old_active(&mut registry, &pipeline, &participant_id, media_type, pad_key);
          }

          state
        },
      };

      let mut registry = pad_registry
        .lock()
        .map_err(|e| anyhow!("pad_registry lock poisoned: {}", e))?;
      registry.insert(pad_key, new_state);

      Ok(())
    };

    if let Err(e) = f() {
      error!("handling pad-added for pad {:?}: {:?}", pad_key, e);
    }

    None
  });
}

pub(super) fn connect_pad_removed(
  rtpbin: &gstreamer::Element,
  pipeline: &gstreamer::Pipeline,
  conference: &JitsiConference,
  pad_registry: &Arc<Mutex<HashMap<PadKey, PadState>>>,
) {
  let pipeline = pipeline.clone();
  let conference = conference.clone();
  let pad_registry = pad_registry.clone();
  let handle = tokio::runtime::Handle::current();
  rtpbin.connect("pad-removed", false, move |values| {
    let pad: gstreamer::Pad = match values[1].get() {
      Ok(p) => p,
      Err(_) => return None,
    };
    let pad_name: String = pad.property("name");
    let Some(pad_key) = parse_pad_name(&pad_name) else {
      return None;
    };

    let state = match pad_registry.lock() {
      Ok(mut g) => g.remove(&pad_key),
      Err(e) => {
        warn!("pad_registry lock poisoned in pad-removed: {:?}", e);
        return None;
      },
    };

    if let Some(state) = state {
      debug!("rtpbin removed pad {:?}, cleaning up state", pad_key);

      if let PadState::Active {
        ref participant_id,
        media_type,
        ..
      } = state
      {
        let conference = conference.clone();
        let event = StreamEvent {
          participant_id: participant_id.clone(),
          ssrc: pad_key.ssrc,
          media_type,
          kind: StreamEventKind::Removed,
        };
        handle.spawn(async move {
          conference.fire_stream_event(event).await;
        });
      }

      cleanup_pad(&pipeline, state, pad_key);
    }

    None
  });
}
