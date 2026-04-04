use anyhow::{anyhow, Context, Result};
use glib::prelude::Cast as _;
use gstreamer::{
  prelude::{
    ElementExt as _, ElementExtManual as _, GObjectExtManualGst as _, GstBinExt as _, GstObjectExt as _,
    ObjectExt as _, PadExt as _,
  },
  Bin, GhostPad,
};
use tracing::{debug, info, warn};

use crate::source::MediaType;

use super::{codec::Codec, decode::connect_request_extension};

pub(super) fn build_decode_bin(
  codecs: &[Codec],
  media_type: MediaType,
  pt: u8,
  ssrc: u32,
  recv_video_width: u16,
  recv_video_height: u16,
) -> Result<Bin> {
  let decode_bin = Bin::new();
  decode_bin.set_property("name", format!("decode_{}", ssrc));

  let codec = codecs
    .iter()
    .filter(|codec| {
      if media_type.is_audio() {
        codec.is_audio()
      }
      else {
        codec.is_video()
      }
    })
    .find(|codec| codec.is(pt))
    .context(format!("received {:?} with unsupported PT {}", media_type, pt))?;

  let depayloader = gstreamer::ElementFactory::make(codec.depayloader_name()).build()?;

  depayloader.set_property("auto-header-extension", false);
  connect_request_extension(&depayloader, "depayloader");

  decode_bin
    .add(&depayloader)
    .context("failed to add depayloader to decode bin")?;

  let pre_decoder_queue = gstreamer::ElementFactory::make("queue").build()?;
  decode_bin
    .add(&pre_decoder_queue)
    .context("failed to add queue to decode bin")?;
  depayloader
    .link(&pre_decoder_queue)
    .context("failed to link depayloader to queue")?;

  let decoder = gstreamer::ElementFactory::make(codec.decoder_name()).build()?;
  if media_type.is_video() {
    decoder.set_property("automatic-request-sync-points", true);
    decoder.set_property_from_str("automatic-request-sync-point-flags", "corrupt-output");
    decoder.set_property("min-force-key-unit-interval", 2_000_000_000u64);
  }

  decode_bin
    .add(&decoder)
    .context("failed to add decoder to decode bin")?;
  pre_decoder_queue
    .link(&decoder)
    .context("failed to link queue to decoder")?;

  let post_decoder_queue = {
    let mut q = gstreamer::ElementFactory::make("queue");
    if media_type.is_video() {
      q = q
        .property("max-size-buffers", 2u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .property_from_str("leaky", "downstream");
    }
    q.build()?
  };
  decode_bin
    .add(&post_decoder_queue)
    .context("failed to add queue to decode bin")?;
  decoder
    .link(&post_decoder_queue)
    .context("failed to link decoder to queue")?;

  let last_element = if media_type.is_audio() {
    post_decoder_queue.clone()
  }
  else {
    let videoscale = gstreamer::ElementFactory::make("videoscale").build()?;
    decode_bin
      .add(&videoscale)
      .context("failed to add videoscale to decode bin")?;
    post_decoder_queue
      .link(&videoscale)
      .context("failed to link queue to videoscale")?;

    let capsfilter = gstreamer::ElementFactory::make("capsfilter").build()?;
    capsfilter.set_property_from_str(
      "caps",
      &format!(
        "video/x-raw, width=(int)[1,{}], height=(int)[1,{}], pixel-aspect-ratio=1/1",
        recv_video_width, recv_video_height
      ),
    );
    decode_bin
      .add(&capsfilter)
      .context("failed to add capsfilter to decode bin")?;
    videoscale
      .link(&capsfilter)
      .context("failed to link videoscale to capsfilter")?;

    let videoconvert = gstreamer::ElementFactory::make("videoconvert").build()?;
    decode_bin
      .add(&videoconvert)
      .context("failed to add videoconvert to decode bin")?;
    capsfilter
      .link(&videoconvert)
      .context("failed to link capsfilter to videoconvert")?;

    let post_videoconvert_queue = gstreamer::ElementFactory::make("queue")
      .property("max-size-buffers", 2u32)
      .property("max-size-bytes", 0u32)
      .property("max-size-time", 0u64)
      .property_from_str("leaky", "downstream")
      .build()?;
    decode_bin
      .add(&post_videoconvert_queue)
      .context("failed to add queue to decode bin")?;
    videoconvert
      .link(&post_videoconvert_queue)
      .context("failed to link videoconvert to queue")?;

    post_videoconvert_queue
  };

  let depay_sink = depayloader
    .static_pad("sink")
    .context("depayloader has no sink pad")?;
  let bin_sink_pad = GhostPad::builder_with_target(&depay_sink)?.name("sink").build();
  decode_bin.add_pad(&bin_sink_pad)?;

  let last_src = last_element
    .static_pad("src")
    .context("last element has no src pad")?;
  let bin_src_pad = GhostPad::builder_with_target(&last_src)?.name("src").build();
  decode_bin.add_pad(&bin_src_pad)?;

  Ok(decode_bin)
}

/// Link `decode_bin`'s src ghost pad to `sink_element` which lives inside a sub-bin.
/// Creates a named ghost pad on the sub-bin so both the decode bin and the sub-bin
/// are siblings at the pipeline level — satisfying GStreamer's hierarchy check.
///
/// Returns `(ghost_pad_name, ext_bin, request_pad)` where `request_pad` is the
/// request pad that was allocated on `sink_element` and must be released on teardown.
pub(super) fn link_decode_bin_via_ghost_pad(
  decode_bin: &Bin,
  media_type: MediaType,
  participant_id: &str,
  sink_element: &gstreamer::Element,
) -> Result<(String, Bin, gstreamer::Pad)> {
  let bin_src = decode_bin
    .static_pad("src")
    .context("decode bin has no src ghost pad")?;

  let sink_pad = sink_element
    .request_pad_simple("sink_%u")
    .context("sink element has no requestable sink pads")?;

  let result: Result<(String, Bin, gstreamer::Pad)> = (|| {
    let gp_name = format!("participant_{}_{:?}", participant_id, media_type);
    let ext_bin: Bin = sink_element
      .parent()
      .context("sink element has no parent")?
      .downcast()
      .map_err(|_| anyhow!("sink element's parent is not a Bin"))?;

    if let Some(old_pad) = ext_bin.static_pad(&gp_name) {
      if let Some(peer) = old_pad.peer() {
        let _ = peer.unlink(&old_pad);
      }
      if let Err(e) = old_pad.set_active(false) {
        warn!("failed to deactivate ghost pad {}: {:?}", gp_name, e);
      }
      if let Err(e) = ext_bin.remove_pad(&old_pad) {
        warn!("failed to remove ghost pad {}: {:?}", gp_name, e);
      }
    }

    let ghost_pad = GhostPad::builder_with_target(&sink_pad)?
      .name(&gp_name)
      .build();
    ext_bin.add_pad(&ghost_pad)?;

    bin_src
      .link(&ghost_pad)
      .map_err(|e| anyhow!("failed to link decode bin to global sink: {:?}", e))?;

    info!(
      "linked {}/{:?} via ghost pad to global sink",
      participant_id, media_type
    );

    Ok((gp_name, ext_bin, sink_pad.clone()))
  })();

  match result {
    Ok(v) => Ok(v),
    Err(e) => {
      sink_element.release_request_pad(&sink_pad);
      Err(e)
    },
  }
}

/// Link `decode_bin`'s src ghost pad to the appropriate named sink pad on
/// `participant_bin`.  Returns `Ok(())` if a matching sink pad was found and linked,
/// `Ok(())` (with a warning) if no matching pad exists on the bin.
pub(super) fn link_decode_bin_to_participant_bin(
  decode_bin: &Bin,
  media_type: MediaType,
  participant_id: &str,
  participant_bin: &Bin,
) -> Result<()> {
  let bin_src = decode_bin
    .static_pad("src")
    .context("decode bin has no src ghost pad")?;

  let sink_pad_name = match media_type {
    MediaType::Audio => "audio",
    MediaType::Video => "video",
    MediaType::AudioScreenshare => "audioscreenshare",
    MediaType::VideoScreenshare => "videoscreenshare",
  };

  if let Some(sink_pad) = participant_bin.static_pad(sink_pad_name) {
    if let Some(old_peer) = sink_pad.peer() {
      old_peer.unlink(&sink_pad)?;
    }
    bin_src
      .link(&sink_pad)
      .context("failed to link decode bin to participant bin")?;
    info!(
      "linked {}/{:?} to participant bin",
      participant_id, media_type
    );
  }
  else {
    warn!(
      "no {} sink pad on {} participant bin",
      sink_pad_name, participant_id
    );
  }

  Ok(())
}