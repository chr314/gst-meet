use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{anyhow, Context, Result};
use glib::prelude::{Cast as _, ToValue as _};
use gstreamer::{
    prelude::{
        ElementExt as _, ElementExtManual as _, GObjectExtManualGst as _, GstBinExt as _, GstBinExtManual as _,
        GstObjectExt as _, ObjectExt as _, PadExt as _,
    },
    Bin, GhostPad,
};
use gstreamer_rtp::{prelude::RTPHeaderExtensionExt as _, RTPHeaderExtension};
use tracing::{debug, error, info, warn};

use crate::{
    conference::JitsiConference,
    source::{MediaType, Source},
};

use super::Codec;

pub(super) struct DecodeBinInfo {
    pub(super) bin: Bin,
    pub(super) participant_id: String,
    pub(super) media_type: MediaType,
    pub(super) external_ghost_pad: Option<(String, Bin)>,
}

fn parse_pad_name(name: &str) -> Option<(u32, u8)> {
    let name = name.strip_prefix("recv_rtp_src_0_")?;
    let mut parts = name.split('_');
    let ssrc: u32 = parts.next()?.parse().ok()?;
    let pt: u8 = parts.next()?.parse().ok()?;
    Some((ssrc, pt))
}

fn find_codec_for_pt<'a>(codecs: &'a [Codec], media_type: MediaType, pt: u8) -> Result<&'a Codec> {
    codecs
        .iter()
        .filter(|codec| match media_type {
            MediaType::Audio => codec.is_audio(),
            MediaType::Video => codec.is_video(),
        })
        .find(|codec| codec.is(pt))
        .context(format!("received {:?} with unsupported PT {}", media_type, pt))
}

fn build_decode_bin(
    codecs: &[Codec],
    media_type: MediaType,
    pt: u8,
    ssrc: u32,
    recv_video_width: u16,
    recv_video_height: u16,
) -> Result<Bin> {
    let decode_bin = Bin::new();
    decode_bin.set_property("name", format!("decode_{}", ssrc));

    let codec = find_codec_for_pt(codecs, media_type, pt)?;
    let depayloader = gstreamer::ElementFactory::make(codec.depayloader_name()).build()?;

    depayloader.set_property("auto-header-extension", false);
    connect_request_extension(&depayloader, "depayloader");

    decode_bin
        .add(&depayloader)
        .context("failed to add depayloader to decode bin")?;
    debug!("created depayloader");

    let pre_decoder_queue = gstreamer::ElementFactory::make("queue").build()?;
    decode_bin
        .add(&pre_decoder_queue)
        .context("failed to add queue to decode bin")?;
    depayloader
        .link(&pre_decoder_queue)
        .context("failed to link depayloader to queue")?;

    let decoder = gstreamer::ElementFactory::make(codec.decoder_name()).build()?;
    if media_type == MediaType::Video {
        decoder.set_property("automatic-request-sync-points", true);
        decoder.set_property_from_str("automatic-request-sync-point-flags", "corrupt-output");
    }

    decode_bin
        .add(&decoder)
        .context("failed to add decoder to decode bin")?;
    pre_decoder_queue
        .link(&decoder)
        .context("failed to link queue to decoder")?;

    let post_decoder_queue = gstreamer::ElementFactory::make("queue").build()?;
    decode_bin
        .add(&post_decoder_queue)
        .context("failed to add queue to decode bin")?;
    decoder
        .link(&post_decoder_queue)
        .context("failed to link decoder to queue")?;

    let last_element = match media_type {
        MediaType::Audio => post_decoder_queue.clone(),
        MediaType::Video => {
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
                &format!("video/x-raw, width={}, height={}", recv_video_width, recv_video_height),
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

            let post_videoconvert_queue = gstreamer::ElementFactory::make("queue").build()?;
            decode_bin
                .add(&post_videoconvert_queue)
                .context("failed to add queue to decode bin")?;
            videoconvert
                .link(&post_videoconvert_queue)
                .context("failed to link videoconvert to queue")?;

            post_videoconvert_queue
        }
    };

    let depay_sink = depayloader.static_pad("sink").context("depayloader has no sink pad")?;
    let bin_sink_pad = GhostPad::builder_with_target(&depay_sink)?.name("sink").build();
    decode_bin.add_pad(&bin_sink_pad)?;

    let last_src = last_element.static_pad("src").context("last element has no src pad")?;
    let bin_src_pad = GhostPad::builder_with_target(&last_src)?.name("src").build();
    decode_bin.add_pad(&bin_src_pad)?;

    Ok(decode_bin)
}

fn link_decode_bin_to_sink(
    decode_bin: &Bin,
    media_type: MediaType,
    participant_id: &str,
    sink_element: Option<gstreamer::Element>,
    participant_bin: Option<Bin>,
) -> Result<Option<(String, Bin)>> {
    let bin_src = decode_bin
        .static_pad("src")
        .context("decode bin has no src ghost pad")?;

    if let Some(sink_element) = sink_element {
        let sink_pad = sink_element
            .request_pad_simple("sink_%u")
            .context("no suitable sink pad provided by sink element in recv pipeline")?;
        let gp_name = format!("participant_{}_{:?}", participant_id, media_type);
        let ext_bin: Bin = sink_element
            .parent()
            .context("sink element has no parent")?
            .downcast()
            .map_err(|_| anyhow!("sink element's parent is not a bin"))?;

        if let Some(old_pad) = ext_bin.static_pad(&gp_name) {
            debug!("removing existing ghost pad {} before re-adding", gp_name);
            old_pad.set_active(false).ok();
            let _ = ext_bin.remove_pad(&old_pad);
        }

        let ghost_pad = GhostPad::builder_with_target(&sink_pad)?.name(&gp_name).build();
        ext_bin.add_pad(&ghost_pad)?;

        bin_src
            .link(&ghost_pad)
            .context("failed to link decode bin to participant bin from recv pipeline")?;
        info!("linked {}/{:?} to new pad in recv pipeline", participant_id, media_type);

        return Ok(Some((gp_name, ext_bin)));
    }

    if let Some(participant_bin) = participant_bin {
        let sink_pad_name = match media_type {
            MediaType::Audio => "audio",
            MediaType::Video => "video",
        };
        if let Some(sink_pad) = participant_bin.static_pad(sink_pad_name) {
            if let Some(old_peer) = sink_pad.peer() {
                debug!(
                    "unlinking existing peer from {} sink pad before re-linking",
                    sink_pad_name
                );
                old_peer.unlink(&sink_pad)?;
            }
            bin_src
                .link(&sink_pad)
                .context("failed to link decode bin to participant bin from recv participant pipeline")?;
            info!(
                "linked {}/{:?} to recv participant pipeline",
                participant_id, media_type
            );
        } else {
            warn!(
                "no {} sink pad on {} participant bin in recv participant pipeline",
                sink_pad_name, participant_id
            );
        }
    }

    Ok(None)
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
            }
        }
    });
}

pub(super) fn connect_pad_removed(
    rtpbin: &gstreamer::Element,
    pipeline: &gstreamer::Pipeline,
    decode_bins: &Arc<Mutex<HashMap<u32, DecodeBinInfo>>>,
) {
    let pipeline = pipeline.clone();
    let decode_bins = decode_bins.clone();
    rtpbin.connect("pad-removed", false, move |values| {
        let pad: gstreamer::Pad = match values[1].get() {
            Ok(p) => p,
            Err(_) => return None,
        };
        let pad_name: String = pad.property("name");
        let Some((ssrc, _pt)) = parse_pad_name(&pad_name) else {
            return None;
        };
        let existing = match decode_bins.lock() {
            Ok(mut g) => g.remove(&ssrc),
            Err(e) => {
                warn!("decode_bins lock poisoned in pad-removed: {:?}", e);
                return None;
            }
        };
        if let Some(info) = existing {
            debug!("rtpbin removed pad for ssrc {}, tearing down decode bin", ssrc);
            teardown_decode_bin(&pipeline, info, ssrc);
        }
        None
    });
}

pub(super) fn connect_pad_added(
    rtpbin: &gstreamer::Element,
    pipeline: &gstreamer::Pipeline,
    conference: &JitsiConference,
    codecs: &[Codec],
    remote_ssrc_map: &Arc<RwLock<HashMap<u32, Source>>>,
    decode_bins: &Arc<Mutex<HashMap<u32, DecodeBinInfo>>>,
) {
    let pipeline = pipeline.clone();
    let conference = conference.clone();
    let codecs = codecs.to_vec();
    let remote_ssrc_map = remote_ssrc_map.clone();
    let decode_bins = decode_bins.clone();
    let handle = tokio::runtime::Handle::current();
    rtpbin.connect("pad-added", false, move |values| {
        let pad: gstreamer::Pad = match values[1].get() {
            Ok(p) => p,
            Err(e) => {
                error!("pad-added: failed to get pad: {:?}", e);
                return None;
            }
        };
        let pad_name: String = pad.property("name");
        debug!("pad added: {}", pad_name);
        let Some((ssrc, pt)) = parse_pad_name(&pad_name) else {
            return None;
        };

        let source = match remote_ssrc_map.read() {
            Ok(g) => match g.get(&ssrc).cloned() {
                Some(s) => s,
                None => {
                    error!("pad-added: unknown ssrc {}", ssrc);
                    return None;
                }
            },
            Err(e) => {
                error!("pad-added: remote_ssrc_map lock poisoned: {:?}", e);
                return None;
            }
        };

        debug!("pad added for remote source: {:?}", source);

        let Some(participant_id) = source.participant_id.clone() else {
            return None;
        };

        let f = || -> Result<()> {
            handle.block_on(conference.ensure_participant(&participant_id))?;

            let maybe_sink_element = match source.media_type {
                MediaType::Audio => handle.block_on(conference.remote_participant_audio_sink_element()),
                MediaType::Video => handle.block_on(conference.remote_participant_video_sink_element()),
            };
            let maybe_participant_bin = pipeline.by_name(&format!("participant_{}", participant_id));

            if maybe_sink_element.is_none() && maybe_participant_bin.is_none() {
                debug!("no sink for participant {}", participant_id);
                return Ok(());
            }

            // Build the new decode bin BEFORE tearing down the old one.
            // If building fails (e.g. unsupported codec), link a fakesink so the
            // new rtpbin pad is never left dangling (which propagates NOT_LINKED to nicesrc).
            let decode_bin = match build_decode_bin(
                &codecs,
                source.media_type,
                pt,
                ssrc,
                conference.config.recv_video_scale_width,
                conference.config.recv_video_scale_height,
            ) {
                Ok(bin) => bin,
                Err(e) => {
                    warn!("cannot decode ssrc {} (pt {}): {:?} — linking fakesink", ssrc, pt, e);
                    let fakesink = gstreamer::ElementFactory::make("fakesink").build()?;
                    pipeline.add(&fakesink)?;
                    fakesink.sync_state_with_parent()?;
                    if let Some(sink_pad) = fakesink.static_pad("sink") {
                        pad.link(&sink_pad).ok();
                    }
                    return Ok(());
                }
            };

            // Teardown any existing decode bin for the same participant+media_type now
            // that we know the new bin is ready to take over.
            {
                let mut bins = decode_bins
                    .lock()
                    .map_err(|e| anyhow::anyhow!("decode_bins lock poisoned: {}", e))?;
                let old_ssrc = bins
                    .iter()
                    .find(|(_, info)| info.participant_id == participant_id && info.media_type == source.media_type)
                    .map(|(s, _)| *s);
                if let Some(old_ssrc) = old_ssrc {
                    if let Some(old_info) = bins.remove(&old_ssrc) {
                        debug!("tearing down previous decode bin for {}/{:?} (ssrc {}) before linking new one", participant_id, source.media_type, old_ssrc);
                        teardown_decode_bin(&pipeline, old_info, old_ssrc);
                    }
                }
            }

            pipeline.add(&decode_bin).context("failed to add decode bin to pipeline")?;
            decode_bin.sync_state_with_parent()?;

            let decode_sink = decode_bin
                .static_pad("sink")
                .context("decode bin has no sink pad")?;
            pad.link(&decode_sink)
                .map_err(|e| anyhow::anyhow!("failed to link rtpbin.{} to decode bin: {:?}", pad_name, e))?;

            debug!("linked rtpbin.{} to decode bin decode_{}", pad_name, ssrc);

            let maybe_participant_bin: Option<Bin> = maybe_participant_bin.and_then(|e| e.downcast().ok());
            let external_ghost_pad = link_decode_bin_to_sink(
                &decode_bin,
                source.media_type,
                &participant_id,
                maybe_sink_element,
                maybe_participant_bin,
            )?;

            {
                let mut bins = decode_bins
                    .lock()
                    .map_err(|e| anyhow::anyhow!("decode_bins lock poisoned: {}", e))?;
                if let Some(old) = bins.remove(&ssrc) {
                    warn!("ssrc {} already had a decode bin, tearing down old one", ssrc);
                    teardown_decode_bin(&pipeline, old, ssrc);
                }
                bins.insert(ssrc, DecodeBinInfo { bin: decode_bin, participant_id, media_type: source.media_type, external_ghost_pad });
            }

            pipeline.debug_to_dot_file(gstreamer::DebugGraphDetails::ALL, &format!("ssrc-added-{}", ssrc));

            if let Err(e) = pipeline.recalculate_latency() {
                warn!("recalculate_latency failed after adding decode bin: {:?}", e);
            }

            Ok(())
        };
        if let Err(e) = f() {
            error!("handling pad-added: {:?}", e);
        }

        None
    });
}

pub(super) fn teardown_decode_bin(pipeline: &gstreamer::Pipeline, info: DecodeBinInfo, ssrc: u32) {
    debug!("tearing down decode bin for ssrc {}", ssrc);

    // Unlink upstream (rtpbin's recv_rtp_src → decode bin sink) first.
    // Without this, removing the bin leaves rtpbin's pad with a dead peer,
    // causing NOT_LINKED to propagate back through the pipeline to nicesrc.
    if let Some(sink_pad) = info.bin.static_pad("sink") {
        if let Some(peer) = sink_pad.peer() {
            let _ = peer.unlink(&sink_pad);
        }
    }

    if let Some((pad_name, ext_bin)) = info.external_ghost_pad {
        if let Some(pad) = ext_bin.static_pad(&pad_name) {
            debug!("removing external ghost pad {}", pad_name);
            pad.set_active(false).ok();
            let _ = ext_bin.remove_pad(&pad);
        }
    } else if let Some(src_pad) = info.bin.static_pad("src") {
        if let Some(peer) = src_pad.peer() {
            let _ = src_pad.unlink(&peer);
        }
    }

    if let Err(e) = info.bin.set_state(gstreamer::State::Null) {
        warn!("failed to set decode bin state to Null: {:?}", e);
    }
    if let Err(e) = pipeline.remove(&info.bin) {
        warn!("failed to remove decode bin from pipeline: {:?}", e);
    }
}
