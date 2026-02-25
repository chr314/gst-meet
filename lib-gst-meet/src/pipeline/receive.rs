use anyhow::{anyhow, bail, Context};
use glib::prelude::{Cast as _, ToValue as _};
use gstreamer::{
  prelude::{
    ElementExt as _, ElementExtManual as _, GObjectExtManualGst as _, GstBinExt as _,
    GstBinExtManual as _, GstObjectExt as _, ObjectExt as _, PadExt as _,
  },
  Bin, GhostPad,
};
use gstreamer_rtp::{prelude::RTPHeaderExtensionExt as _, RTPHeaderExtension};
use tokio::runtime::Handle;
use tracing::{debug, error, info, warn};

use crate::{
  conference::JitsiConference,
  source::MediaType,
};

use super::Codec;

pub(super) struct DecodeBinInfo {
  pub(super) bin: Bin,
  pub(super) participant_id: String,
  pub(super) external_ghost_pad: Option<(String, Bin)>,
}

pub(super) fn connect_pad_added(
  rtpbin: &gstreamer::Element,
  pipeline: &gstreamer::Pipeline,
  conference: &JitsiConference,
  codecs: Vec<Codec>,
) {
  let handle = Handle::current();
  let conference = conference.clone();
  let pipeline = pipeline.clone();
  let rtpbin_ = rtpbin.clone();
  rtpbin.connect("pad-added", false, move |values| {
    let rtpbin = &rtpbin_;
    let f = || {
      debug!("rtpbin pad-added {:?}", values);
      let pad: gstreamer::Pad = values[1].get()?;
      let pad_name: String = pad.property("name");
      if pad_name.starts_with("recv_rtp_src_0_") {
        let mut parts = pad_name.split('_').skip(4);
        let ssrc: u32 = parts.next().context("malformed pad name")?.parse()?;
        let pt: u8 = parts.next().context("malformed pad name")?.parse()?;
        let source = handle.block_on(async {
          Ok::<_, anyhow::Error>(
            conference
              .jingle_session
              .lock()
              .await
              .as_ref()
              .context("not connected (no jingle session)")?
              .media_pipeline
              .remote_ssrc_map
              .get(&ssrc)
              .context(format!("unknown ssrc: {}", ssrc))?
              .clone(),
          )
        })?;

        debug!("pad added for remote source: {:?}", source);

        let (maybe_participant_id, maybe_sink_element, maybe_participant_bin) =
          if let Some(participant_id) = source.participant_id {
            handle.block_on(conference.ensure_participant(&participant_id))?;
            let maybe_sink_element = match source.media_type {
              MediaType::Audio => {
                handle.block_on(conference.remote_participant_audio_sink_element())
              },
              MediaType::Video => {
                handle.block_on(conference.remote_participant_video_sink_element())
              },
            };
            let maybe_participant_bin =
              pipeline.by_name(&format!("participant_{}", participant_id));
            (
              Some(participant_id),
              maybe_sink_element,
              maybe_participant_bin,
            )
          }
          else {
            debug!("source is owned by JVB");
            (None, None, None)
          };

        if maybe_participant_id.is_none()
          || (maybe_sink_element.is_none() && maybe_participant_bin.is_none())
        {
          debug!("nothing to link to decoder, adding fakesink");
          let fakesink = gstreamer::ElementFactory::make("fakesink").build()?;
          pipeline.add(&fakesink)?;
          fakesink.sync_state_with_parent()?;
          let sink_pad = fakesink
            .static_pad("sink")
            .context("fakesink has no sink pad")?;
          pad.link(&sink_pad)?;

          // Track JVB fakesink for cleanup
          if maybe_participant_id.is_none() {
            handle.block_on(async {
              if let Some(session) = conference.jingle_session.lock().await.as_mut() {
                session.media_pipeline.jvb_fakesinks.push(fakesink);
              }
            });
          }

          return Ok(());
        }

        let participant_id = maybe_participant_id.context("expected participant_id")?;

        // Create a per-SSRC bin for the decode chain
        let decode_bin = Bin::new();
        decode_bin.set_property("name", format!("decode_{}", ssrc));

        let depayloader = match source.media_type {
          MediaType::Audio => {
            let codec = codecs
              .iter()
              .filter(|codec| codec.is_audio())
              .find(|codec| codec.is(pt));
            if let Some(codec) = codec {
              gstreamer::ElementFactory::make(codec.depayloader_name()).build()?
            }
            else {
              bail!("received audio with unsupported PT {}", pt);
            }
          },
          MediaType::Video => {
            let codec = codecs
              .iter()
              .filter(|codec| codec.is_video())
              .find(|codec| codec.is(pt));
            if let Some(codec) = codec {
              gstreamer::ElementFactory::make(codec.depayloader_name()).build()?
            }
            else {
              bail!("received video with unsupported PT {}", pt);
            }
          },
        };

        depayloader.set_property("auto-header-extension", false);
        depayloader.connect("request-extension", false, move |values| {
          let f = || {
            let ext_id: u32 = values[1].get()?;
            let ext_uri: String = values[2].get()?;
            debug!("depayloader requested extension: {} {}", ext_id, ext_uri);
            let hdrext = RTPHeaderExtension::create_from_uri(&ext_uri)
              .context("failed to create hdrext")?;
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

        let decoder = match source.media_type {
          MediaType::Audio => {
            let codec = codecs
              .iter()
              .filter(|codec| codec.is_audio())
              .find(|codec| codec.is(pt));
            if let Some(codec) = codec {
              gstreamer::ElementFactory::make(codec.decoder_name()).build()?
            }
            else {
              bail!("received audio with unsupported PT {}", pt);
            }
          },
          MediaType::Video => {
            let codec = codecs
              .iter()
              .filter(|codec| codec.is_video())
              .find(|codec| codec.is(pt));
            if let Some(codec) = codec {
              let decoder = gstreamer::ElementFactory::make(codec.decoder_name()).build()?;
              decoder.set_property("automatic-request-sync-points", true);
              decoder.set_property_from_str(
                "automatic-request-sync-point-flags",
                "GST_VIDEO_DECODER_REQUEST_SYNC_POINT_CORRUPT_OUTPUT",
              );
              decoder
            }
            else {
              bail!("received video with unsupported PT {}", pt);
            }
          },
        };

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

        let last_element = match source.media_type {
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
              &format!(
                "video/x-raw, width={}, height={}",
                conference.config.recv_video_scale_width,
                conference.config.recv_video_scale_height
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

            let post_videoconvert_queue = gstreamer::ElementFactory::make("queue").build()?;
            decode_bin
              .add(&post_videoconvert_queue)
              .context("failed to add queue to decode bin")?;
            videoconvert
              .link(&post_videoconvert_queue)
              .context("failed to link videoconvert to queue")?;

            post_videoconvert_queue
          },
        };

        // Add ghost pads on the decode bin
        let depay_sink = depayloader
          .static_pad("sink")
          .context("depayloader has no sink pad")?;
        let bin_sink_pad = GhostPad::builder_with_target(&depay_sink)?
          .name("sink")
          .build();
        decode_bin.add_pad(&bin_sink_pad)?;

        let last_src = last_element
          .static_pad("src")
          .context("last element has no src pad")?;
        let bin_src_pad = GhostPad::builder_with_target(&last_src)?
          .name("src")
          .build();
        decode_bin.add_pad(&bin_src_pad)?;

        // Add the decode bin to the pipeline and sync state
        pipeline
          .add(&decode_bin)
          .context("failed to add decode bin to pipeline")?;
        decode_bin.sync_state_with_parent()?;

        // Link rtpbin pad -> decode bin sink ghost pad
        rtpbin
          .link_pads(Some(&pad_name), &decode_bin, Some("sink"))
          .context(format!("failed to link rtpbin.{} to decode bin", pad_name))?;
        debug!("linked rtpbin.{} to decode bin decode_{}", pad_name, ssrc);

        // Link decode bin src ghost pad -> participant sink/recv pipeline
        let bin_src = decode_bin
          .static_pad("src")
          .context("decode bin has no src ghost pad")?;

        let mut external_ghost_pad: Option<(String, Bin)> = None;

        if let Some(sink_element) = maybe_sink_element {
          let sink_pad = sink_element
            .request_pad_simple("sink_%u")
            .context("no suitable sink pad provided by sink element in recv pipeline")?;
          let gp_name = format!(
            "participant_{}_{:?}",
            participant_id, source.media_type
          );
          let ext_bin: Bin = sink_element
            .parent()
            .context("sink element has no parent")?
            .downcast()
            .map_err(|_| anyhow!("sink element's parent is not a bin"))?;

          if let Some(old_pad) = ext_bin.static_pad(&gp_name) {
            debug!(
              "removing existing ghost pad {} before re-adding",
              gp_name
            );
            old_pad.set_active(false).ok();
            let _ = ext_bin.remove_pad(&old_pad);
          }

          let ghost_pad = GhostPad::builder_with_target(&sink_pad)?
            .name(&gp_name)
            .build();
          ext_bin.add_pad(&ghost_pad)?;

          bin_src
            .link(&ghost_pad)
            .context("failed to link decode bin to participant bin from recv pipeline")?;
          info!(
            "linked {}/{:?} to new pad in recv pipeline",
            participant_id, source.media_type
          );

          external_ghost_pad = Some((gp_name, ext_bin));
        }
        else if let Some(participant_bin) = maybe_participant_bin {
          let sink_pad_name = match source.media_type {
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
            bin_src.link(&sink_pad).context(
              "failed to link decode bin to participant bin from recv participant pipeline",
            )?;
            info!(
              "linked {}/{:?} to recv participant pipeline",
              participant_id, source.media_type
            );
          }
          else {
            warn!(
              "no {} sink pad on {} participant bin in recv participant pipeline",
              sink_pad_name, participant_id
            );
          }
        }

        // Store the decode bin info keyed by SSRC
        {
          let participant_id_clone = participant_id.clone();
          let decode_bin_clone = decode_bin.clone();
          handle.block_on(async {
            if let Some(session) = conference.jingle_session.lock().await.as_mut() {
              session.media_pipeline.decode_bins.insert(
                ssrc,
                DecodeBinInfo {
                  bin: decode_bin_clone,
                  participant_id: participant_id_clone,
                  external_ghost_pad,
                },
              );
            }
          });
        }

        pipeline.debug_to_dot_file(
          gstreamer::DebugGraphDetails::ALL,
          &format!("ssrc-added-{}", ssrc),
        );

        Ok::<_, anyhow::Error>(())
      }
      else {
        Ok(())
      }
    };
    if let Err(e) = f() {
      error!("handling pad-added: {:?}", e);
    }
    None
  });
}

pub(super) fn teardown_decode_bin(
  pipeline: &gstreamer::Pipeline,
  info: DecodeBinInfo,
  ssrc: u32,
) {
  debug!("tearing down decode bin for ssrc {}", ssrc);

  if let Some((pad_name, ext_bin)) = info.external_ghost_pad {
    if let Some(pad) = ext_bin.static_pad(&pad_name) {
      debug!("removing external ghost pad {}", pad_name);
      pad.set_active(false).ok();
      let _ = ext_bin.remove_pad(&pad);
    }
  }

  if let Err(e) = info.bin.set_state(gstreamer::State::Null) {
    warn!("failed to set decode bin state to Null: {:?}", e);
  }
  if let Err(e) = pipeline.remove(&info.bin) {
    warn!("failed to remove decode bin from pipeline: {:?}", e);
  }
}
