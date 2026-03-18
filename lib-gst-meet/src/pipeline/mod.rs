use std::{
  collections::HashMap,
  fmt,
  ops::ControlFlow,
  sync::{Arc, Mutex, RwLock},
  time::Duration,
};

use anyhow::{Context, Result};
use gstreamer::prelude::{
  ElementExt as _, ElementExtManual as _, GstBinExt as _, GstBinExtManual as _, ObjectExt as _, PadExt as _,
};
use jitsi_xmpp_parsers::{
  jingle::{Description, Jingle},
  jingle_ice_udp::Transport as IceUdpTransport,
  jingle_rtp::Description as RtpDescription,
};
use nice_gst_meet as nice;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::{
  conference::JitsiConference,
  source::{MediaType, Source},
};

mod build;
mod codec;
mod decode;
mod ice;
mod send;
mod transport;

pub(crate) use build::PipelineBuildConfig;
pub(crate) use codec::{Codec, CodecName, ParsedRtpDescription};
use decode::DecodeBinInfo;
pub(crate) use ice::participant_id_for_owner;

pub(crate) struct MediaPipeline {
  pipeline: gstreamer::Pipeline,
  audio_sink_element: gstreamer::Element,
  video_sink_element: gstreamer::Element,
  pub(crate) remote_ssrc_map: Arc<RwLock<HashMap<u32, Source>>>,
  ice_agent: nice::Agent,
  ice_stream_id: u32,
  ice_component_id: u32,
  pipeline_state_null_rx: oneshot::Receiver<()>,
  decode_bins: Arc<Mutex<HashMap<u32, DecodeBinInfo>>>,
}

impl fmt::Debug for MediaPipeline {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("MediaPipeline").finish()
  }
}

impl MediaPipeline {
  pub(crate) fn pipeline(&self) -> gstreamer::Pipeline {
    self.pipeline.clone()
  }

  pub(crate) fn audio_sink_element(&self) -> gstreamer::Element {
    self.audio_sink_element.clone()
  }

  pub(crate) fn video_sink_element(&self) -> gstreamer::Element {
    self.video_sink_element.clone()
  }

  pub(crate) fn ice_local_credentials(&self) -> Option<(String, String)> {
    self.ice_agent.local_credentials(self.ice_stream_id)
  }

  pub(crate) fn ice_local_candidates(&self) -> Vec<nice::Candidate> {
    self
      .ice_agent
      .local_candidates(self.ice_stream_id, self.ice_component_id)
  }

  pub(crate) fn pause_all_sinks(&self) {
    if let Some(rtpbin) = self.pipeline.by_name("rtpbin") {
      rtpbin.foreach_src_pad(|_, pad| {
        let pad_name: String = pad.property("name");
        if pad_name.starts_with("recv_rtp_src_0_") {
          if let Some(peer_pad) = pad.peer() {
            if let Some(element) = peer_pad.parent_element() {
              element.set_state(gstreamer::State::Paused);
            }
          }
        }
        ControlFlow::Continue(())
      });
    }
  }

  pub(crate) async fn pipeline_stopped(self) -> Result<()> {
    match tokio::time::timeout(Duration::from_secs(5), self.pipeline_state_null_rx).await {
      Ok(Ok(())) => Ok(()),
      Ok(Err(e)) => Err(e.into()),
      Err(_) => {
        warn!("timed out waiting for pipeline to reach Null state");
        Ok(())
      },
    }
  }

  pub(crate) fn parse_rtp_description(
    description: &RtpDescription,
    remote_ssrc_map: &mut HashMap<u32, Source>,
  ) -> Result<Option<ParsedRtpDescription>> {
    codec::parse_rtp_description(description, remote_ssrc_map)
  }

  pub(crate) async fn build(
    conference: &JitsiConference,
    ice_transport: &IceUdpTransport,
    config: PipelineBuildConfig,
  ) -> Result<Self> {
    debug!("building gstreamer pipeline");

    let PipelineBuildConfig {
      codecs,
      audio_hdrext_ssrc_audio_level,
      audio_hdrext_transport_cc,
      video_hdrext_transport_cc,
      remote_ssrc_map,
      dtls_cert_pem,
      dtls_private_key_pem,
      audio_ssrc,
      video_ssrc,
      video_rtx_ssrc,
    } = config;

    let pipeline = gstreamer::Pipeline::new();

    let rtpbin = build::build_rtpbin(conference.config.buffer_size)?;
    pipeline.add(&rtpbin)?;

    let (ice_agent, ice_stream_id, ice_component_id) = ice::setup_ice(conference, ice_transport).await?;

    let pts: Vec<(String, u32)> = codecs
      .iter()
      .filter(|codec| codec.is_video())
      .flat_map(|codec| codec.rtx_pt.map(|rtx_pt| (codec.pt.to_string(), rtx_pt as u32)))
      .collect();

    let remote_ssrc_map = Arc::new(RwLock::new(remote_ssrc_map));
    let decode_bins = Arc::new(Mutex::new(HashMap::new()));

    build::connect_request_pt_map(
      &rtpbin,
      codecs.clone(),
      audio_hdrext_ssrc_audio_level,
      audio_hdrext_transport_cc,
      video_hdrext_transport_cc,
    );
    build::connect_new_jitterbuffer(&rtpbin, &remote_ssrc_map, conference.config.buffer_size);
    build::connect_request_aux(&rtpbin, pts, video_ssrc, video_rtx_ssrc);

    decode::connect_pad_added(&rtpbin, &pipeline, conference, &codecs, &remote_ssrc_map, &decode_bins);
    decode::connect_pad_removed(&rtpbin, &pipeline, &decode_bins);

    let (audio_sink_element, video_sink_element, rtpfunnel) =
      send::build_send_path(&pipeline, &codecs, audio_ssrc, video_ssrc, conference)?;

    debug!("linking rtpfunnel -> rtpbin");
    rtpfunnel.link_pads(None, &rtpbin, Some("send_rtp_sink_0"))?;

    let transport_bin = transport::build_transport_bin(
      &ice_agent,
      ice_stream_id,
      ice_component_id,
      &dtls_cert_pem,
      &dtls_private_key_pem,
      conference,
    )?;
    pipeline.add(&transport_bin)?;
    transport_bin.sync_state_with_parent()?;

    debug!("linking transport bin <-> rtpbin");
    transport_bin.link_pads(Some("rtp_recv_src"), &rtpbin, Some("recv_rtp_sink_0"))?;
    transport_bin.link_pads(Some("rtcp_recv_src"), &rtpbin, Some("recv_rtcp_sink_0"))?;
    rtpbin.link_pads(Some("send_rtp_src_0"), &transport_bin, Some("rtp_send_sink"))?;
    rtpbin.link_pads(Some("send_rtcp_src_0"), &transport_bin, Some("rtcp_send_sink"))?;

    let pipeline_state_null_rx = build::setup_bus_monitor(&pipeline)?;

    pipeline.debug_to_dot_file(gstreamer::DebugGraphDetails::ALL, "session-initiate");

    Ok(Self {
      pipeline,
      audio_sink_element,
      video_sink_element,
      remote_ssrc_map,
      ice_agent,
      ice_stream_id,
      ice_component_id,
      pipeline_state_null_rx,
      decode_bins,
    })
  }

  pub(crate) async fn source_add(&self, jingle: &Jingle) -> Result<()> {
    for content in &jingle.contents {
      if let Some(Description::Rtp(description)) = &content.description {
        for ssrc in &description.ssrcs {
          let owner = ssrc.info.as_ref().context("missing ssrc-info")?.owner.clone();

          debug!("adding ssrc to remote_ssrc_map: {:?}", ssrc);
          self
            .remote_ssrc_map
            .write()
            .map_err(|e| anyhow::anyhow!("remote_ssrc_map lock poisoned: {}", e))?
            .insert(
              ssrc.id,
              Source {
                ssrc: ssrc.id,
                participant_id: participant_id_for_owner(owner)?,
                media_type: if description.media == "audio" {
                  MediaType::Audio
                } else {
                  MediaType::Video
                },
              },
            );
        }
      }
    }
    Ok(())
  }

  pub(crate) fn source_remove(&self, jingle: &Jingle) -> Result<()> {
    for content in &jingle.contents {
      if let Some(Description::Rtp(description)) = &content.description {
        for ssrc in &description.ssrcs {
          debug!("removing ssrc from remote_ssrc_map: {:?}", ssrc);
          self
            .remote_ssrc_map
            .write()
            .map_err(|e| anyhow::anyhow!("remote_ssrc_map lock poisoned: {}", e))?
            .remove(&ssrc.id);
          if let Some(info) = self
            .decode_bins
            .lock()
            .map_err(|e| anyhow::anyhow!("decode_bins lock poisoned: {}", e))?
            .remove(&ssrc.id)
          {
            decode::teardown_decode_bin(&self.pipeline, info, ssrc.id);
          }
        }
      }
    }
    Ok(())
  }

  pub(crate) fn remove_participant(&self, participant_id: &str) {
    match self.remote_ssrc_map.write() {
      Ok(mut g) => g.retain(|_, source| source.participant_id.as_deref() != Some(participant_id)),
      Err(e) => {
        warn!("remote_ssrc_map lock poisoned in remove_participant: {:?}", e);
        return;
      },
    }

    let mut decode_bins = match self.decode_bins.lock() {
      Ok(g) => g,
      Err(e) => {
        warn!("decode_bins lock poisoned in remove_participant: {:?}", e);
        return;
      },
    };
    let to_remove: Vec<u32> = decode_bins
      .iter()
      .filter(|(_, info)| info.participant_id == participant_id)
      .map(|(ssrc, _)| *ssrc)
      .collect();
    for ssrc in to_remove {
      if let Some(info) = decode_bins.remove(&ssrc) {
        decode::teardown_decode_bin(&self.pipeline, info, ssrc);
      }
    }
    drop(decode_bins);

    if let Some(bin) = self.pipeline.by_name(&format!("participant_{}", participant_id)) {
      debug!("removing participant bin for {}", participant_id);
      if let Err(e) = bin.set_state(gstreamer::State::Null) {
        warn!("failed to set participant bin state to Null: {:?}", e);
      }
      if let Err(e) = self.pipeline.remove(&bin) {
        warn!("failed to remove participant bin: {:?}", e);
      }
    }

    self.pipeline.debug_to_dot_file(
      gstreamer::DebugGraphDetails::ALL,
      &format!("participant-removed-{}", participant_id),
    );
  }
}
