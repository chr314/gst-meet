use std::{
  collections::HashMap,
  fmt,
  ops::ControlFlow,
  sync::{Arc, Mutex},
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
mod decode_bin;
mod ice;
mod send;
mod transport;

pub(crate) use build::PipelineBuildConfig;
pub(crate) use codec::{Codec, CodecName, ParsedRtpDescription};
pub(crate) use ice::participant_id_for_owner;

use decode::SsrcState;

pub(crate) struct MediaPipeline {
  pipeline: gstreamer::Pipeline,
  audio_sink_element: gstreamer::Element,
  video_sink_element: gstreamer::Element,
  ssrc_registry: Arc<Mutex<HashMap<u32, SsrcState>>>,
  conference: JitsiConference,
  codecs: Vec<Codec>,
  ice_agent: nice::Agent,
  ice_stream_id: u32,
  ice_component_id: u32,
  pipeline_state_null_rx: oneshot::Receiver<()>,
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

  pub(crate) fn remote_ssrc_snapshot(&self) -> HashMap<u32, Source> {
    match self.ssrc_registry.lock() {
      Ok(g) => g
        .iter()
        .filter_map(|(ssrc, state)| match state {
          SsrcState::Signaled { participant_id, media_type } => Some((
            *ssrc,
            Source {
              ssrc: *ssrc,
              participant_id: participant_id.clone(),
              media_type: *media_type,
            },
          )),
          SsrcState::Active { participant_id, media_type, .. } => Some((
            *ssrc,
            Source {
              ssrc: *ssrc,
              participant_id: Some(participant_id.clone()),
              media_type: *media_type,
            },
          )),
          SsrcState::Pending { .. } => None,
        })
        .collect(),
      Err(_) => HashMap::new(),
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

    let initial_registry: HashMap<u32, SsrcState> = remote_ssrc_map
      .into_iter()
      .map(|(ssrc, source)| {
        (ssrc, SsrcState::Signaled {
          participant_id: source.participant_id,
          media_type: source.media_type,
        })
      })
      .collect();
    let ssrc_registry = Arc::new(Mutex::new(initial_registry));

    build::connect_request_pt_map(
      &rtpbin,
      codecs.clone(),
      audio_hdrext_ssrc_audio_level,
      audio_hdrext_transport_cc,
      video_hdrext_transport_cc,
    );
    build::connect_new_jitterbuffer(&rtpbin, &ssrc_registry, conference.config.buffer_size);
    build::connect_request_aux(&rtpbin, pts, video_ssrc, video_rtx_ssrc);

    decode::connect_pad_added(&rtpbin, &pipeline, conference, &codecs, &ssrc_registry);
    decode::connect_pad_removed(&rtpbin, &pipeline, conference, &ssrc_registry);

    let (audio_sink_element, video_sink_element, rtpfunnel) =
      send::build_send_path(&pipeline, &codecs, audio_ssrc, video_ssrc, conference)?;

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
      ssrc_registry,
      conference: conference.clone(),
      codecs,
      ice_agent,
      ice_stream_id,
      ice_component_id,
      pipeline_state_null_rx,
    })
  }

  pub(crate) async fn source_add(&self, jingle: &Jingle) -> Result<()> {
    for content in &jingle.contents {
      if let Some(Description::Rtp(description)) = &content.description {
        for ssrc in &description.ssrcs {
          let owner = ssrc.info.as_ref().context("missing ssrc-info")?.owner.clone();
          let participant_id = participant_id_for_owner(owner)?;
          let media_type = MediaType::classify(&description.media, ssrc.video_type.as_deref());
          let ssrc_id = ssrc.id;

          debug!(
            "source-add: ssrc {} participant_id={:?} media_type={:?}",
            ssrc_id, participant_id, media_type
          );

          let upgrade = {
            let mut registry = self
              .ssrc_registry
              .lock()
              .map_err(|e| anyhow::anyhow!("ssrc_registry lock poisoned: {}", e))?;

            match registry.get(&ssrc_id) {
              Some(SsrcState::Pending { .. }) if participant_id.is_some() => {
                let state = registry.remove(&ssrc_id).unwrap();
                if let SsrcState::Pending { rtpbin_pad, pt, fakesink } = state {
                  Some((rtpbin_pad, pt, fakesink))
                }
                else {
                  unreachable!()
                }
              },
              _ => {
                registry.insert(ssrc_id, SsrcState::Signaled { participant_id: participant_id.clone(), media_type });
                None
              },
            }
          }; // registry lock dropped

          if let (Some(pid), Some((rtpbin_pad, pt, old_fakesink))) = (participant_id, upgrade) {
            if let Some(sink_pad) = old_fakesink.static_pad("sink") {
              let _ = rtpbin_pad.unlink(&sink_pad);
            }
            let _ = old_fakesink.set_state(gstreamer::State::Null);
            let _ = self.pipeline.remove(&old_fakesink);

            {
              let mut registry = self
                .ssrc_registry
                .lock()
                .map_err(|e| anyhow::anyhow!("ssrc_registry lock poisoned: {}", e))?;
              decode::park_old_active(&mut registry, &self.pipeline, &pid, media_type, ssrc_id);
            }

            let new_state = match decode::activate_ssrc(
              &self.pipeline,
              &self.conference,
              &self.codecs,
              ssrc_id,
              pt,
              media_type,
              &pid,
              &rtpbin_pad,
            )
            .await
            {
              Ok(state) => state,
              Err(e) => {
                warn!("source-add: failed to activate ssrc {}: {:?} — re-parking as Pending", ssrc_id, e);
                match decode::make_fakesink(&self.pipeline, &rtpbin_pad) {
                  Ok(fakesink) => decode::SsrcState::Pending { rtpbin_pad: rtpbin_pad.clone(), pt, fakesink },
                  Err(e2) => {
                    warn!("source-add: cannot create fallback fakesink for ssrc {}: {:?}", ssrc_id, e2);
                    continue;
                  },
                }
              },
            };

            let mut registry = self
              .ssrc_registry
              .lock()
              .map_err(|e| anyhow::anyhow!("ssrc_registry lock poisoned: {}", e))?;
            registry.insert(ssrc_id, new_state);
          }
        }
      }
    }
    Ok(())
  }

  pub(crate) fn source_remove(&self, jingle: &Jingle) -> Result<()> {
    for content in &jingle.contents {
      if let Some(Description::Rtp(description)) = &content.description {
        for ssrc in &description.ssrcs {
          debug!("source-remove: ssrc {}", ssrc.id);
          let state = self
            .ssrc_registry
            .lock()
            .map_err(|e| anyhow::anyhow!("ssrc_registry lock poisoned: {}", e))?
            .remove(&ssrc.id);
          if let Some(state) = state {
            if let SsrcState::Active {
              ref participant_id,
              media_type,
              ..
            } = state
            {
              let conference = self.conference.clone();
              let event = crate::conference::StreamEvent {
                participant_id: participant_id.clone(),
                ssrc: ssrc.id,
                media_type,
                kind: crate::conference::StreamEventKind::Removed,
              };
              tokio::spawn(async move {
                conference.fire_stream_event(event).await;
              });
            }
            decode::cleanup_ssrc(&self.pipeline, state, ssrc.id);
          }
        }
      }
    }
    Ok(())
  }

  pub(crate) fn remove_participant(&self, participant_id: &str) {
    let to_remove: Vec<(u32, SsrcState)> = match self.ssrc_registry.lock() {
      Ok(mut g) => {
        let ssrcs: Vec<u32> = g
          .iter()
          .filter(|(_, state)| match state {
            SsrcState::Signaled { participant_id: Some(pid), .. } => pid == participant_id,
            SsrcState::Active { participant_id: pid, .. } => pid == participant_id,
            _ => false,
          })
          .map(|(ssrc, _)| *ssrc)
          .collect();
        ssrcs.into_iter().filter_map(|ssrc| g.remove(&ssrc).map(|s| (ssrc, s))).collect()
      },
      Err(e) => {
        warn!("ssrc_registry lock poisoned in remove_participant: {:?}", e);
        return;
      },
    };

    for (ssrc, state) in to_remove {
      if let SsrcState::Active { participant_id: ref pid, media_type, .. } = state {
        let conference = self.conference.clone();
        let event = crate::conference::StreamEvent {
          participant_id: pid.clone(),
          ssrc,
          media_type,
          kind: crate::conference::StreamEventKind::Removed,
        };
        tokio::spawn(async move {
          conference.fire_stream_event(event).await;
        });
      }
      decode::cleanup_ssrc(&self.pipeline, state, ssrc);
    }

    if let Some(bin) = self
      .pipeline
      .by_name(&format!("participant_{}", participant_id))
    {
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
