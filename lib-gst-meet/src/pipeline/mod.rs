use std::{collections::HashMap, fmt, ops::ControlFlow};

use anyhow::{Context, Result};
use glib::prelude::ToValue as _;
use gstreamer::prelude::{
  ElementExt as _, ElementExtManual as _, GstBinExt as _, GstBinExtManual as _, ObjectExt as _,
  PadExt as _,
};
use jitsi_xmpp_parsers::{
  jingle::{Description, Jingle},
  jingle_ice_udp::Transport as IceUdpTransport,
  jingle_rtp::Description as RtpDescription,
};
use nice_gst_meet as nice;
use tokio::{runtime::Handle, sync::oneshot};
use tracing::{debug, error, warn};

use crate::{
  conference::JitsiConference,
  source::{MediaType, Source},
};

mod codec;
mod ice;
mod receive;
mod send;
mod transport;

pub(crate) use codec::{Codec, CodecName, ParsedRtpDescription};
pub(crate) use ice::participant_id_for_owner;
use codec::{RTP_HDREXT_SSRC_AUDIO_LEVEL, RTP_HDREXT_TRANSPORT_CC};
use receive::DecodeBinInfo;

pub(crate) struct MediaPipeline {
  pipeline: gstreamer::Pipeline,
  audio_sink_element: gstreamer::Element,
  video_sink_element: gstreamer::Element,
  pub(crate) remote_ssrc_map: HashMap<u32, Source>,
  ice_agent: nice::Agent,
  ice_stream_id: u32,
  ice_component_id: u32,
  pipeline_state_null_rx: oneshot::Receiver<()>,
  /// Per-SSRC decode bins for cleanup.
  decode_bins: HashMap<u32, DecodeBinInfo>,
  /// Fakesinks added for JVB-owned sources.
  jvb_fakesinks: Vec<gstreamer::Element>,
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
    self.ice_agent.local_candidates(self.ice_stream_id, self.ice_component_id)
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
    Ok(self.pipeline_state_null_rx.await?)
  }

  pub(crate) fn parse_rtp_description(
    description: &RtpDescription,
    remote_ssrc_map: &mut HashMap<u32, Source>,
  ) -> Result<Option<ParsedRtpDescription>> {
    codec::parse_rtp_description(description, remote_ssrc_map)
  }

  /// Creates and configures the `rtpbin` element.
  fn build_rtpbin() -> Result<gstreamer::Element> {
    Ok(
      gstreamer::ElementFactory::make("rtpbin")
        .name("rtpbin")
        .property_from_str("rtp-profile", "savpf")
        .property("autoremove", true)
        .property("do-lost", true)
        .property("do-sync-event", true)
        .build()?,
    )
  }

  /// Connects the `request-pt-map` signal handler that maps payload types to GStreamer Caps.
  fn connect_request_pt_map(
    rtpbin: &gstreamer::Element,
    codecs: Vec<Codec>,
    audio_hdrext_ssrc_audio_level: Option<u16>,
    audio_hdrext_transport_cc: Option<u16>,
    video_hdrext_transport_cc: Option<u16>,
  ) {
    rtpbin.connect("request-pt-map", false, move |values| {
      let f = || {
        debug!("rtpbin request-pt-map {:?}", values);
        let pt = values[2].get::<u32>()? as u8;
        let mut caps = gstreamer::Caps::builder("application/x-rtp").field("payload", pt as i32);
        for codec in codecs.iter() {
          if codec.is(pt) {
            if codec.is_audio() {
              caps = caps
                .field("media", "audio")
                .field("encoding-name", "OPUS")
                .field("clock-rate", 48000);
              if let Some(hdrext) = audio_hdrext_ssrc_audio_level {
                caps = caps.field(&format!("extmap-{}", hdrext), RTP_HDREXT_SSRC_AUDIO_LEVEL);
              }
              if let Some(hdrext) = audio_hdrext_transport_cc {
                caps = caps.field(&format!("extmap-{}", hdrext), RTP_HDREXT_TRANSPORT_CC);
              }
            }
            else {
              caps = caps
                .field("media", "video")
                .field("clock-rate", 90000)
                .field("encoding-name", codec.encoding_name())
                .field("rtcp-fb-nack-pli", true);
              if let Some(hdrext) = video_hdrext_transport_cc {
                caps = caps.field(&format!("extmap-{}", hdrext), RTP_HDREXT_TRANSPORT_CC);
              }
            }
            return Ok::<_, anyhow::Error>(Some(caps.build()));
          }
          else if codec.is_rtx(pt) {
            caps = caps
              .field("media", "video")
              .field("clock-rate", 90000)
              .field("encoding-name", "RTX")
              .field("apt", codec.pt);
            return Ok(Some(caps.build()));
          }
        }

        warn!("unknown payload type: {}", pt);
        Ok(None)
      };
      match f() {
        Ok(Some(caps)) => {
          debug!("mapped pt to caps: {:?}", caps);
          Some(caps.to_value())
        },
        Ok(None) => None,
        Err(e) => {
          error!("handling request-pt-map: {:?}", e);
          None
        },
      }
    });
  }

  /// Connects the `new-jitterbuffer` signal that enables retransmission and configures latency.
  fn connect_new_jitterbuffer(rtpbin: &gstreamer::Element, conference: &JitsiConference) {
    let handle = Handle::current();
    let jingle_session = conference.jingle_session.clone();
    let buffer_size = conference.config.buffer_size;
    rtpbin.connect("new-jitterbuffer", false, move |values| {
      let handle = handle.clone();
      let jingle_session = jingle_session.clone();
      let f = move || {
        let rtpjitterbuffer: gstreamer::Element = values[1].get()?;
        let session: u32 = values[2].get()?;
        let ssrc: u32 = values[3].get()?;
        debug!(
          "new jitterbuffer created for session {} ssrc {}",
          session, ssrc
        );

        let source = handle.block_on(async move {
          Ok::<_, anyhow::Error>(
            jingle_session
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
        debug!("jitterbuffer is for remote source: {:?}", source);
        if source.media_type == MediaType::Video && source.participant_id.is_some() {
          debug!("enabling RTX for ssrc {}", ssrc);
          rtpjitterbuffer.set_property("do-retransmission", true);
          rtpjitterbuffer.set_property("drop-on-latency", true);
          rtpjitterbuffer.set_property("latency", buffer_size);
        }
        Ok::<_, anyhow::Error>(())
      };
      if let Err(e) = f() {
        warn!("new-jitterbuffer: {:?}", e);
      }
      None
    });
  }

  /// Connects both `request-aux-sender` and `request-aux-receiver` signal handlers.
  fn connect_request_aux(
    rtpbin: &gstreamer::Element,
    pts: Vec<(String, u32)>,
    video_ssrc: u32,
    video_rtx_ssrc: u32,
  ) {
    {
      let pts = pts.clone();
      rtpbin.connect("request-aux-sender", false, move |values| {
        let f = || {
          let session: u32 = values[1].get()?;
          debug!("creating RTX sender for session {}", session);
          let mut pt_map = gstreamer::Structure::builder("application/x-rtp-pt-map");
          let mut ssrc_map = gstreamer::Structure::builder("application/x-rtp-ssrc-map");
          for (pt, rtx_pt) in pts.iter() {
            pt_map = pt_map.field(pt, rtx_pt);
          }
          ssrc_map = ssrc_map.field(&video_ssrc.to_string(), &(video_rtx_ssrc as u32));
          let bin = gstreamer::Bin::new();
          let rtx_sender = gstreamer::ElementFactory::make("rtprtxsend")
            .property("payload-type-map", pt_map.build())
            .property("ssrc-map", ssrc_map.build())
            .build()?;
          bin.add(&rtx_sender)?;
          bin.add_pad(
            &gstreamer::GhostPad::builder_with_target(
              &rtx_sender
                .static_pad("src")
                .context("rtprtxsend has no src pad")?,
            )?
            .name(format!("src_{}", session))
            .build(),
          )?;
          bin.add_pad(
            &gstreamer::GhostPad::builder_with_target(
              &rtx_sender
                .static_pad("sink")
                .context("rtprtxsend has no sink pad")?,
            )?
            .name(format!("sink_{}", session))
            .build(),
          )?;
          Ok::<_, anyhow::Error>(Some(bin.to_value()))
        };
        match f() {
          Ok(o) => o,
          Err(e) => {
            warn!("request-aux-sender: {:?}", e);
            None
          },
        }
      });
    }

    rtpbin.connect("request-aux-receiver", false, move |values| {
      let f = || {
        let session: u32 = values[1].get()?;
        debug!("creating RTX receiver for session {}", session);
        let mut pt_map = gstreamer::Structure::builder("application/x-rtp-pt-map");
        for (pt, rtx_pt) in pts.iter() {
          pt_map = pt_map.field(pt, rtx_pt);
        }
        let bin = gstreamer::Bin::new();
        let rtx_receiver = gstreamer::ElementFactory::make("rtprtxreceive")
          .property("payload-type-map", pt_map.build())
          .build()?;
        bin.add(&rtx_receiver)?;
        bin.add_pad(
          &gstreamer::GhostPad::builder_with_target(
            &rtx_receiver
              .static_pad("src")
              .context("rtprtxreceive has no src pad")?,
          )?
          .name(format!("src_{}", session))
          .build(),
        )?;
        bin.add_pad(
          &gstreamer::GhostPad::builder_with_target(
            &rtx_receiver
              .static_pad("sink")
              .context("rtprtxreceive has no sink pad")?,
          )?
          .name(format!("sink_{}", session))
          .build(),
        )?;
        Ok::<_, anyhow::Error>(Some(bin.to_value()))
      };
      match f() {
        Ok(o) => o,
        Err(e) => {
          warn!("request-aux-receiver: {:?}", e);
          None
        },
      }
    });
  }

  /// Spawns the bus-watching task and returns a receiver that fires when the pipeline hits Null.
  fn setup_bus_monitor(pipeline: &gstreamer::Pipeline) -> Result<oneshot::Receiver<()>> {
    let bus = pipeline.bus().context("failed to get pipeline bus")?;
    let (pipeline_state_null_tx, pipeline_state_null_rx) = oneshot::channel();
    tokio::spawn(async move {
      use futures::stream::StreamExt as _;
      let mut stream = bus.stream();
      while let Some(msg) = stream.next().await {
        match msg.view() {
          gstreamer::MessageView::Error(e) => {
            if let Some(d) = e.debug() {
              error!("{}", d);
            }
          },
          gstreamer::MessageView::Warning(e) => {
            if let Some(d) = e.debug() {
              warn!("{}", d);
            }
          },
          gstreamer::MessageView::StateChanged(state)
            if state.current() == gstreamer::State::Null =>
          {
            debug!("pipeline state is null");
            pipeline_state_null_tx.send(());
            break;
          },
          _ => {},
        }
      }
    });
    Ok(pipeline_state_null_rx)
  }

  #[allow(clippy::too_many_arguments)]
  pub(crate) async fn build(
    conference: &JitsiConference,
    ice_transport: &IceUdpTransport,
    codecs: Vec<Codec>,
    audio_hdrext_ssrc_audio_level: Option<u16>,
    audio_hdrext_transport_cc: Option<u16>,
    video_hdrext_transport_cc: Option<u16>,
    remote_ssrc_map: HashMap<u32, Source>,
    dtls_cert_pem: &str,
    dtls_private_key_pem: &str,
    audio_ssrc: u32,
    video_ssrc: u32,
    video_rtx_ssrc: u32,
  ) -> Result<Self> {
    debug!("building gstreamer pipeline");

    let pipeline = gstreamer::Pipeline::new();

    let rtpbin = Self::build_rtpbin()?;
    pipeline.add(&rtpbin)?;

    let (ice_agent, ice_stream_id, ice_component_id) =
      ice::setup_ice(conference, ice_transport).await?;

    let pts: Vec<(String, u32)> = codecs
      .iter()
      .filter(|codec| codec.is_video())
      .flat_map(|codec| {
        codec
          .rtx_pt
          .map(|rtx_pt| (codec.pt.to_string(), rtx_pt as u32))
      })
      .collect();

    Self::connect_request_pt_map(
      &rtpbin,
      codecs.clone(),
      audio_hdrext_ssrc_audio_level,
      audio_hdrext_transport_cc,
      video_hdrext_transport_cc,
    );
    Self::connect_new_jitterbuffer(&rtpbin, conference);
    Self::connect_request_aux(&rtpbin, pts, video_ssrc, video_rtx_ssrc);
    receive::connect_pad_added(&rtpbin, &pipeline, conference, codecs.clone());

    let (audio_sink_element, video_sink_element, rtpfunnel) =
      send::build_send_path(&pipeline, &codecs, audio_ssrc, video_ssrc, conference)?;

    debug!("linking rtpfunnel -> rtpbin");
    rtpfunnel.link_pads(None, &rtpbin, Some("send_rtp_sink_0"))?;

    let transport_bin = transport::build_transport_bin(
      &ice_agent,
      ice_stream_id,
      ice_component_id,
      dtls_cert_pem,
      dtls_private_key_pem,
      conference,
    )?;
    pipeline.add(&transport_bin)?;
    transport_bin.sync_state_with_parent()?;

    debug!("linking transport bin <-> rtpbin");
    transport_bin.link_pads(Some("rtp_recv_src"), &rtpbin, Some("recv_rtp_sink_0"))?;
    transport_bin.link_pads(Some("rtcp_recv_src"), &rtpbin, Some("recv_rtcp_sink_0"))?;
    rtpbin.link_pads(Some("send_rtp_src_0"), &transport_bin, Some("rtp_send_sink"))?;
    rtpbin.link_pads(Some("send_rtcp_src_0"), &transport_bin, Some("rtcp_send_sink"))?;

    let pipeline_state_null_rx = Self::setup_bus_monitor(&pipeline)?;

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
      decode_bins: HashMap::new(),
      jvb_fakesinks: Vec::new(),
    })
  }

  pub(crate) async fn source_add(&mut self, jingle: &Jingle) -> Result<()> {
    for content in &jingle.contents {
      if let Some(Description::Rtp(description)) = &content.description {
        for ssrc in &description.ssrcs {
          let owner = ssrc
            .info
            .as_ref()
            .context("missing ssrc-info")?
            .owner
            .clone();

          debug!("adding ssrc to remote_ssrc_map: {:?}", ssrc);
          self.remote_ssrc_map.insert(
            ssrc.id,
            Source {
              ssrc: ssrc.id,
              participant_id: participant_id_for_owner(owner)?,
              media_type: if description.media == "audio" {
                MediaType::Audio
              }
              else {
                MediaType::Video
              },
            },
          );
        }
      }
    }
    Ok(())
  }

  pub(crate) fn source_remove(&mut self, jingle: &Jingle) -> Result<()> {
    for content in &jingle.contents {
      if let Some(Description::Rtp(description)) = &content.description {
        for ssrc in &description.ssrcs {
          debug!("removing ssrc from remote_ssrc_map: {:?}", ssrc);
          self.remote_ssrc_map.remove(&ssrc.id);
          if let Some(info) = self.decode_bins.remove(&ssrc.id) {
            receive::teardown_decode_bin(&self.pipeline, info, ssrc.id);
          }
        }
      }
    }
    Ok(())
  }

  pub(crate) fn remove_participant(&mut self, participant_id: &str) {
    // Remove all SSRCs for this participant from remote_ssrc_map
    self
      .remote_ssrc_map
      .retain(|_, source| source.participant_id.as_deref() != Some(participant_id));

    // Tear down all decode bins for this participant
    let to_remove: Vec<u32> = self
      .decode_bins
      .iter()
      .filter(|(_, info)| info.participant_id == participant_id)
      .map(|(ssrc, _)| *ssrc)
      .collect();
    for ssrc in to_remove {
      if let Some(info) = self.decode_bins.remove(&ssrc) {
        receive::teardown_decode_bin(&self.pipeline, info, ssrc);
      }
    }

    // Remove participant bin if it exists
    if let Some(bin) = self
      .pipeline
      .by_name(&format!("participant_{}", participant_id))
    {
      debug!("removing participant bin for {}", participant_id);
      if let Err(e) = bin.set_state(gstreamer::State::Null) {
        warn!(
          "failed to set participant bin state to Null: {:?}",
          e
        );
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
