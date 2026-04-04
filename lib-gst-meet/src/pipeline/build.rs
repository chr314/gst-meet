use std::{
  collections::HashMap,
  sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use glib::prelude::{Cast as _, ToValue as _};
use gstreamer::prelude::{ElementExt as _, GstBinExt as _, GstObjectExt as _, ObjectExt as _};
use tokio::sync::oneshot;
use tracing::{debug, error, warn};

use super::{
  codec::{Codec, RTP_HDREXT_SSRC_AUDIO_LEVEL, RTP_HDREXT_TRANSPORT_CC},
  decode::SsrcState,
};

pub(crate) struct PipelineBuildConfig {
  pub(crate) codecs: Vec<Codec>,
  pub(crate) audio_hdrext_ssrc_audio_level: Option<u16>,
  pub(crate) audio_hdrext_transport_cc: Option<u16>,
  pub(crate) video_hdrext_transport_cc: Option<u16>,
  pub(crate) remote_ssrc_map: HashMap<u32, crate::source::Source>,
  pub(crate) dtls_cert_pem: String,
  pub(crate) dtls_private_key_pem: String,
  pub(crate) audio_ssrc: u32,
  pub(crate) video_ssrc: u32,
  pub(crate) video_rtx_ssrc: u32,
}

pub(super) fn build_rtpbin(latency: u32) -> Result<gstreamer::Element> {
  Ok(
    gstreamer::ElementFactory::make("rtpbin")
      .name("rtpbin")
      .property_from_str("rtp-profile", "savpf")
      .property("autoremove", true)
      .property("do-lost", true)
      .property("do-sync-event", true)
      .property("latency", latency)
      .build()?,
  )
}

pub(super) fn connect_request_pt_map(
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
              .field("rtcp-fb-nack-pli", true)
              .field("rtcp-fb-transport-cc", true);
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
      Ok(None) => Some(Option::<gstreamer::Caps>::None.to_value()),
      Err(e) => {
        error!("handling request-pt-map: {:?}", e);
        Some(Option::<gstreamer::Caps>::None.to_value())
      },
    }
  });
}

/// Connects the `new-jitterbuffer` signal that enables retransmission and configures latency.
pub(super) fn connect_new_jitterbuffer(
  rtpbin: &gstreamer::Element,
  ssrc_registry: &Arc<Mutex<HashMap<u32, SsrcState>>>,
  buffer_size: u32,
) {
  let ssrc_registry = ssrc_registry.clone();
  rtpbin.connect("new-jitterbuffer", false, move |values| {
    let ssrc_registry = ssrc_registry.clone();
    let f = move || {
      let rtpjitterbuffer: gstreamer::Element = values[1].get()?;
      let session: u32 = values[2].get()?;
      let ssrc: u32 = values[3].get()?;
      debug!("new jitterbuffer created for session {} ssrc {}", session, ssrc);

      let enable_rtx = match ssrc_registry
        .lock()
        .map_err(|e| anyhow::anyhow!("ssrc_registry lock poisoned: {}", e))?
        .get(&ssrc)
      {
        Some(SsrcState::Signaled { media_type, participant_id }) => {
          media_type.is_video() && participant_id.is_some()
        },
        Some(SsrcState::Active { media_type, participant_id, .. }) => {
          media_type.is_video() && !participant_id.is_empty()
        },
        Some(SsrcState::Pending { .. }) => {
          warn!("new-jitterbuffer for pending ssrc {} (no media type known yet)", ssrc);
          false
        },
        None => {
          warn!("new-jitterbuffer for unknown ssrc {} — speculatively enabling RTX", ssrc);
          true
        },
      };

      if enable_rtx {
        debug!("enabling RTX for ssrc {}", ssrc);
        rtpjitterbuffer.set_property("do-retransmission", true);
        rtpjitterbuffer.set_property("drop-on-latency", false);
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

pub(super) fn connect_request_aux(
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
        let bin = build_rtx_bin("rtprtxsend", pt_map.build(), Some(ssrc_map.build()), session)?;
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
      let bin = build_rtx_bin("rtprtxreceive", pt_map.build(), None, session)?;
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

pub(super) fn build_rtx_bin(
  element_name: &str,
  pt_map: gstreamer::Structure,
  ssrc_map: Option<gstreamer::Structure>,
  session: u32,
) -> Result<gstreamer::Bin> {
  let mut builder = gstreamer::ElementFactory::make(element_name).property("payload-type-map", pt_map);
  if let Some(ssrc_map) = ssrc_map {
    builder = builder.property("ssrc-map", ssrc_map);
  }
  let rtx_element = builder.build()?;
  let bin = gstreamer::Bin::new();
  bin.add(&rtx_element)?;
  bin.add_pad(
    &gstreamer::GhostPad::builder_with_target(
      &rtx_element
        .static_pad("src")
        .context(format!("{} has no src pad", element_name))?,
    )?
    .name(format!("src_{}", session))
    .build(),
  )?;
  bin.add_pad(
    &gstreamer::GhostPad::builder_with_target(
      &rtx_element
        .static_pad("sink")
        .context(format!("{} has no sink pad", element_name))?,
    )?
    .name(format!("sink_{}", session))
    .build(),
  )?;
  Ok(bin)
}

/// Spawns the bus-watching task and returns a receiver that fires when the pipeline hits Null.
pub(super) fn setup_bus_monitor(pipeline: &gstreamer::Pipeline) -> Result<oneshot::Receiver<()>> {
  let bus = pipeline.bus().context("failed to get pipeline bus")?;
  let pipeline_weak = pipeline.downgrade();
  let (pipeline_state_null_tx, pipeline_state_null_rx) = oneshot::channel();
  tokio::spawn(async move {
    use futures::stream::StreamExt as _;
    let mut stream = bus.stream();
    while let Some(msg) = stream.next().await {
      match msg.view() {
        gstreamer::MessageView::Error(e) => {
          let src = msg
            .src()
            .map(|s| s.name().to_string())
            .unwrap_or_else(|| "<unknown>".into());
          error!("GStreamer error from {}: {} (debug: {:?})", src, e.error(), e.debug());
          // Transition the pipeline to Null so pipeline_stopped() unblocks.
          if let Some(pipeline) = pipeline_weak.upgrade() {
            let _ = pipeline.set_state(gstreamer::State::Null);
          }
        },
        gstreamer::MessageView::Warning(e) => {
          if let Some(d) = e.debug() {
            warn!("{}", d);
          }
        },
        gstreamer::MessageView::Latency(_) => {
          if let Some(pipeline) = pipeline_weak.upgrade() {
            if let Err(e) = pipeline.recalculate_latency() {
              warn!("recalculate_latency failed: {:?}", e);
            }
          }
        },
        gstreamer::MessageView::StateChanged(state) if state.current() == gstreamer::State::Null => {
          if msg.src().is_some_and(|s| s.is::<gstreamer::Pipeline>()) {
            debug!("pipeline state is null");
            let _ = pipeline_state_null_tx.send(());
            break;
          }
        },
        _ => {},
      }
    }
  });
  Ok(pipeline_state_null_rx)
}
