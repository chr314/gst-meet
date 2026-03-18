#[cfg(feature = "log-rtp")]
use anyhow::anyhow;
use anyhow::{Context, Result};
use gstreamer::{
    prelude::{ElementExt as _, ElementExtManual as _, GstBinExt as _},
    GhostPad,
};
use nice_gst_meet as nice;
use tracing::debug;
#[cfg(feature = "log-rtp")]
use tracing::warn;

#[cfg(feature = "log-rtp")]
use gstreamer_rtp::RTPBuffer;

use crate::conference::JitsiConference;

pub(super) fn build_transport_bin(
  ice_agent: &nice::Agent,
  ice_stream_id: u32,
  ice_component_id: u32,
  dtls_cert_pem: &str,
  dtls_private_key_pem: &str,
  conference: &JitsiConference,
) -> Result<gstreamer::Bin> {
  // Suppress unused-variable warning when log-rtp feature is disabled.
  #[cfg(not(feature = "log-rtp"))]
  let _ = conference;

  let bin = gstreamer::Bin::new();

  let nicesrc = gstreamer::ElementFactory::make("nicesrc")
    .property("stream", ice_stream_id)
    .property("component", ice_component_id)
    .property("agent", ice_agent)
    .build()?;
  bin.add(&nicesrc)?;

  let nicesink = gstreamer::ElementFactory::make("nicesink")
    .property("stream", ice_stream_id)
    .property("component", ice_component_id)
    .property("agent", ice_agent)
    .property("sync", false)
    .property("async", false)
    .build()?;
  bin.add(&nicesink)?;

  let dtls_srtp_connection_id = "gst-meet";

  let dtlssrtpenc = gstreamer::ElementFactory::make("dtlssrtpenc")
    .property("connection-id", dtls_srtp_connection_id)
    .property("is-client", true)
    .build()?;
  bin.add(&dtlssrtpenc)?;

  let dtlssrtpdec = gstreamer::ElementFactory::make("dtlssrtpdec")
    .property("connection-id", dtls_srtp_connection_id)
    .property("pem", format!("{}\n{}", dtls_cert_pem, dtls_private_key_pem))
    .build()?;
  bin.add(&dtlssrtpdec)?;

  let rtp_recv_identity = gstreamer::ElementFactory::make("identity").build()?;
  bin.add(&rtp_recv_identity)?;
  let rtcp_recv_identity = gstreamer::ElementFactory::make("identity").build()?;
  bin.add(&rtcp_recv_identity)?;
  let rtp_send_identity = gstreamer::ElementFactory::make("identity").build()?;
  bin.add(&rtp_send_identity)?;
  let rtcp_send_identity = gstreamer::ElementFactory::make("identity").build()?;
  bin.add(&rtcp_send_identity)?;

  #[cfg(feature = "log-rtp")]
  {
    if conference.config.log_rtp {
      debug!("setting up RTP packet logging");

      let make_rtp_logger = |direction: &'static str| {
        move |values: &[glib::Value]| -> Option<glib::Value> {
          let f = || {
            let buffer: gstreamer::Buffer = values[1].get()?;
            let rtp_buffer = RTPBuffer::from_buffer_readable(&buffer)?;
            debug!(
              ssrc = rtp_buffer.ssrc(),
              pt = rtp_buffer.payload_type(),
              seq = rtp_buffer.seq(),
              ts = rtp_buffer.timestamp(),
              marker = rtp_buffer.is_marker(),
              extension = rtp_buffer.is_extension(),
              payload_size = rtp_buffer.payload_size(),
              "RTP {}",
              direction,
            );
            Ok::<_, anyhow::Error>(())
          };
          if let Err(e) = f() {
            warn!("RTP {}: {:?}", direction, e);
          }
          None
        }
      };

      rtp_recv_identity.connect("handoff", false, make_rtp_logger("RECV"));
      rtp_send_identity.connect("handoff", false, make_rtp_logger("SEND"));
    }

    if conference.config.log_rtcp {
      debug!("setting up RTCP packet logging");

      let make_rtcp_logger = |direction: &'static str| {
        move |values: &[glib::Value]| -> Option<glib::Value> {
          let f = || {
            let buffer: gstreamer::Buffer = values[1].get()?;
            let mut buf = [0u8; 1500];
            buffer
              .copy_to_slice(0, &mut buf[..buffer.size()])
              .map_err(|_| anyhow!("invalid RTCP packet size"))?;
            let decoded = rtcp::packet::unmarshal(&mut &buf[..buffer.size()])?;
            debug!("RTCP {} size={}\n{:#?}", direction, buffer.size(), decoded,);
            Ok::<_, anyhow::Error>(())
          };
          if let Err(e) = f() {
            warn!("RTCP {}: {:?}", direction, e);
          }
          None
        }
      };

      rtcp_recv_identity.connect("handoff", false, make_rtcp_logger("RECV"));
      rtcp_send_identity.connect("handoff", false, make_rtcp_logger("SEND"));
    }
  }

  // Internal element linking
  debug!("link dtlssrtpdec -> identity elements");
  dtlssrtpdec.link_pads(Some("rtp_src"), &rtp_recv_identity, None)?;
  dtlssrtpdec.link_pads(Some("rtcp_src"), &rtcp_recv_identity, None)?;

  debug!("linking identity elements -> dtlssrtpenc");
  rtp_send_identity.link_pads(None, &dtlssrtpenc, Some("rtp_sink_0"))?;
  rtcp_send_identity.link_pads(None, &dtlssrtpenc, Some("rtcp_sink_0"))?;

  debug!("linking ice src -> dtlssrtpdec");
  nicesrc.link(&dtlssrtpdec)?;

  debug!("linking dtlssrtpenc -> ice sink");
  dtlssrtpenc.link_pads(Some("src"), &nicesink, Some("sink"))?;

  // Expose ghost pads for connections to/from rtpbin
  bin.add_pad(
    &GhostPad::builder_with_target(
      &rtp_recv_identity
        .static_pad("src")
        .context("rtp_recv_identity has no src pad")?,
    )?
    .name("rtp_recv_src")
    .build(),
  )?;
  bin.add_pad(
    &GhostPad::builder_with_target(
      &rtcp_recv_identity
        .static_pad("src")
        .context("rtcp_recv_identity has no src pad")?,
    )?
    .name("rtcp_recv_src")
    .build(),
  )?;
  bin.add_pad(
    &GhostPad::builder_with_target(
      &rtp_send_identity
        .static_pad("sink")
        .context("rtp_send_identity has no sink pad")?,
    )?
    .name("rtp_send_sink")
    .build(),
  )?;
  bin.add_pad(
    &GhostPad::builder_with_target(
      &rtcp_send_identity
        .static_pad("sink")
        .context("rtcp_send_identity has no sink pad")?,
    )?
    .name("rtcp_send_sink")
    .build(),
  )?;

  Ok(bin)
}
