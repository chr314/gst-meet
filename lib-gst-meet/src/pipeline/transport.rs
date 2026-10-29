use std::sync::Mutex;

#[cfg(not(feature = "log-rtp"))]
use anyhow::Context;
use anyhow::Result;
use gstreamer::{prelude::*, GhostPad};
use nice_gst_meet as nice;
use tracing::warn;
use uuid::Uuid;

use super::{dtls, offer::Fingerprint};

pub(super) struct TransportParams<'a> {
  pub(super) agent: &'a nice::Agent,
  pub(super) stream_id: u32,
  pub(super) component_id: u32,
  pub(super) pem: &'a str,
  pub(super) is_client: bool,
  pub(super) peer_fingerprint: Fingerprint,
  /// Called (once) with the reason when the peer's certificate does not match its fingerprint.
  pub(super) on_fingerprint_mismatch: Box<dyn Fn(String) + Send + Sync>,
  #[cfg(feature = "log-rtp")]
  pub(super) log_rtp: bool,
  #[cfg(feature = "log-rtp")]
  pub(super) log_rtcp: bool,
}

/// The two directions are bins of their own: together they make a cycle with rtpbin, which GStreamer warns
/// about whenever it sorts the elements of the session bin.
pub(super) struct TransportBins {
  /// With the ghost pads `rtp_recv_src` and `rtcp_recv_src`.
  pub(super) receive: gstreamer::Bin,
  /// With the ghost pads `rtp_send_sink` and `rtcp_send_sink`.
  pub(super) send: gstreamer::Bin,
}

pub(super) struct Transport {
  pub(super) bins: TransportBins,
  /// Held in `Null` until ICE has connected when we are the DTLS client, so that its ClientHello is not sent
  /// into a connection that does not exist yet (OpenSSL would only retransmit it seconds later). `None` when we
  /// are the server. [`EncoderRelease`] lets it go.
  pub(super) locked_encoder: Option<gstreamer::Element>,
}

enum Held {
  Encoder(glib::WeakRef<gstreamer::Element>),
  /// Started, or the session is going away.
  Done,
}

/// Lets go of the encoder that [`Transport::locked_encoder`] holds back, at most once and never after
/// [`close`](Self::close).
///
/// ICE reports `Connected` on the thread that happens to emit libnice's signals, which is `nicesrc`'s streaming
/// thread as likely as the agent's main context, so it can run at any moment of the session's teardown. Taking
/// the bin apart must not interleave with it: `sync_state_with_parent` reads the state of the parent and then
/// sets the encoder's, and setting `Playing` after the bin went to `Null` leaves a running element in a bin that
/// is no longer part of anything. Hence both go through one lock, and whatever teardown does comes after `close`.
pub(super) struct EncoderRelease {
  held: Mutex<Held>,
}

impl EncoderRelease {
  pub(super) fn new(encoder: &gstreamer::Element) -> Self {
    Self {
      held: Mutex::new(Held::Encoder(encoder.downgrade())),
    }
  }

  pub(super) fn release(&self) {
    let mut held = self.held.lock().unwrap_or_else(|e| e.into_inner());
    let Held::Encoder(encoder) = &*held else {
      return;
    };
    if let Some(encoder) = encoder.upgrade() {
      encoder.set_locked_state(false);
      if let Err(e) = encoder.sync_state_with_parent() {
        warn!("failed to start the DTLS encoder: {:?}", e);
      }
    }
    *held = Held::Done;
  }

  /// Returns once a [`release`](Self::release) that was running has finished; none starts the encoder after.
  pub(super) fn close(&self) {
    *self.held.lock().unwrap_or_else(|e| e.into_inner()) = Held::Done;
  }
}

pub(super) fn build_transport_bin(params: TransportParams) -> Result<Transport> {
  let nicesrc = gstreamer::ElementFactory::make("nicesrc")
    .property("stream", params.stream_id)
    .property("component", params.component_id)
    .property("agent", params.agent)
    .build()?;
  let nicesink = gstreamer::ElementFactory::make("nicesink")
    .property("stream", params.stream_id)
    .property("component", params.component_id)
    .property("agent", params.agent)
    .property("sync", false)
    .property("async", false)
    .build()?;

  // Encoder and decoder are paired by this id, which must be unique among all DTLS connections of the process.
  let connection_id = Uuid::new_v4().to_string();
  let dtlssrtpenc = gstreamer::ElementFactory::make("dtlssrtpenc")
    .property("connection-id", &connection_id)
    .property("is-client", params.is_client)
    .build()?;
  let dtlssrtpdec = gstreamer::ElementFactory::make("dtlssrtpdec")
    .property("connection-id", &connection_id)
    .property("pem", params.pem)
    .build()?;
  let (receive, send) = (gstreamer::Bin::new(), gstreamer::Bin::new());
  receive.add_many([&nicesrc, &dtlssrtpdec])?;
  send.add_many([&dtlssrtpenc, &nicesink])?;

  let gate = dtls::gate_decoder(&dtlssrtpdec, params.peer_fingerprint, params.on_fingerprint_mismatch)?;

  let locked_encoder = params.is_client.then(|| {
    dtlssrtpenc.set_locked_state(true);
    dtlssrtpenc.clone()
  });

  #[cfg(feature = "log-rtp")]
  let (rtp_recv_src, rtcp_recv_src, rtp_send_sink, rtcp_send_sink) = logging::link_loggers(
    &receive,
    &send,
    &dtlssrtpdec,
    &dtlssrtpenc,
    params.log_rtp,
    params.log_rtcp,
  )?;

  #[cfg(not(feature = "log-rtp"))]
  let (rtp_recv_src, rtcp_recv_src, rtp_send_sink, rtcp_send_sink) = (
    dtlssrtpdec
      .static_pad("rtp_src")
      .context("dtlssrtpdec has no rtp_src pad")?,
    dtlssrtpdec
      .static_pad("rtcp_src")
      .context("dtlssrtpdec has no rtcp_src pad")?,
    dtlssrtpenc
      .request_pad_simple("rtp_sink_0")
      .context("dtlssrtpenc refused to create rtp_sink_0")?,
    dtlssrtpenc
      .request_pad_simple("rtcp_sink_0")
      .context("dtlssrtpenc refused to create rtcp_sink_0")?,
  );

  nicesrc.link(&dtlssrtpdec)?;
  dtlssrtpenc.link_pads(Some("src"), &nicesink, Some("sink"))?;
  // The keys exist as soon as the handshake is through, whoever the peer turns out to be.
  gate.gate_encoder_sink(&dtlssrtpenc, "rtp_sink_0")?;
  gate.gate_encoder_sink(&dtlssrtpenc, "rtcp_sink_0")?;

  for (bin, name, target) in [
    (&receive, "rtp_recv_src", &rtp_recv_src),
    (&receive, "rtcp_recv_src", &rtcp_recv_src),
    (&send, "rtp_send_sink", &rtp_send_sink),
    (&send, "rtcp_send_sink", &rtcp_send_sink),
  ] {
    bin.add_pad(&GhostPad::builder_with_target(target)?.name(name).build())?;
  }

  Ok(Transport {
    bins: TransportBins { receive, send },
    locked_encoder,
  })
}

#[cfg(feature = "log-rtp")]
mod logging {
  use anyhow::{anyhow, Context, Result};
  use gstreamer::prelude::*;
  use gstreamer_rtp::RTPBuffer;
  use tracing::{debug, warn};

  type Pads = (gstreamer::Pad, gstreamer::Pad, gstreamer::Pad, gstreamer::Pad);

  /// Puts an `identity` between the DTLS elements and rtpbin on each of the four paths, and logs the packets
  /// that pass if asked to.
  pub(super) fn link_loggers(
    receive: &gstreamer::Bin,
    send: &gstreamer::Bin,
    dtlssrtpdec: &gstreamer::Element,
    dtlssrtpenc: &gstreamer::Element,
    log_rtp: bool,
    log_rtcp: bool,
  ) -> Result<Pads> {
    let identity = || gstreamer::ElementFactory::make("identity").build();
    let rtp_recv = identity()?;
    let rtcp_recv = identity()?;
    let rtp_send = identity()?;
    let rtcp_send = identity()?;
    receive.add_many([&rtp_recv, &rtcp_recv])?;
    send.add_many([&rtp_send, &rtcp_send])?;

    if log_rtp {
      debug!("setting up RTP packet logging");
      rtp_recv.connect("handoff", false, rtp_logger("RECV"));
      rtp_send.connect("handoff", false, rtp_logger("SEND"));
    }
    if log_rtcp {
      debug!("setting up RTCP packet logging");
      rtcp_recv.connect("handoff", false, rtcp_logger("RECV"));
      rtcp_send.connect("handoff", false, rtcp_logger("SEND"));
    }

    dtlssrtpdec.link_pads(Some("rtp_src"), &rtp_recv, None)?;
    dtlssrtpdec.link_pads(Some("rtcp_src"), &rtcp_recv, None)?;
    rtp_send.link_pads(None, dtlssrtpenc, Some("rtp_sink_0"))?;
    rtcp_send.link_pads(None, dtlssrtpenc, Some("rtcp_sink_0"))?;

    let pad = |element: &gstreamer::Element, name: &str| {
      element
        .static_pad(name)
        .with_context(|| format!("{} has no {} pad", element.name(), name))
    };
    Ok((
      pad(&rtp_recv, "src")?,
      pad(&rtcp_recv, "src")?,
      pad(&rtp_send, "sink")?,
      pad(&rtcp_send, "sink")?,
    ))
  }

  fn rtp_logger(direction: &'static str) -> impl Fn(&[glib::Value]) -> Option<glib::Value> + Send + Sync + 'static {
    move |values| {
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
  }

  fn rtcp_logger(direction: &'static str) -> impl Fn(&[glib::Value]) -> Option<glib::Value> + Send + Sync + 'static {
    move |values| {
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
  }
}
