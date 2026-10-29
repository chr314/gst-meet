//! The send path lives at the top level of the pipeline for the whole conference, so that the application's
//! encoders keep running while sessions come and go. Each session links it in through a barrier, which turns the
//! time without a session (and a DTLS encoder that is not up yet) into dropped packets rather than errors.

use anyhow::{bail, Context, Result};
use gstreamer::prelude::*;
use tracing::{debug, warn};

use super::{
  codec::{Codec, CodecName},
  elements::StreamBarrier,
  offer::SessionOffer,
  rtp::connect_request_extension,
  LocalSsrcs,
};

pub(super) struct SendPath {
  audio: gstreamer::Element,
  video: gstreamer::Element,
  video_codec: CodecName,
  barrier: StreamBarrier,
}

impl SendPath {
  /// The payloaders for the codecs negotiated in the first offer: Opus, and the first of `video_codecs` that the
  /// offer has.
  pub(super) fn new(
    pipeline: &gstreamer::Pipeline,
    offer: &SessionOffer,
    video_codecs: &[String],
    ssrcs: LocalSsrcs,
  ) -> Result<Self> {
    let Some(opus) = offer.codecs.iter().find(|codec| codec.name == CodecName::Opus) else {
      bail!("no opus payload type in the offer");
    };
    let Some(video_codec) = Self::video_codec(offer, video_codecs) else {
      bail!("unsupported video codec(s): {:?}", video_codecs);
    };

    let audio = gstreamer::ElementFactory::make(opus.payloader_name()).build()?;
    audio.set_property("pt", opus.pt as u32);
    audio.set_property("min-ptime", 10i64 * 1000 * 1000);
    audio.set_property("ssrc", ssrcs.audio);
    configure_header_extensions(&audio, "audio payloader");

    let video = gstreamer::ElementFactory::make(video_codec.payloader_name()).build()?;
    video.set_property("pt", video_codec.pt as u32);
    match video_codec.name {
      CodecName::H264 => video.set_property_from_str("aggregate-mode", "zero-latency"),
      CodecName::Vp8 | CodecName::Vp9 => video.set_property_from_str("picture-id-mode", "15-bit"),
      CodecName::Opus | CodecName::Av1 => {},
    }
    video.set_property("ssrc", ssrcs.video);
    configure_header_extensions(&video, "video payloader");

    let funnel = gstreamer::ElementFactory::make("rtpfunnel").build()?;
    let barrier = StreamBarrier::new("send_barrier");
    barrier.set_callbacks(
      || {},
      |flow| {
        debug!(
          "the session does not take our RTP ({:?}); dropping it until it does",
          flow
        )
      },
    );

    pipeline.add_many([&audio, &video, &funnel, barrier.upcast_ref()])?;
    video.link(&funnel)?;
    audio.link(&funnel)?;
    funnel.link(&barrier)?;

    Ok(Self {
      audio,
      video,
      video_codec: video_codec.name,
      barrier,
    })
  }

  fn video_codec<'a>(offer: &'a SessionOffer, video_codecs: &[String]) -> Option<&'a Codec> {
    video_codecs
      .iter()
      .find_map(|name| offer.codecs.iter().find(|codec| codec.is_codec(name)))
  }

  pub(super) fn audio_sink_element(&self) -> gstreamer::Element {
    self.audio.clone()
  }

  pub(super) fn video_sink_element(&self) -> gstreamer::Element {
    self.video.clone()
  }

  /// A later session may use other payload types. The video codec is that of the encoder the application
  /// feeds, so it cannot follow.
  pub(super) fn update(&self, offer: &SessionOffer, video_codecs: &[String]) {
    if let Some(opus) = offer.codecs.iter().find(|codec| codec.name == CodecName::Opus) {
      self.audio.set_property("pt", opus.pt as u32);
    }
    match Self::video_codec(offer, video_codecs) {
      Some(codec) if codec.name == self.video_codec => self.video.set_property("pt", codec.pt as u32),
      Some(codec) => warn!(
        "the offer now selects {} for video, but the video payloader is for {:?}",
        codec.encoding_name(),
        self.video_codec
      ),
      None => warn!("the offer has none of the configured video codecs"),
    }
  }

  pub(super) fn attach(&self, session_sink: &gstreamer::Pad) -> Result<()> {
    self
      .barrier
      .static_pad("src")
      .context("the send barrier has no src pad")?
      .link(session_sink)
      .map(|_| ())
      .map_err(|e| anyhow::anyhow!("failed to link the send path to the session: {:?}", e))
  }

  /// Until the next session is attached the barrier drops what the payloaders produce.
  pub(super) fn detach(&self, session_sink: &gstreamer::Pad) {
    if let Some(src) = self.barrier.static_pad("src") {
      if src.peer().as_ref() == Some(session_sink) {
        let _ = src.unlink(session_sink);
      }
    }
  }
}

fn configure_header_extensions(payloader: &gstreamer::Element, description: &str) {
  if payloader.has_property("auto-header-extension") {
    payloader.set_property("auto-header-extension", false);
    connect_request_extension(payloader, description);
  }
  else {
    debug!("{}: no rtp header extension support", description);
  }
}
