use anyhow::{bail, Context, Result};
use glib::prelude::ToValue as _;
use gstreamer::prelude::{
  ElementExtManual as _, GObjectExtManualGst as _, GstBinExt as _, ObjectExt as _,
};
use gstreamer_rtp::{prelude::RTPHeaderExtensionExt as _, RTPHeaderExtension};
use tracing::{debug, warn};

use crate::conference::JitsiConference;

use super::{Codec, CodecName};

pub(super) fn build_send_path(
  pipeline: &gstreamer::Pipeline,
  codecs: &[Codec],
  audio_ssrc: u32,
  video_ssrc: u32,
  conference: &JitsiConference,
) -> Result<(gstreamer::Element, gstreamer::Element, gstreamer::Element)> {
  let opus = codecs.iter().find(|codec| codec.name == CodecName::Opus);
  let audio_sink_element = if let Some(opus) = opus {
    let audio_sink_element = gstreamer::ElementFactory::make(opus.payloader_name()).build()?;
    audio_sink_element.set_property("pt", opus.pt as u32);
    audio_sink_element
  }
  else {
    bail!("no opus payload type in jingle session-initiate");
  };
  audio_sink_element.set_property("min-ptime", 10i64 * 1000 * 1000);
  audio_sink_element.set_property("ssrc", audio_ssrc);
  if audio_sink_element.has_property("auto-header-extension") {
    audio_sink_element.set_property("auto-header-extension", false);
    audio_sink_element.connect("request-extension", false, move |values| {
      let f = || {
        let ext_id: u32 = values[1].get()?;
        let ext_uri: String = values[2].get()?;
        debug!(
          "audio payloader requested extension: {} {}",
          ext_id, ext_uri
        );
        let hdrext =
          RTPHeaderExtension::create_from_uri(&ext_uri).context("failed to create hdrext")?;
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
  }
  else {
    debug!("audio payloader: no rtp header extension support");
  }
  pipeline.add(&audio_sink_element)?;

  let codec_name = conference.config.video_codec.as_str();
  let codec = codecs.iter().find(|codec| codec.is_codec(codec_name));
  let video_sink_element = if let Some(codec) = codec {
    let element = gstreamer::ElementFactory::make(codec.payloader_name()).build()?;
    element.set_property("pt", codec.pt as u32);
    if codec.name == CodecName::H264 {
      element.set_property_from_str("aggregate-mode", "zero-latency");
    }
    else if codec.name == CodecName::Vp8 || codec.name == CodecName::Vp9 {
      element.set_property_from_str("picture-id-mode", "15-bit");
    }
    element
  }
  else {
    bail!("unsupported video codec: {}", codec_name);
  };
  video_sink_element.set_property("ssrc", video_ssrc);
  if video_sink_element.has_property("auto-header-extension") {
    video_sink_element.set_property("auto-header-extension", false);
    video_sink_element.connect("request-extension", false, move |values| {
      let f = || {
        let ext_id: u32 = values[1].get()?;
        let ext_uri: String = values[2].get()?;
        debug!(
          "video payloader requested extension: {} {}",
          ext_id, ext_uri
        );
        let hdrext =
          RTPHeaderExtension::create_from_uri(&ext_uri).context("failed to create hdrext")?;
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
  }
  else {
    debug!("video payloader: no rtp header extension support");
  }
  pipeline.add(&video_sink_element)?;

  let rtpfunnel = gstreamer::ElementFactory::make("rtpfunnel").build()?;
  pipeline.add(&rtpfunnel)?;

  debug!("linking video payloader -> rtpfunnel");
  video_sink_element.link(&rtpfunnel)?;

  debug!("linking audio payloader -> rtpfunnel");
  audio_sink_element.link(&rtpfunnel)?;

  Ok((audio_sink_element, video_sink_element, rtpfunnel))
}
