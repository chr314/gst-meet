use std::sync::OnceLock;

use anyhow::Result;
use gstreamer::ElementFactory;
use jitsi_xmpp_parsers::jingle_rtp::Description as RtpDescription;
use tracing::debug;
use xmpp_parsers::jingle_rtcp_fb::RtcpFb;

use super::offer::HdrextIds;
use crate::conference::ReceiveMode;

pub(crate) const RTP_HDREXT_SSRC_AUDIO_LEVEL: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
pub(crate) const RTP_HDREXT_TRANSPORT_CC: &str =
  "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodecName {
  Opus,
  H264,
  Vp8,
  Vp9,
  Av1,
}

impl CodecName {
  /// From the lower case names of `video_codecs`.
  pub(crate) fn parse_video(name: &str) -> Option<Self> {
    match name {
      "h264" => Some(Self::H264),
      "vp8" => Some(Self::Vp8),
      "vp9" => Some(Self::Vp9),
      "av1" => Some(Self::Av1),
      _ => None,
    }
  }

  /// A depayloader is present, and then in `Decoded` mode a decoder, in `Encoded` mode the parser the codec
  /// needs. Both the offer and our presence only advertise codecs for which this holds.
  pub(crate) fn receive_supported(self, mode: ReceiveMode) -> bool {
    let present = |name: &str| ElementFactory::find(name).is_some();
    if !present(self.depayloader_name()) {
      return false;
    }
    match mode {
      ReceiveMode::Decoded => present(self.decoder_name()),
      ReceiveMode::Encoded => match self {
        Self::H264 => present("h264parse"),
        Self::Vp9 => present("vp9parse"),
        Self::Av1 => present("av1parse"),
        Self::Opus | Self::Vp8 => true,
      },
    }
  }

  pub(crate) fn depayloader_name(self) -> &'static str {
    match self {
      Self::Opus => "rtpopusdepay",
      Self::H264 => "rtph264depay",
      Self::Vp8 => "rtpvp8depay",
      Self::Vp9 => "rtpvp9depay",
      Self::Av1 => "rtpav1depay",
    }
  }

  pub(crate) fn decoder_name(self) -> &'static str {
    match self {
      Self::Opus => "opusdec",
      Self::H264 => "avdec_h264",
      Self::Vp8 => "vp8dec",
      Self::Vp9 => "vp9dec",
      Self::Av1 => av1_decoder_name(),
    }
  }

  pub(crate) fn payloader_name(self) -> &'static str {
    match self {
      Self::Opus => "rtpopuspay",
      Self::H264 => "rtph264pay",
      Self::Vp8 => "rtpvp8pay",
      Self::Vp9 => "rtpvp9pay",
      Self::Av1 => "rtpav1pay",
    }
  }
}

#[derive(Clone)]
pub(crate) struct Codec {
  pub(crate) name: CodecName,
  pub(crate) pt: u8,
  pub(crate) rtx_pt: Option<u8>,
  pub(crate) rtcp_fbs: Vec<RtcpFb>,
}

impl Codec {
  pub(crate) fn is(&self, pt: u8) -> bool {
    self.pt == pt
  }

  pub(crate) fn is_rtx(&self, rtx_pt: u8) -> bool {
    self.rtx_pt == Some(rtx_pt)
  }

  pub(crate) fn is_audio(&self) -> bool {
    self.name == CodecName::Opus
  }

  pub(crate) fn is_video(&self) -> bool {
    self.name != CodecName::Opus
  }

  pub(crate) fn is_codec(&self, name: &str) -> bool {
    CodecName::parse_video(name) == Some(self.name)
  }

  pub(crate) fn receive_supported(&self, mode: ReceiveMode) -> bool {
    self.name.receive_supported(mode)
  }

  pub(crate) fn encoding_name(&self) -> &'static str {
    match self.name {
      CodecName::Opus => "opus",
      CodecName::H264 => "H264",
      CodecName::Vp8 => "VP8",
      CodecName::Vp9 => "VP9",
      CodecName::Av1 => "AV1",
    }
  }

  pub(crate) fn depayloader_name(&self) -> &'static str {
    self.name.depayloader_name()
  }

  pub(crate) fn decoder_name(&self) -> &'static str {
    self.name.decoder_name()
  }

  pub(crate) fn payloader_name(&self) -> &'static str {
    self.name.payloader_name()
  }
}

fn av1_decoder_name() -> &'static str {
  static CACHED: OnceLock<&'static str> = OnceLock::new();
  CACHED.get_or_init(|| {
    if ElementFactory::find("dav1ddec").is_some() {
      "dav1ddec"
    }
    else {
      "av1dec"
    }
  })
}

pub(crate) struct ParsedRtpDescription {
  pub(crate) codecs: Vec<Codec>,
  pub(crate) hdrexts: HdrextIds,
}

/// The codecs and header extensions of one `<description>`; `None` for media other than audio and video.
pub(crate) fn parse_rtp_description(description: &RtpDescription) -> Result<Option<ParsedRtpDescription>> {
  let mut opus = None;
  let mut h264 = None;
  let mut vp8 = None;
  let mut vp9 = None;
  let mut av1 = None;
  let mut hdrexts = HdrextIds::default();

  if description.media == "audio" {
    for pt in description.payload_types.iter() {
      if pt.name.as_deref() == Some("opus") {
        opus = Some(Codec {
          name: CodecName::Opus,
          pt: pt.id,
          rtx_pt: None,
          rtcp_fbs: pt.rtcp_fbs.clone(),
        });
      }
    }
    for hdrext in description.hdrexts.iter() {
      if hdrext.uri == RTP_HDREXT_SSRC_AUDIO_LEVEL {
        hdrexts.audio_level = Some(hdrext.id);
      }
      else if hdrext.uri == RTP_HDREXT_TRANSPORT_CC {
        hdrexts.transport_cc_audio = Some(hdrext.id);
      }
    }
  }
  else if description.media == "video" {
    for pt in description.payload_types.iter() {
      let slot = match pt.name.as_deref() {
        Some("H264") => Some((&mut h264, CodecName::H264)),
        Some("VP8") => Some((&mut vp8, CodecName::Vp8)),
        Some("VP9") => Some((&mut vp9, CodecName::Vp9)),
        Some("AV1") => Some((&mut av1, CodecName::Av1)),
        _ => None,
      };
      if let Some((slot, name)) = slot {
        *slot = Some(Codec {
          name,
          pt: pt.id,
          rtx_pt: None,
          rtcp_fbs: pt.rtcp_fbs.clone(),
        });
      }
    }
    for pt in description
      .payload_types
      .iter()
      .filter(|pt| pt.name.as_deref() == Some("rtx"))
    {
      for param in pt.parameters.iter().filter(|param| param.name == "apt") {
        let apt_pt: u8 = param.value.parse()?;
        for codec in [&mut h264, &mut vp8, &mut vp9, &mut av1].into_iter().flatten() {
          if codec.pt == apt_pt {
            codec.rtx_pt = Some(pt.id);
          }
        }
      }
    }
    for hdrext in description.hdrexts.iter() {
      if hdrext.uri == RTP_HDREXT_TRANSPORT_CC {
        hdrexts.transport_cc_video = Some(hdrext.id);
      }
    }
  }
  else {
    debug!("skipping media: {}", description.media);
    return Ok(None);
  }

  Ok(Some(ParsedRtpDescription {
    codecs: [opus, h264, vp8, vp9, av1].into_iter().flatten().collect(),
    hdrexts,
  }))
}
