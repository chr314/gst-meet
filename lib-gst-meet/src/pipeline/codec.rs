use std::collections::HashMap;

use anyhow::{Context, Result};
use jitsi_xmpp_parsers::jingle_rtp::Description as RtpDescription;
use tracing::debug;
use xmpp_parsers::jingle_rtcp_fb::RtcpFb;

use crate::source::{MediaType, Source};

pub(super) const RTP_HDREXT_SSRC_AUDIO_LEVEL: &str =
  "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
pub(super) const RTP_HDREXT_TRANSPORT_CC: &str =
  "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01";

#[derive(Clone, PartialEq)]
pub(crate) enum CodecName {
  Opus,
  H264,
  Vp8,
  Vp9,
  Av1,
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
    if let Some(pt) = self.rtx_pt {
      pt == rtx_pt
    }
    else {
      false
    }
  }

  pub(crate) fn is_audio(&self) -> bool {
    self.name == CodecName::Opus
  }

  pub(crate) fn is_video(&self) -> bool {
    self.name != CodecName::Opus
  }

  pub(crate) fn is_codec(&self, name: &str) -> bool {
    match name {
      "h264" => self.name == CodecName::H264,
      "vp8" => self.name == CodecName::Vp8,
      "vp9" => self.name == CodecName::Vp9,
      "av1" => self.name == CodecName::Av1,
      _ => false,
    }
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
    match self.name {
      CodecName::Opus => "rtpopusdepay",
      CodecName::H264 => "rtph264depay",
      CodecName::Vp8 => "rtpvp8depay",
      CodecName::Vp9 => "rtpvp9depay",
      CodecName::Av1 => "rtpav1depay",
    }
  }

  pub(crate) fn decoder_name(&self) -> &'static str {
    match self.name {
      CodecName::Opus => "opusdec",
      CodecName::H264 => "avdec_h264",
      CodecName::Vp8 => "vp8dec",
      CodecName::Vp9 => "vp9dec",
      CodecName::Av1 => "av1dec",
    }
  }

  pub(crate) fn payloader_name(&self) -> &'static str {
    match self.name {
      CodecName::Opus => "rtpopuspay",
      CodecName::H264 => "rtph264pay",
      CodecName::Vp8 => "rtpvp8pay",
      CodecName::Vp9 => "rtpvp9pay",
      CodecName::Av1 => "rtpav1pay",
    }
  }
}

pub(crate) struct ParsedRtpDescription {
  pub(crate) codecs: Vec<Codec>,
  pub(crate) audio_hdrext_ssrc_audio_level: Option<u16>,
  pub(crate) audio_hdrext_transport_cc: Option<u16>,
  pub(crate) video_hdrext_transport_cc: Option<u16>,
}

pub(super) fn parse_rtp_description(
  description: &RtpDescription,
  remote_ssrc_map: &mut HashMap<u32, Source>,
) -> Result<Option<ParsedRtpDescription>> {
  let mut opus = None;
  let mut h264 = None;
  let mut vp8 = None;
  let mut vp9 = None;
  let mut av1 = None;
  let mut audio_hdrext_ssrc_audio_level = None;
  let mut audio_hdrext_transport_cc = None;
  let mut video_hdrext_transport_cc = None;

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
        audio_hdrext_ssrc_audio_level = Some(hdrext.id);
      }
      else if hdrext.uri == RTP_HDREXT_TRANSPORT_CC {
        audio_hdrext_transport_cc = Some(hdrext.id);
      }
    }
  }
  else if description.media == "video" {
    for pt in description.payload_types.iter() {
      if let Some(name) = &pt.name {
        match name.as_str() {
          "H264" => {
            h264 = Some(Codec {
              name: CodecName::H264,
              pt: pt.id,
              rtx_pt: None,
              rtcp_fbs: pt.rtcp_fbs.clone(),
            });
          },
          "VP8" => {
            vp8 = Some(Codec {
              name: CodecName::Vp8,
              pt: pt.id,
              rtx_pt: None,
              rtcp_fbs: pt.rtcp_fbs.clone(),
            });
          },
          "VP9" => {
            vp9 = Some(Codec {
              name: CodecName::Vp9,
              pt: pt.id,
              rtx_pt: None,
              rtcp_fbs: pt.rtcp_fbs.clone(),
            });
          },
          "AV1" => {
            av1 = Some(Codec {
              name: CodecName::Av1,
              pt: pt.id,
              rtx_pt: None,
              rtcp_fbs: pt.rtcp_fbs.clone(),
            });
          },
          _ => (),
        }
      }
    }
    for pt in description.payload_types.iter() {
      if let Some(name) = &pt.name {
        if name == "rtx" {
          for param in pt.parameters.iter() {
            if param.name == "apt" {
              let apt_pt: u8 = param.value.parse()?;
              if let Some(h264) = &mut h264 {
                if apt_pt == h264.pt {
                  h264.rtx_pt = Some(pt.id);
                }
              }
              if let Some(vp8) = &mut vp8 {
                if apt_pt == vp8.pt {
                  vp8.rtx_pt = Some(pt.id);
                }
              }
              if let Some(vp9) = &mut vp9 {
                if apt_pt == vp9.pt {
                  vp9.rtx_pt = Some(pt.id);
                }
              }
              if let Some(av1) = &mut av1 {
                if apt_pt == av1.pt {
                  av1.rtx_pt = Some(pt.id);
                }
              }
            }
          }
        }
      }
    }
    for hdrext in description.hdrexts.iter() {
      if hdrext.uri == RTP_HDREXT_TRANSPORT_CC {
        video_hdrext_transport_cc = Some(hdrext.id);
      }
    }
  }
  else {
    debug!("skipping media: {}", description.media);
    return Ok(None);
  }

  let codecs = [opus, h264, vp8, vp9, av1]
    .iter()
    .flatten()
    .cloned()
    .collect();

  for ssrc in &description.ssrcs {
    let owner = ssrc
      .info
      .as_ref()
      .context("missing ssrc-info")?
      .owner
      .clone();

    debug!("adding ssrc to remote_ssrc_map: {:?}", ssrc);
    remote_ssrc_map.insert(
      ssrc.id,
      Source {
        ssrc: ssrc.id,
        participant_id: super::participant_id_for_owner(owner)?,
        media_type: if description.media == "audio" {
          MediaType::Audio
        }
        else {
          MediaType::Video
        },
      },
    );
  }
  Ok(Some(ParsedRtpDescription {
    codecs,
    audio_hdrext_ssrc_audio_level,
    audio_hdrext_transport_cc,
    video_hdrext_transport_cc,
  }))
}
