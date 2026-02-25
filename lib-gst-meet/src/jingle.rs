use std::{collections::HashMap, fmt};

use anyhow::{bail, Context, Result};
use jitsi_xmpp_parsers::{
  jingle::{Action, Content, Description, Jingle, Transport},
  jingle_dtls_srtp::Fingerprint,
  jingle_ice_udp::Transport as IceUdpTransport,
  jingle_rtp::Description as RtpDescription,
  jingle_ssma::{self, Parameter},
};
use rand::RngExt;
use sha2::{Digest as _, Sha256};
use tokio::task::JoinHandle;
use tracing::{debug, warn};
use uuid::Uuid;
use xmpp_parsers::{
  hashes::Algo,
  iq::Iq,
  jingle::{Creator, Senders},
  jingle_dtls_srtp::Setup,
  jingle_grouping::{self, Content as GroupContent},
  jingle_rtp::{self, PayloadType, RtcpMux},
  jingle_rtp_hdrext::RtpHdrext,
  jingle_ssma::Semantics,
};

use crate::{
  colibri::ColibriChannel,
  conference::JitsiConference,
  pipeline::{CodecName, MediaPipeline},
  util::generate_id,
};

const RTP_HDREXT_SSRC_AUDIO_LEVEL: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
const RTP_HDREXT_TRANSPORT_CC: &str =
  "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01";

pub(crate) struct JingleSession {
  pub(crate) media_pipeline: MediaPipeline,
  pub(crate) accept_iq_id: Option<String>,
  pub(crate) colibri_url: Option<String>,
  pub(crate) colibri_channel: Option<ColibriChannel>,
  pub(crate) stats_handler_task: Option<JoinHandle<()>>,
}

impl fmt::Debug for JingleSession {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("JingleSession").finish()
  }
}

impl JingleSession {
  pub(crate) fn pipeline(&self) -> gstreamer::Pipeline {
    self.media_pipeline.pipeline()
  }

  pub(crate) fn audio_sink_element(&self) -> gstreamer::Element {
    self.media_pipeline.audio_sink_element()
  }

  pub(crate) fn video_sink_element(&self) -> gstreamer::Element {
    self.media_pipeline.video_sink_element()
  }

  pub(crate) fn pause_all_sinks(&self) {
    self.media_pipeline.pause_all_sinks();
  }

  pub(crate) async fn pipeline_stopped(self) -> Result<()> {
    self.media_pipeline.pipeline_stopped().await
  }

  pub(crate) async fn initiate(conference: &JitsiConference, jingle: Jingle) -> Result<Self> {
    let initiator = jingle
      .initiator
      .as_ref()
      .context("session-initiate with no initiator")?
      .clone();

    debug!("Received Jingle session-initiate from {}", initiator);

    let mut ice_transport = None;
    let mut codecs = vec![];
    let mut audio_hdrext_ssrc_audio_level = None;
    let mut audio_hdrext_transport_cc = None;
    let mut video_hdrext_transport_cc = None;

    let mut remote_ssrc_map = HashMap::new();

    for content in &jingle.contents {
      if let Some(Description::Rtp(description)) = &content.description {
        if let Some(description) =
          MediaPipeline::parse_rtp_description(description, &mut remote_ssrc_map)?
        {
          codecs.extend(description.codecs);
          audio_hdrext_ssrc_audio_level =
            audio_hdrext_ssrc_audio_level.or(description.audio_hdrext_ssrc_audio_level);
          audio_hdrext_transport_cc =
            audio_hdrext_transport_cc.or(description.audio_hdrext_transport_cc);
          video_hdrext_transport_cc =
            video_hdrext_transport_cc.or(description.video_hdrext_transport_cc);
        }
      }

      if let Some(Transport::IceUdp(transport)) = &content.transport {
        if let Some(fingerprint) = &transport.fingerprint {
          if fingerprint.hash != Algo::Sha_256 {
            bail!("unsupported fingerprint hash: {:?}", fingerprint.hash);
          }
        }
        ice_transport = Some(transport);
      }
    }

    let ice_transport = ice_transport.context("missing ICE transport")?;

    if let Some(remote_fingerprint) = &ice_transport.fingerprint {
      warn!(
        "Remote DTLS fingerprint (verification not implemented yet): {:?}",
        remote_fingerprint
      );
    }

    let dtls_key_pair = rcgen::KeyPair::generate()?;
    let mut dtls_cert_params = rcgen::CertificateParams::new(vec!["gst-meet".to_owned()])?;
    let dtls_cert = dtls_cert_params.self_signed(&dtls_key_pair)?;
    let fingerprint: Vec<u8> = Sha256::digest(dtls_cert.der()).to_vec();
    let fingerprint_str =
      itertools::join(fingerprint.iter().map(|byte| format!("{:X}", byte)), ":");
    let dtls_cert_pem = dtls_cert.pem();
    let dtls_private_key_pem = dtls_key_pair.serialize_pem();
    debug!("Local DTLS certificate:\n{}", dtls_cert_pem);
    debug!("Local DTLS fingerprint: {}", fingerprint_str);

    let audio_ssrc: u32 = rand::rng().random();
    let video_ssrc: u32 = rand::rng().random();
    let video_rtx_ssrc: u32 = rand::rng().random();

    debug!("audio SSRC: {}", audio_ssrc);
    debug!("video SSRC: {}", video_ssrc);
    debug!("video RTX SSRC: {}", video_rtx_ssrc);

    let media_pipeline = MediaPipeline::build(
      conference,
      ice_transport,
      codecs.clone(),
      audio_hdrext_ssrc_audio_level,
      audio_hdrext_transport_cc,
      video_hdrext_transport_cc,
      remote_ssrc_map,
      &dtls_cert_pem,
      &dtls_private_key_pem,
      audio_ssrc,
      video_ssrc,
      video_rtx_ssrc,
    )
    .await?;

    let (ice_local_ufrag, ice_local_pwd) = media_pipeline
      .ice_local_credentials()
      .context("no local ICE credentials")?;

    let local_candidates = media_pipeline.ice_local_candidates();
    debug!("local candidates: {:?}", local_candidates);

    debug!("building Jingle session-accept");
    let mut jingle_accept = Jingle::new(Action::SessionAccept, jingle.sid.clone())
      .with_initiator(
        jingle
          .initiator
          .as_ref()
          .context("jingle session-initiate with no initiator")?
          .clone(),
      )
      .with_responder(conference.jid.clone().into());

    for initiate_content in &jingle.contents {
      let mut description = RtpDescription::new(initiate_content.name.0.clone());

      description.payload_types = if initiate_content.name.0 == "audio" {
        let codec = codecs.iter().find(|codec| codec.name == CodecName::Opus);
        if let Some(codec) = codec {
          let mut pt = PayloadType::new(codec.pt, "opus".to_owned(), 48000, 2);
          pt.rtcp_fbs = codec.rtcp_fbs.clone();
          vec![pt]
        }
        else {
          bail!("no opus payload type in jingle session-initiate");
        }
      }
      else {
        let mut pts = vec![];
        let codec_name = conference.config.video_codec.as_str();
        let codec = codecs.iter().find(|codec| codec.is_codec(codec_name));
        if let Some(codec) = codec {
          let mut pt = PayloadType::new(codec.pt, codec.encoding_name().to_owned(), 90000, 1);
          pt.rtcp_fbs = codec.rtcp_fbs.clone();
          pts.push(pt);
          if let Some(rtx_pt) = codec.rtx_pt {
            let mut rtx_pt = PayloadType::new(rtx_pt, "rtx".to_owned(), 90000, 1);
            rtx_pt.parameters = vec![jingle_rtp::Parameter {
              name: "apt".to_owned(),
              value: codec.pt.to_string(),
            }];
            rtx_pt.rtcp_fbs = codec
              .rtcp_fbs
              .clone()
              .into_iter()
              .filter(|fb| fb.type_ != "transport-cc")
              .collect();
            pts.push(rtx_pt);
          }
        }
        else {
          bail!("unsupported video codec: {}", codec_name);
        }
        pts
      };

      description.rtcp_mux = Some(RtcpMux);

      let endpoint_id = conference.endpoint_id()?;

      let mslabel = format!("{}-{}-0-1", endpoint_id, initiate_content.name.0);
      let label = Uuid::new_v4().to_string();

      description.ssrc = Some(if initiate_content.name.0 == "audio" {
        audio_ssrc.to_string()
      }
      else {
        video_ssrc.to_string()
      });

      description.ssrcs = if initiate_content.name.0 == "audio" {
        vec![jingle_ssma::Source::new(
          audio_ssrc,
          Some(format!("{endpoint_id}-a0")),
          None,
        )]
      }
      else {
        let source_name = format!("{endpoint_id}-v0");
        vec![
          jingle_ssma::Source::new(video_ssrc, Some(source_name.clone()), Some("camera".into())),
          jingle_ssma::Source::new(video_rtx_ssrc, Some(source_name), Some("camera".into())),
        ]
      };

      for ssrc in description.ssrcs.iter_mut() {
        ssrc.parameters.push(Parameter {
          name: "msid".to_owned(),
          value: Some(format!("{} {}", mslabel, label)),
        });
      }

      description.ssrc_groups = if initiate_content.name.0 == "audio" {
        vec![]
      }
      else {
        vec![jingle_ssma::Group {
          semantics: Semantics::Fid,
          sources: vec![
            jingle_ssma::Source::new(video_ssrc, None, None),
            jingle_ssma::Source::new(video_rtx_ssrc, None, None),
          ],
        }]
      };

      if initiate_content.name.0 == "audio" {
        if let Some(hdrext) = audio_hdrext_ssrc_audio_level {
          description.hdrexts.push(RtpHdrext::new(
            hdrext,
            RTP_HDREXT_SSRC_AUDIO_LEVEL.to_owned(),
          ));
        }
        if let Some(hdrext) = audio_hdrext_transport_cc {
          description
            .hdrexts
            .push(RtpHdrext::new(hdrext, RTP_HDREXT_TRANSPORT_CC.to_owned()));
        }
      }
      else if initiate_content.name.0 == "video" {
        if let Some(hdrext) = video_hdrext_transport_cc {
          description
            .hdrexts
            .push(RtpHdrext::new(hdrext, RTP_HDREXT_TRANSPORT_CC.to_owned()));
        }
      }

      let mut transport = IceUdpTransport::new().with_fingerprint(Fingerprint {
        hash: Algo::Sha_256,
        setup: Some(Setup::Active),
        value: fingerprint.clone(),
      });
      transport.ufrag = Some(ice_local_ufrag.clone());
      transport.pwd = Some(ice_local_pwd.clone());
      transport.candidates = vec![];
      for c in &local_candidates {
        let addr = c.addr();
        let foundation = c.foundation()?;
        transport
          .candidates
          .push(xmpp_parsers::jingle_ice_udp::Candidate {
            component: c.component_id() as u8,
            foundation: foundation.to_owned(),
            generation: 0,
            id: Uuid::new_v4().to_string(),
            ip: addr.ip(),
            port: addr.port(),
            priority: c.priority(),
            protocol: "udp".to_owned(),
            type_: match c.type_() {
              nice_gst_meet::CandidateType::Host => {
                xmpp_parsers::jingle_ice_udp::Type::Host
              },
              nice_gst_meet::CandidateType::PeerReflexive => {
                xmpp_parsers::jingle_ice_udp::Type::Prflx
              },
              nice_gst_meet::CandidateType::ServerReflexive => {
                xmpp_parsers::jingle_ice_udp::Type::Srflx
              },
              nice_gst_meet::CandidateType::Relayed => {
                xmpp_parsers::jingle_ice_udp::Type::Relay
              },
              other => bail!("unsupported candidate type: {:?}", other),
            },
            rel_addr: None,
            rel_port: None,
            network: None,
          });
      }

      jingle_accept = jingle_accept.add_content(
        Content::new(Creator::Responder, initiate_content.name.clone())
          .with_senders(Senders::Both)
          .with_description(description)
          .with_transport(transport),
      );
    }

    jingle_accept = jingle_accept.set_group(jingle_grouping::Group {
      semantics: jingle_grouping::Semantics::Bundle,
      contents: vec![GroupContent::new("video"), GroupContent::new("audio")],
    });

    let accept_iq_id = generate_id();
    let session_accept_iq = Iq::from_set(accept_iq_id.clone(), jingle_accept)
      .with_to(conference.focus_jid_in_muc()?.into())
      .with_from(conference.jid.clone().into());
    conference.xmpp_tx.send(session_accept_iq.into()).await?;

    Ok(Self {
      media_pipeline,
      accept_iq_id: Some(accept_iq_id),
      colibri_url: ice_transport.web_socket.clone().map(|ws| ws.url),
      colibri_channel: None,
      stats_handler_task: None,
    })
  }

  pub(crate) async fn source_add(&mut self, jingle: Jingle) -> Result<()> {
    self.media_pipeline.source_add(&jingle).await
  }

  pub(crate) fn source_remove(&mut self, jingle: Jingle) -> Result<()> {
    self.media_pipeline.source_remove(&jingle)
  }
}
