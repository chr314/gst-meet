use std::collections::{BTreeMap, HashMap};

use anyhow::{bail, Context, Result};
use jid::{FullJid, Jid};
use jitsi_xmpp_parsers::{
  jingle::{Action, Content, Description, Jingle, Transport},
  jingle_dtls_srtp::Fingerprint,
  jingle_ice_udp::Transport as IceUdpTransport,
  jingle_rtp::{Description as RtpDescription, RtcpMux},
  jingle_ssma::{self, Parameter, Semantics, SsrcInfo},
};
use minidom::Element;
use tracing::{debug, warn};
use uuid::Uuid;
use xmpp_parsers::{
  hashes::Algo,
  jingle::{ContentId, Creator, Reason, ReasonElement, Senders, SessionId},
  jingle_dtls_srtp::Setup,
  jingle_grouping::{self, Content as GroupContent},
  jingle_ice_udp::Candidate,
  jingle_rtp::{self, PayloadType},
  jingle_rtp_hdrext::RtpHdrext,
};

use crate::{
  conference::ReceiveMode,
  pipeline::{
    self, participant_id_for_owner, Codec, CodecName, Fingerprint as OfferFingerprint, HdrextIds,
    LocalIceCandidate, LocalSsrcs, LocalTransport, RemoteIce, RemoteSourceDesc, SessionOffer,
    RTP_HDREXT_SSRC_AUDIO_LEVEL, RTP_HDREXT_TRANSPORT_CC,
  },
  source::{MediaType, RemoteSource},
  xmpp::ns::{JITSI_FOCUS, JITSI_JITMEET},
};

#[derive(Debug, Clone)]
pub(crate) struct IceParts {
  pub(crate) ufrag: String,
  pub(crate) pwd: String,
  pub(crate) candidates: Vec<Candidate>,
  pub(crate) fingerprint: OfferFingerprint,
  pub(crate) setup: Setup,
  pub(crate) colibri_ws_url: Option<String>,
}

/// What a session-initiate or a transport-replace of the focus offers, in the form we keep: a transport-replace
/// starts a new media session from the codecs of the offer that came first and the new transport.
#[derive(Clone)]
pub(crate) struct OfferParts {
  pub(crate) sid: String,
  pub(crate) initiator: Jid,
  pub(crate) bridge_session_id: Option<String>,
  pub(crate) codecs: Vec<Codec>,
  pub(crate) hdrexts: HdrextIds,
  /// The audio and video contents; the answer has the same.
  pub(crate) content_names: Vec<ContentId>,
  pub(crate) sources: Vec<RemoteSourceDesc>,
  pub(crate) ice: IceParts,
}

impl OfferParts {
  /// What the media layer is given; `sources` are those of the conference, not necessarily the ones of the offer.
  pub(crate) fn session_offer(&self, sources: Vec<RemoteSourceDesc>) -> SessionOffer {
    SessionOffer {
      sid: self.sid.clone(),
      bridge_session_id: self.bridge_session_id.clone(),
      codecs: self.codecs.clone(),
      hdrexts: self.hdrexts,
      sources,
      ice: RemoteIce {
        ufrag: self.ice.ufrag.clone(),
        pwd: self.ice.pwd.clone(),
        candidates: self.ice.candidates.clone(),
        fingerprint: self.ice.fingerprint.clone(),
        setup: self.ice.setup.clone(),
      },
    }
  }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionRef {
  pub(crate) sid: String,
  pub(crate) initiator: Option<Jid>,
  pub(crate) bridge_session_id: Option<String>,
}

impl SessionRef {
  pub(crate) fn of(jingle: &Jingle) -> Self {
    Self {
      sid: jingle.sid.0.clone(),
      initiator: jingle.initiator.clone(),
      bridge_session_id: bridge_session_id(jingle),
    }
  }
}

impl SessionRef {
  /// The session after a transport-replace: it keeps its sid, and the bridge session is the one the replace names.
  pub(crate) fn replaced(&self, replace: &Jingle) -> Self {
    Self {
      sid: self.sid.clone(),
      initiator: replace.initiator.clone().or_else(|| self.initiator.clone()),
      bridge_session_id: bridge_session_id(replace).or_else(|| self.bridge_session_id.clone()),
    }
  }
}

impl From<&OfferParts> for SessionRef {
  fn from(offer: &OfferParts) -> Self {
    Self {
      sid: offer.sid.clone(),
      initiator: Some(offer.initiator.clone()),
      bridge_session_id: offer.bridge_session_id.clone(),
    }
  }
}

pub(crate) struct OfferConfig<'a> {
  pub(crate) video_codecs: &'a [String],
  pub(crate) receive_mode: ReceiveMode,
}

/// The `id` of the `<bridge-session/>` Jicofo puts in its offers: its name for our allocation on the bridge.
pub(crate) fn bridge_session_id(jingle: &Jingle) -> Option<String> {
  jingle
    .other
    .iter()
    .find(|element| element.is("bridge-session", JITSI_FOCUS))
    .and_then(|element| element.attr("id"))
    .map(ToOwned::to_owned)
}

/// The offer as the media layer wants it: the codecs we can receive, the sources and the remote ICE and DTLS
/// parameters. A transport-replace that leaves out the descriptions gets the codecs of `prior`, the offer it
/// replaces.
pub(crate) fn parse_offer(jingle: &Jingle, config: &OfferConfig, prior: Option<&OfferParts>) -> Result<OfferParts> {
  let mut codecs = vec![];
  let mut hdrexts = HdrextIds::default();
  let mut content_names = vec![];
  let mut ice_transport = None;

  for content in &jingle.contents {
    if let Some(Description::Rtp(description)) = &content.description {
      if let Some(parsed) = pipeline::parse_rtp_description(description)? {
        codecs.extend(parsed.codecs);
        hdrexts.audio_level = hdrexts.audio_level.or(parsed.hdrexts.audio_level);
        hdrexts.transport_cc_audio = hdrexts.transport_cc_audio.or(parsed.hdrexts.transport_cc_audio);
        hdrexts.transport_cc_video = hdrexts.transport_cc_video.or(parsed.hdrexts.transport_cc_video);
        content_names.push(content.name.clone());
      }
    }
    if let Some(Transport::IceUdp(transport)) = &content.transport {
      ice_transport = Some(transport);
    }
  }

  if content_names.is_empty() {
    let prior = prior.context("the offer has no audio or video content")?;
    codecs = prior.codecs.clone();
    hdrexts = prior.hdrexts;
    content_names = prior.content_names.clone();
  }
  else {
    codecs.retain(|codec| {
      if codec.is_audio() {
        return true;
      }
      if !config.video_codecs.iter().any(|name| codec.is_codec(name.as_str())) {
        debug!("dropping unconfigured receive codec {}", codec.encoding_name());
        return false;
      }
      if !codec.receive_supported(config.receive_mode) {
        warn!(
          "dropping receive codec {} because local GStreamer support is incomplete",
          codec.encoding_name()
        );
        return false;
      }
      true
    });
    for name in &content_names {
      match name.0.as_str() {
        "audio" if !codecs.iter().any(|codec| codec.name == CodecName::Opus) => {
          bail!("the offer has no opus payload type")
        },
        "video" if !codecs.iter().any(|codec| codec.is_video()) => {
          bail!("none of the video codecs {:?} is offered and receivable", config.video_codecs)
        },
        _ => {},
      }
    }
  }

  let ice_transport = ice_transport.context("missing ICE transport")?;
  let (fingerprint, setup) = pipeline::pick_fingerprint(&ice_transport.fingerprints)?;
  debug!(
    "remote DTLS fingerprint: algo={:?} setup={:?} bytes={}",
    fingerprint.algo,
    setup,
    fingerprint.value.len()
  );

  Ok(OfferParts {
    sid: jingle.sid.0.clone(),
    initiator: jingle
      .initiator
      .clone()
      .or_else(|| prior.map(|prior| prior.initiator.clone()))
      .context("the offer has no initiator")?,
    bridge_session_id: bridge_session_id(jingle).or_else(|| prior.and_then(|prior| prior.bridge_session_id.clone())),
    codecs,
    hdrexts,
    content_names,
    sources: sources_from_jingle(jingle)?,
    ice: IceParts {
      ufrag: ice_transport.ufrag.clone().context("missing ICE ufrag")?,
      pwd: ice_transport.pwd.clone().context("missing ICE pwd")?,
      candidates: ice_transport.candidates.clone(),
      fingerprint,
      setup: setup.unwrap_or(Setup::Actpass),
      colibri_ws_url: ice_transport.web_sockets.first().map(|ws| ws.url.clone()),
    },
  })
}

/// The sources of every audio and video description, with the RTX SSRC of those that have one. The RTX SSRCs are
/// not sources of their own.
pub(crate) fn sources_from_jingle(jingle: &Jingle) -> Result<Vec<RemoteSourceDesc>> {
  let mut sources = vec![];
  for content in &jingle.contents {
    let Some(Description::Rtp(description)) = &content.description else {
      continue;
    };
    let rtx_ssrcs = rtx_ssrcs(description);
    for ssrc in &description.ssrcs {
      if rtx_ssrcs.values().any(|rtx| *rtx == ssrc.id) {
        continue;
      }
      let owner = ssrc.info.as_ref().context("missing ssrc-info")?.owner.clone();
      sources.push(RemoteSourceDesc {
        source: RemoteSource {
          participant_id: participant_id_for_owner(owner)?,
          ssrc: ssrc.id,
          media_type: MediaType::classify(&description.media, ssrc.video_type.as_deref()),
          source_name: ssrc.name.clone(),
        },
        rtx_ssrc: rtx_ssrcs.get(&ssrc.id).copied(),
      });
    }
  }
  Ok(sources)
}

/// The SSRCs a source-remove names. Unlike a source-add it need not say who owns them.
pub(crate) fn removed_ssrcs(jingle: &Jingle) -> Vec<u32> {
  jingle
    .contents
    .iter()
    .filter_map(|content| match &content.description {
      Some(Description::Rtp(description)) => Some(description),
      _ => None,
    })
    .flat_map(|description| {
      let rtx_ssrcs = rtx_ssrcs(description);
      description
        .ssrcs
        .iter()
        .map(|ssrc| ssrc.id)
        .filter(move |ssrc| !rtx_ssrcs.values().any(|rtx| rtx == ssrc))
    })
    .collect()
}

/// The FID groups of a description: the RTX SSRC of each primary SSRC.
fn rtx_ssrcs(description: &RtpDescription) -> HashMap<u32, u32> {
  description
    .ssrc_groups
    .iter()
    .filter(|group| group.semantics == Semantics::Fid)
    .filter_map(|group| match group.sources.as_slice() {
      [primary, rtx] => Some((primary.id, rtx.id)),
      _ => None,
    })
    .collect()
}

pub(crate) fn local_candidate_to_xmpp(c: &LocalIceCandidate) -> Candidate {
  Candidate {
    component: c.component_id as u8,
    foundation: c.foundation.clone(),
    generation: 0,
    id: Uuid::new_v4().to_string(),
    ip: c.addr.ip(),
    port: c.addr.port(),
    priority: c.priority,
    protocol: "udp".to_owned(),
    type_: match c.candidate_type {
      nice_gst_meet::CandidateType::Host => xmpp_parsers::jingle_ice_udp::Type::Host,
      nice_gst_meet::CandidateType::PeerReflexive => xmpp_parsers::jingle_ice_udp::Type::Prflx,
      nice_gst_meet::CandidateType::ServerReflexive => xmpp_parsers::jingle_ice_udp::Type::Srflx,
      nice_gst_meet::CandidateType::Relayed => xmpp_parsers::jingle_ice_udp::Type::Relay,
      _ => xmpp_parsers::jingle_ice_udp::Type::Host,
    },
    rel_addr: c.rel_addr.map(|addr| addr.ip()),
    rel_port: c.rel_addr.map(|addr| addr.port()),
    network: None,
  }
}

fn local_ice_transport(local: &LocalTransport, candidates: Vec<Candidate>) -> IceUdpTransport {
  let mut transport = IceUdpTransport::new().with_fingerprint(Fingerprint {
    hash: Algo::Sha_256,
    setup: Some(local.setup.clone()),
    value: local.fingerprint.clone(),
  });
  transport.ufrag = Some(local.ufrag.clone());
  transport.pwd = Some(local.pwd.clone());
  transport.candidates = candidates;
  transport
}

pub(crate) struct AnswerParams<'a> {
  /// `SessionAccept`, or `TransportAccept` for a transport-replace.
  pub(crate) action: Action,
  pub(crate) offer: &'a OfferParts,
  pub(crate) responder: &'a FullJid,
  pub(crate) endpoint_id: &'a str,
  pub(crate) video_codecs: &'a [String],
  pub(crate) local_ssrcs: LocalSsrcs,
  pub(crate) local: &'a LocalTransport,
  pub(crate) candidates: &'a [LocalIceCandidate],
}

/// The session-accept: our sources, the codec we chose for each content and our transport. The transport-accept has
/// the transport only.
pub(crate) fn answer(params: AnswerParams) -> Result<Jingle> {
  let AnswerParams {
    action,
    offer,
    responder,
    endpoint_id,
    video_codecs,
    local_ssrcs,
    local,
    candidates,
  } = params;
  let candidates: Vec<Candidate> = candidates.iter().map(local_candidate_to_xmpp).collect();

  let mut jingle = Jingle::new(action.clone(), SessionId(offer.sid.clone()))
    .with_initiator(offer.initiator.clone())
    .with_responder(responder.clone().into());

  // The focus knows our sources and our codecs from the session-accept of the session whose transport this replaces.
  // It treats the sources of a transport-accept as new ones, finds our SSRCs used already (`SsrcAlreadyUsed`) and
  // answers bad-request. All it takes from the accept is the transport (`Participant.onSessionOrTransportAccept`).
  if action == Action::TransportAccept {
    for name in &offer.content_names {
      jingle = jingle.add_content(
        Content::new(Creator::Responder, name.clone())
          .with_senders(Senders::Both)
          .with_transport(local_ice_transport(local, candidates.clone())),
      );
    }
    return Ok(jingle);
  }

  for name in &offer.content_names {
    let is_audio = name.0 == "audio";
    let mut description = RtpDescription::new(name.0.clone());

    description.payload_types = if is_audio {
      let codec = offer
        .codecs
        .iter()
        .find(|codec| codec.name == CodecName::Opus)
        .context("no opus payload type in the offer")?;
      let mut pt = PayloadType::new(codec.pt, "opus".to_owned(), 48000, 2);
      pt.rtcp_fbs = codec.rtcp_fbs.clone();
      vec![pt]
    }
    else {
      let codec = video_codecs
        .iter()
        .find_map(|name| offer.codecs.iter().find(|codec| codec.is_codec(name.as_str())))
        .with_context(|| format!("unsupported video codec(s): {:?}", video_codecs))?;
      let mut pt = PayloadType::new(codec.pt, codec.encoding_name().to_owned(), 90000, 1);
      pt.rtcp_fbs = codec.rtcp_fbs.clone();
      let mut pts = vec![pt];
      if let Some(rtx_pt) = codec.rtx_pt {
        let mut rtx_pt = PayloadType::new(rtx_pt, "rtx".to_owned(), 90000, 1);
        rtx_pt.parameters = vec![jingle_rtp::Parameter {
          name: "apt".to_owned(),
          value: codec.pt.to_string(),
        }];
        // RTX PTs carry only `a=fmtp:<pt> apt=<primary>`; no rtcp-fb.
        // See lib-jitsi-meet modules/sdp/SampleSdpStrings.ts:125-140.
        pts.push(rtx_pt);
      }
      pts
    };

    description.rtcp_mux = Some(RtcpMux);

    let mslabel = format!("{}-{}-0-1", endpoint_id, name.0);
    let label = Uuid::new_v4().to_string();

    description.ssrc = Some(if is_audio { local_ssrcs.audio } else { local_ssrcs.video }.to_string());

    description.ssrcs = if is_audio {
      vec![jingle_ssma::Source::new(
        local_ssrcs.audio,
        Some(format!("{endpoint_id}-a0")),
        None,
      )]
    }
    else {
      let source_name = format!("{endpoint_id}-v0");
      vec![
        jingle_ssma::Source::new(local_ssrcs.video, Some(source_name.clone()), Some("camera".into())),
        jingle_ssma::Source::new(local_ssrcs.video_rtx, Some(source_name), Some("camera".into())),
      ]
    };
    for ssrc in description.ssrcs.iter_mut() {
      ssrc.parameters.push(Parameter {
        name: "msid".to_owned(),
        value: Some(format!("{} {}", mslabel, label)),
      });
    }

    description.ssrc_groups = if is_audio {
      vec![]
    }
    else {
      vec![jingle_ssma::Group {
        semantics: Semantics::Fid,
        sources: vec![
          jingle_ssma::Source::new(local_ssrcs.video, None, None),
          jingle_ssma::Source::new(local_ssrcs.video_rtx, None, None),
        ],
      }]
    };

    if is_audio {
      if let Some(hdrext) = offer.hdrexts.audio_level {
        description
          .hdrexts
          .push(RtpHdrext::new(hdrext, RTP_HDREXT_SSRC_AUDIO_LEVEL.to_owned()));
      }
      if let Some(hdrext) = offer.hdrexts.transport_cc_audio {
        description
          .hdrexts
          .push(RtpHdrext::new(hdrext, RTP_HDREXT_TRANSPORT_CC.to_owned()));
      }
    }
    else if let Some(hdrext) = offer.hdrexts.transport_cc_video {
      description
        .hdrexts
        .push(RtpHdrext::new(hdrext, RTP_HDREXT_TRANSPORT_CC.to_owned()));
    }

    jingle = jingle.add_content(
      Content::new(Creator::Responder, name.clone())
        .with_senders(Senders::Both)
        .with_description(description)
        .with_transport(local_ice_transport(local, candidates.clone())),
    );
  }

  Ok(jingle.set_group(jingle_grouping::Group {
    semantics: jingle_grouping::Semantics::Bundle,
    contents: offer
      .content_names
      .iter()
      .map(|name| GroupContent::new(name.0.as_str()))
      .collect(),
  }))
}

pub(crate) fn transport_reject(replace: &Jingle) -> Jingle {
  let mut jingle = Jingle::new(Action::TransportReject, replace.sid.clone());
  jingle.initiator = replace.initiator.clone();
  for content in &replace.contents {
    jingle = jingle.add_content(
      Content::new(Creator::Responder, content.name.clone())
        .with_senders(Senders::Both)
        .with_transport(IceUdpTransport::new()),
    );
  }
  jingle
}

pub(crate) fn trickle_candidate(
  offer: &OfferParts,
  local: &LocalTransport,
  candidate: &LocalIceCandidate,
) -> Jingle {
  let content_name = offer
    .content_names
    .first()
    .cloned()
    .unwrap_or_else(|| ContentId("audio".to_owned()));
  Jingle::new(Action::TransportInfo, SessionId(offer.sid.clone())).add_content(
    Content::new(Creator::Responder, content_name)
      .with_senders(Senders::Both)
      .with_transport(local_ice_transport(local, vec![local_candidate_to_xmpp(candidate)])),
  )
}

fn bridge_session_element(id: Option<&str>, restart: Option<bool>) -> Element {
  let mut element = Element::builder("bridge-session", JITSI_FOCUS);
  if let Some(id) = id {
    element = element.attr(::minidom::rxml::xml_ncname!("id").to_owned(), id);
  }
  if let Some(restart) = restart {
    element = element.attr(::minidom::rxml::xml_ncname!("restart").to_owned(), restart.to_string());
  }
  element.build()
}

/// Tells the focus that ICE failed, with the bridge session it failed for; Jicofo then starts a new session.
pub(crate) fn ice_failed(session: &SessionRef) -> Jingle {
  let mut jingle = Jingle::new(Action::SessionInfo, SessionId(session.sid.clone()));
  jingle.initiator = session.initiator.clone();
  jingle.other = vec![Element::builder("ice-state", JITSI_FOCUS).append("failed").build()];
  if let Some(id) = &session.bridge_session_id {
    jingle.other.push(bridge_session_element(Some(id), None));
  }
  jingle
}

/// Ends the session. `restart` asks the focus for a new one.
pub(crate) fn session_terminate(session: &SessionRef, reason: Reason, text: Option<&str>, restart: bool) -> Jingle {
  let mut jingle = Jingle::new(Action::SessionTerminate, SessionId(session.sid.clone())).set_reason(ReasonElement {
    reason,
    texts: text
      .map(|text| (String::new(), text.to_owned()))
      .into_iter()
      .collect::<BTreeMap<_, _>>(),
  });
  jingle.initiator = session.initiator.clone();
  if session.bridge_session_id.is_some() || restart {
    jingle.other = vec![bridge_session_element(session.bridge_session_id.as_deref(), Some(restart))];
  }
  jingle
}

/// Expands Jicofo's compact `<json-message xmlns='http://jitsi.org/jitmeet'>` sources into `<source>` and
/// `<ssrc-group>` children of the audio and video descriptions, as lib-jitsi-meet's `expandSourcesFromJson` does.
/// Jicofo sends sources only this way because our disco-info has `http://jitsi.org/json-encoded-sources`.
pub(crate) fn expand_json_sources(jingle: &mut Jingle) {
  let mut payloads = Vec::new();
  jingle.other.retain(|el| {
    if el.is("json-message", JITSI_JITMEET) {
      payloads.push(el.text());
      false
    } else {
      true
    }
  });

  for text in payloads {
    let value: serde_json::Value = match serde_json::from_str(&text) {
      Ok(v) => v,
      Err(e) => {
        warn!("ignoring malformed json-message: {:?}", e);
        continue;
      },
    };
    let Some(sources) = value.get("sources").and_then(|s| s.as_object()) else {
      continue;
    };

    for (owner, owner_sources) in sources {
      let Some(arr) = owner_sources.as_array() else {
        continue;
      };
      let empty: Vec<serde_json::Value> = Vec::new();
      let video_sources = arr.first().and_then(|v| v.as_array()).unwrap_or(&empty);
      let video_groups = arr.get(1).and_then(|v| v.as_array()).unwrap_or(&empty);
      let audio_sources = arr.get(2).and_then(|v| v.as_array()).unwrap_or(&empty);
      let audio_groups = arr.get(3).and_then(|v| v.as_array()).unwrap_or(&empty);

      if !video_sources.is_empty() || !video_groups.is_empty() {
        let desc = ensure_rtp_description(jingle, "video");
        append_sources(desc, owner, video_sources, true);
        append_groups(desc, video_groups);
      }
      if !audio_sources.is_empty() || !audio_groups.is_empty() {
        let desc = ensure_rtp_description(jingle, "audio");
        append_sources(desc, owner, audio_sources, false);
        append_groups(desc, audio_groups);
      }
    }
  }
}

fn ensure_rtp_description<'a>(jingle: &'a mut Jingle, media: &str) -> &'a mut RtpDescription {
  let idx = match jingle.contents.iter().position(|c| c.name.0 == media) {
    Some(i) => i,
    None => {
      let content = Content::new(Creator::Initiator, ContentId(media.to_owned()))
        .with_senders(Senders::Both)
        .with_description(RtpDescription::new(media.to_owned()));
      jingle.contents.push(content);
      jingle.contents.len() - 1
    },
  };
  let content = &mut jingle.contents[idx];
  if !matches!(content.description, Some(Description::Rtp(_))) {
    content.description = Some(Description::Rtp(RtpDescription::new(media.to_owned())));
  }
  match content.description.as_mut().expect("description set above") {
    Description::Rtp(desc) => desc,
    Description::Unknown(_) => unreachable!("description replaced above"),
  }
}

fn append_sources(desc: &mut RtpDescription, owner: &str, sources: &[serde_json::Value], is_video: bool) {
  for src in sources {
    let Some(ssrc) = parse_ssrc(src.get("s")) else {
      warn!("json-message source missing valid ssrc: {:?}", src);
      continue;
    };
    let name = src.get("n").and_then(|v| v.as_str()).map(str::to_owned);
    // lib-jitsi-meet: video source with `v` field → desktop; otherwise camera.
    let video_type = if is_video {
      Some(if src.get("v").is_some() {
        "desktop".to_owned()
      } else {
        "camera".to_owned()
      })
    } else {
      None
    };
    let mut source = jingle_ssma::Source::new(ssrc, name, video_type);
    if let Some(msid) = src.get("m").and_then(|v| v.as_str()) {
      source.parameters.push(Parameter {
        name: "msid".to_owned(),
        value: Some(msid.to_owned()),
      });
    }
    source.info = Some(SsrcInfo {
      owner: owner.to_owned(),
    });
    desc.ssrcs.push(source);
  }
}

fn append_groups(desc: &mut RtpDescription, groups: &[serde_json::Value]) {
  for grp in groups {
    let Some(arr) = grp.as_array() else { continue };
    if arr.is_empty() {
      continue;
    }
    let Some(sem_str) = arr[0].as_str() else { continue };
    let semantics = match sem_str {
      "f" => Semantics::Fid,
      "s" => Semantics::Sim,
      other => {
        warn!("ignoring unknown ssrc-group semantics {:?}", other);
        continue;
      },
    };
    let mut sources = Vec::new();
    for v in &arr[1..] {
      let Some(ssrc) = parse_ssrc(Some(v)) else {
        warn!("ignoring invalid ssrc in ssrc-group: {:?}", v);
        continue;
      };
      sources.push(jingle_ssma::Source::new(ssrc, None, None));
    }
    desc.ssrc_groups.push(jingle_ssma::Group { semantics, sources });
  }
}

fn parse_ssrc(value: Option<&serde_json::Value>) -> Option<u32> {
  match value? {
    serde_json::Value::String(s) => s.parse().ok(),
    serde_json::Value::Number(n) => n.as_u64().and_then(|u| u32::try_from(u).ok()),
    _ => None,
  }
}

#[cfg(test)]
mod tests {
  use xmpp_parsers::{jingle::Reason, ns};

  use super::*;
  use crate::{pipeline::HashAlgo, tests::xml::*};

  fn jingle_of(iq: &str) -> Jingle {
    let iq = parse(iq);
    Jingle::try_from(iq.get_child("jingle", ns::JINGLE).unwrap().clone()).unwrap()
  }

  fn config() -> OfferConfig<'static> {
    gstreamer::init().unwrap();
    OfferConfig {
      video_codecs: &[],
      receive_mode: ReceiveMode::Encoded,
    }
  }

  fn parse_with(video_codecs: &'static [String], iq: &str) -> Result<OfferParts> {
    let mut config = config();
    config.video_codecs = video_codecs;
    parse_offer(&jingle_of(iq), &config, None)
  }

  fn vp8() -> &'static [String] {
    static VP8: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    VP8.get_or_init(|| vec!["vp8".to_owned()])
  }

  #[test]
  fn parses_an_offer_of_the_focus() {
    let offer = parse_with(vp8(), &session_initiate("1", &Offer::default())).unwrap();
    assert_eq!(offer.sid, "sid-1");
    assert_eq!(offer.bridge_session_id.as_deref(), Some("bridge-1"));
    assert_eq!(offer.initiator.to_string(), FOCUS_ACCOUNT);
    assert_eq!(
      offer.content_names.iter().map(|name| name.0.as_str()).collect::<Vec<_>>(),
      ["audio", "video"]
    );
    assert_eq!(offer.hdrexts.audio_level, Some(1));
    assert_eq!(offer.hdrexts.transport_cc_audio, Some(5));
    assert_eq!(offer.hdrexts.transport_cc_video, Some(5));
    let names: Vec<_> = offer.codecs.iter().map(|codec| (codec.encoding_name(), codec.pt, codec.rtx_pt)).collect();
    assert_eq!(names, [("opus", 111, None), ("VP8", 100, Some(96))]);

    // The RTX SSRC belongs to its source and is no source of its own.
    let sources: Vec<_> = offer
      .sources
      .iter()
      .map(|desc| (desc.source.ssrc, desc.source.media_type, desc.source.source_name.clone(), desc.rtx_ssrc))
      .collect();
    assert_eq!(
      sources,
      [
        (1000, MediaType::Audio, Some("bob-a0".to_owned()), None),
        (2000, MediaType::Video, Some("bob-v0".to_owned()), Some(2001)),
      ]
    );
    assert_eq!(offer.sources[0].source.participant_id.as_deref(), Some("bob"));

    assert_eq!(offer.ice.ufrag, "focusufrag");
    assert_eq!(offer.ice.candidates.len(), 1);
    assert_eq!(offer.ice.fingerprint.algo, HashAlgo::Sha256);
    assert_eq!(offer.ice.fingerprint.value, vec![0xAB; 32]);
    assert_eq!(offer.ice.setup, Setup::Actpass);
    assert_eq!(offer.ice.colibri_ws_url, None);
    let with_web_socket = Offer {
      web_socket: true,
      ..Offer::default()
    };
    let offer = parse_with(vp8(), &session_initiate("1", &with_web_socket)).unwrap();
    assert_eq!(offer.ice.colibri_ws_url.as_deref(), Some("wss://jvb.example/colibri-ws/jvb1/abc"));
  }

  #[test]
  fn a_missing_bridge_session_is_none() {
    let offer = Offer {
      bridge_session_id: None,
      ..Offer::default()
    };
    assert_eq!(parse_with(vp8(), &session_initiate("1", &offer)).unwrap().bridge_session_id, None);
  }

  #[test]
  fn an_offer_without_a_fingerprint_is_refused() {
    let iq = session_initiate("1", &Offer::default()).replace("<fingerprint", "<unrelated").replace("</fingerprint>", "</unrelated>");
    let error = parse_with(vp8(), &iq).err().expect("the offer is refused");
    assert!(format!("{error:#}").contains("fingerprint"), "{error:#}");
  }

  #[test]
  fn an_offer_with_none_of_our_video_codecs_is_refused() {
    static H264: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    let h264 = H264.get_or_init(|| vec!["h264".to_owned()]);
    let error = parse_with(h264, &session_initiate("1", &Offer::default()))
      .err()
      .expect("the offer is refused");
    assert!(format!("{error:#}").contains("video codecs"), "{error:#}");
  }

  #[test]
  fn a_transport_replace_without_descriptions_keeps_the_codecs() {
    let prior = parse_with(vp8(), &session_initiate("1", &Offer::default())).unwrap();
    let replace = jingle_of(&transport_replace("2", &Offer::default()));
    let mut replace = replace;
    for content in &mut replace.contents {
      content.description = None;
    }
    replace.other.clear();
    let offer = parse_offer(&replace, &config(), Some(&prior)).unwrap();
    assert_eq!(offer.codecs.len(), prior.codecs.len());
    assert_eq!(offer.content_names, prior.content_names);
    assert!(offer.sources.is_empty());
    assert_eq!(offer.bridge_session_id.as_deref(), Some("bridge-1"));
  }

  #[test]
  fn session_info_for_a_failed_ice_names_the_bridge_session() {
    let offer = parse_with(vp8(), &session_initiate("1", &Offer::default())).unwrap();
    let element = Element::from(ice_failed(&SessionRef::from(&offer)));
    assert_eq!(
      String::from(&element),
      "<jingle xmlns='urn:xmpp:jingle:1' action='session-info' initiator='focus@auth.meet.example/focus' sid='sid-1'>\
       <ice-state xmlns='http://jitsi.org/protocol/focus'>failed</ice-state>\
       <bridge-session xmlns='http://jitsi.org/protocol/focus' id='bridge-1'/></jingle>"
    );
  }

  #[test]
  fn session_terminate_asks_for_a_restart_with_the_bridge_session() {
    let offer = parse_with(vp8(), &session_initiate("1", &Offer::default())).unwrap();
    let session = SessionRef::from(&offer);
    let terminate = Element::from(session_terminate(&session, Reason::ConnectivityError, Some("DTLS failed"), true));
    assert!(terminate.has_child("reason", ns::JINGLE));
    let reason = terminate.get_child("reason", ns::JINGLE).unwrap();
    assert!(reason.has_child("connectivity-error", ns::JINGLE));
    assert_eq!(reason.get_child("text", ns::JINGLE).unwrap().text(), "DTLS failed");
    let bridge = terminate.get_child("bridge-session", JITSI_FOCUS).unwrap();
    assert_eq!(bridge.attr("id"), Some("bridge-1"));
    assert_eq!(bridge.attr("restart"), Some("true"));

    let leave = Element::from(session_terminate(&session, Reason::Success, None, false));
    assert_eq!(leave.get_child("bridge-session", JITSI_FOCUS).unwrap().attr("restart"), Some("false"));
    let unnamed = SessionRef {
      bridge_session_id: None,
      ..session
    };
    assert!(!Element::from(session_terminate(&unnamed, Reason::Success, None, false)).has_child("bridge-session", JITSI_FOCUS));
  }

  #[test]
  fn the_json_encoded_sources_are_expanded() {
    let mut jingle = jingle_of(&source_update("1", "source-add", "sid-1", "bob", 1, None));
    jingle.contents.clear();
    jingle.other = vec![parse(
      "<json-message xmlns='http://jitsi.org/jitmeet'>{\"sources\":{\"carol\":[\
         [{\"s\":3000,\"n\":\"carol-v0\",\"m\":\"msid track\"},{\"s\":3001,\"n\":\"carol-v0\"},{\"s\":3100,\"n\":\"carol-v1\",\"v\":\"d\"}],\
         [[\"f\",3000,3001]],\
         [{\"s\":\"4000\",\"n\":\"carol-a0\"}],[]]}}</json-message>",
    )];
    expand_json_sources(&mut jingle);
    assert!(jingle.other.is_empty());

    let sources: Vec<_> = sources_from_jingle(&jingle)
      .unwrap()
      .into_iter()
      .map(|desc| (desc.source.ssrc, desc.source.media_type, desc.source.source_name.unwrap(), desc.rtx_ssrc))
      .collect();
    assert_eq!(
      sources,
      [
        (3000, MediaType::Video, "carol-v0".to_owned(), Some(3001)),
        (3100, MediaType::VideoScreenshare, "carol-v1".to_owned(), None),
        (4000, MediaType::Audio, "carol-a0".to_owned(), None),
      ]
    );
    assert_eq!(removed_ssrcs(&jingle), [3000, 3100, 4000]);
  }

  #[test]
  fn a_source_remove_need_not_name_the_owner() {
    let mut jingle = jingle_of(&source_update("1", "source-remove", "sid-1", "bob", 1000, Some(2000)));
    for content in &mut jingle.contents {
      if let Some(Description::Rtp(description)) = &mut content.description {
        for ssrc in &mut description.ssrcs {
          ssrc.info = None;
        }
      }
    }
    assert!(sources_from_jingle(&jingle).is_err());
    assert_eq!(removed_ssrcs(&jingle), [1000, 2000]);
  }
}
