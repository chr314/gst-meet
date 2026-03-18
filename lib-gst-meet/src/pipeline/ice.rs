use std::net::SocketAddr;

use anyhow::{bail, Context, Result};
use jitsi_xmpp_parsers::jingle_ice_udp::Transport as IceUdpTransport;
use nice_gst_meet as nice;
use tokio::net::lookup_host;
use tracing::{debug, warn};
use xmpp_parsers::jingle_ice_udp;

use crate::conference::JitsiConference;

const DEFAULT_STUN_PORT: u16 = 3478;
const DEFAULT_TURNS_PORT: u16 = 5349;

pub(super) async fn setup_ice(
  conference: &JitsiConference,
  transport: &IceUdpTransport,
) -> Result<(nice::Agent, u32, u32)> {
  let ice_agent = nice::Agent::new(&conference.glib_main_context, nice::Compatibility::Rfc5245);
  ice_agent.set_ice_tcp(false);
  ice_agent.set_upnp(false);
  let ice_stream_id = ice_agent.add_stream(1);
  let ice_component_id = 1;

  let maybe_stun = conference.external_services.iter().find(|svc| svc.r#type == "stun");

  let stun_addr = if let Some(stun) = maybe_stun {
    debug!("resolving address for STUN server: {}", stun.host);
    lookup_host(format!("{}:{}", stun.host, stun.port.unwrap_or(DEFAULT_STUN_PORT)))
      .await
      .context("failed to resolve STUN server hostname")?
      .next()
  } else {
    None
  };
  debug!("STUN address: {:?}", stun_addr);

  if let Some((stun_addr, stun_port)) = stun_addr.map(|sa| (sa.ip().to_string(), sa.port())) {
    ice_agent.set_stun_server(Some(&stun_addr));
    ice_agent.set_stun_server_port(stun_port as u32);
  }

  let maybe_turn = conference.external_services.iter().find(|svc| svc.r#type == "turns");

  if let Some(turn_server) = maybe_turn {
    let maybe_addr = lookup_host(format!(
      "{}:{}",
      turn_server.host,
      turn_server.port.unwrap_or(DEFAULT_TURNS_PORT)
    ))
    .await?
    .next();

    if let Some(addr) = maybe_addr {
      debug!("TURN address: {:?}", addr);
      ice_agent.set_relay_info(
        ice_stream_id,
        ice_component_id,
        &addr.ip().to_string(),
        addr.port() as u32,
        turn_server.username.as_deref().unwrap_or_default(),
        turn_server.password.as_deref().unwrap_or_default(),
        nice::RelayType::Tls,
      );
    }
  }

  if !ice_agent.attach_recv(
    ice_stream_id,
    ice_component_id,
    &conference.glib_main_context,
    |_, _, _, s| debug!("ICE nice_agent_attach_recv cb: {}", s),
  ) {
    bail!("nice_agent_attach_recv failed");
  }

  debug!("ice_agent={:?}", ice_agent);
  debug!("ice_stream_id={}", ice_stream_id);
  debug!("ice_component_id={}", ice_component_id);

  if let (Some(ufrag), Some(pwd)) = (&transport.ufrag, &transport.pwd) {
    debug!("setting ICE remote credentials");
    if !ice_agent.set_remote_credentials(ice_stream_id, ufrag, pwd) {
      bail!("nice_agent_set_remote_credentials failed");
    }
  }

  ice_agent.connect_candidate_gathering_done(move |_agent, candidates| {
    debug!("ICE candidate-gathering-done {:?}", candidates);
  });

  debug!("gathering ICE candidates");
  if !ice_agent.gather_candidates(ice_stream_id) {
    bail!("nice_agent_gather_candidates failed");
  }

  if let (Some(ufrag), Some(pwd), remote_candidates) = (&transport.ufrag, &transport.pwd, &transport.candidates) {
    debug!("setting ICE remote candidates: {:?}", remote_candidates);
    let remote_candidates: Vec<_> = remote_candidates
      .iter()
      .map(|c| {
        let mut candidate = nice::Candidate::new(match c.type_ {
          jingle_ice_udp::Type::Host => nice::CandidateType::Host,
          jingle_ice_udp::Type::Prflx => nice::CandidateType::PeerReflexive,
          jingle_ice_udp::Type::Srflx => nice::CandidateType::ServerReflexive,
          jingle_ice_udp::Type::Relay => nice::CandidateType::Relayed,
        });
        candidate.set_stream_id(ice_stream_id);
        candidate.set_component_id(c.component as u32);
        candidate.set_foundation(&c.foundation);
        candidate.set_addr(SocketAddr::new(c.ip, c.port));
        candidate.set_priority(c.priority);
        candidate.set_username(ufrag);
        candidate.set_password(pwd);
        debug!("candidate: {:?}", candidate);
        candidate
      })
      .collect();
    let candidate_refs: Vec<_> = remote_candidates.iter().collect();
    let res = ice_agent.set_remote_candidates(ice_stream_id, ice_component_id, &candidate_refs);
    if res < remote_candidates.len() as i32 {
      warn!("some remote candidates failed to add: {}", res);
    }
  }

  Ok((ice_agent, ice_stream_id, ice_component_id))
}

pub(crate) fn participant_id_for_owner(owner: String) -> Result<Option<String>> {
  if owner == "jvb" {
    Ok(None)
  } else {
    Ok(Some(if owner.contains('/') {
      owner.split('/').nth(1).context("invalid ssrc-info owner")?.to_owned()
    } else {
      owner
    }))
  }
}
