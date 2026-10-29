use std::{
  net::SocketAddr,
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use glib::prelude::ObjectExt as _;
use jitsi_xmpp_parsers::jingle_ice_udp::Transport as IceUdpTransport;
use nice_gst_meet as nice;
use tokio::{
  net::lookup_host,
  sync::{mpsc, oneshot, Notify},
  task::JoinHandle,
};
use tracing::{debug, warn};
use xmpp_parsers::jingle_ice_udp::{self, Candidate};

use super::{offer::RemoteIce, turn_tls::TlsTunnel};
use crate::{tls::TlsClient, xmpp::extdisco::Service};

/// A server that does not resolve in time is left out rather than holding up the session.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_STUN_PORT: u16 = 3478;
const DEFAULT_TURN_PORT: u16 = 3478;
const DEFAULT_TURNS_PORT: u16 = 5349;

/// libnice reports a failure only for a component that has pairs to check, and the checks themselves take a while
/// to give up, so these are ours.
#[derive(Debug, Clone, Copy)]
pub(super) struct IceTimeouts {
  /// How long after gathering started the component may take to connect.
  pub(super) connect: Duration,
  /// How long, once gathering is done and the last remote candidate has been added, libnice may go without having
  /// formed a single pair. That is the case when no remote candidate is one we can use (TCP, or IPv6 on a host
  /// with no IPv6 address) and none is coming. Remote candidates can arrive late (transport-info), and a peer that
  /// can reach us may also create a pair by checking us first (peer reflexive), so it is not an immediate failure
  /// but a short wait; whatever comes out of it moves libnice on to checking, and ends the wait.
  pub(super) no_pairs: Duration,
}

impl Default for IceTimeouts {
  fn default() -> Self {
    Self {
      connect: Duration::from_secs(30),
      no_pairs: Duration::from_secs(5),
    }
  }
}

pub(super) const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug)]
pub(crate) struct LocalIceCandidate {
  pub(crate) component_id: u32,
  pub(crate) foundation: String,
  pub(crate) addr: SocketAddr,
  pub(crate) priority: u32,
  pub(crate) candidate_type: nice::CandidateType,
  /// The base address of a server reflexive or relayed candidate.
  pub(crate) rel_addr: Option<SocketAddr>,
}

type StateCallback = Arc<dyn Fn(nice::ComponentState) + Send + Sync>;

/// What is known about the component, shared between the signal handlers of the agent (which run on whichever
/// thread libnice emits from) and the watchdog.
#[derive(Default)]
pub(super) struct WatchState {
  pub(super) gathering_started: Option<Instant>,
  pub(super) gathering_done: Option<Instant>,
  pub(super) gather_failed: bool,
  pub(super) remote_added: Option<Instant>,
  /// libnice has a check list: the component went to `Connecting` or beyond.
  pub(super) checking: bool,
  pub(super) connected: bool,
  /// `Failed` has been passed on, by libnice or by the watchdog.
  pub(super) failed: bool,
  pub(super) closed: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Verdict {
  /// Nothing left to watch for.
  Done,
  Fail(String),
  /// Look again at this time, or earlier if something changes.
  Wait(Instant),
}

impl WatchState {
  pub(super) fn verdict(&self, now: Instant, timeouts: &IceTimeouts) -> Verdict {
    if self.connected || self.failed || self.closed {
      return Verdict::Done;
    }
    let Some(started) = self.gathering_started else {
      return Verdict::Done;
    };
    if self.gather_failed {
      return Verdict::Fail("nice_agent_gather_candidates failed".to_owned());
    }
    let deadline = started + timeouts.connect;
    if now >= deadline {
      return Verdict::Fail(format!("not connected {:?} after gathering started", timeouts.connect));
    }
    if !self.checking {
      if let Some(done) = self.gathering_done {
        let quiet_since = self.remote_added.map_or(done, |added| added.max(done));
        let give_up = quiet_since + timeouts.no_pairs;
        if now >= give_up {
          return Verdict::Fail(format!(
            "no candidate pair could be formed in {:?} since gathering ended and the last remote candidate was added",
            timeouts.no_pairs
          ));
        }
        return Verdict::Wait(give_up.min(deadline));
      }
    }
    Verdict::Wait(deadline)
  }
}

struct Watch {
  state: Mutex<WatchState>,
  wake: Notify,
  callback: Mutex<Option<StateCallback>>,
}

impl Watch {
  fn update(&self, f: impl FnOnce(&mut WatchState)) {
    f(&mut self.state.lock().unwrap_or_else(|e| e.into_inner()));
    self.wake.notify_one();
  }

  /// Passes `state` on to the callback, unless the transport is closing or has failed already: the callbacks
  /// act on the pipeline, and the pipeline is being taken apart.
  fn report(&self, state: nice::ComponentState) {
    let callback = {
      let mut watched = self.state.lock().unwrap_or_else(|e| e.into_inner());
      if watched.closed || watched.failed {
        return;
      }
      match state {
        nice::ComponentState::Connecting => watched.checking = true,
        nice::ComponentState::Connected | nice::ComponentState::Ready => {
          watched.checking = true;
          watched.connected = true;
        },
        nice::ComponentState::Failed => watched.failed = true,
        _ => {},
      }
      self.callback.lock().unwrap_or_else(|e| e.into_inner()).clone()
    };
    self.wake.notify_one();
    if let Some(callback) = callback {
      callback(state);
    }
  }

  fn close(&self) {
    self.update(|state| state.closed = true);
    // The callbacks hold what they act on.
    self.callback.lock().unwrap_or_else(|e| e.into_inner()).take();
  }
}

/// Reports `Failed` when the component has neither connected nor been reported failed by libnice in time, which
/// libnice does not do by itself when it has no pairs to check.
async fn watchdog(watch: Arc<Watch>, timeouts: IceTimeouts) {
  loop {
    let verdict = watch
      .state
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .verdict(Instant::now(), &timeouts);
    match verdict {
      Verdict::Done => return,
      Verdict::Fail(reason) => {
        warn!("ICE failed: {}", reason);
        watch.report(nice::ComponentState::Failed);
        return;
      },
      Verdict::Wait(until) => {
        tokio::select! {
          _ = watch.wake.notified() => {},
          _ = tokio::time::sleep_until(tokio::time::Instant::from_std(until)) => {},
        }
      },
    }
  }
}

pub(super) struct IceTransport {
  agent: nice::Agent,
  context: glib::MainContext,
  stream_id: u32,
  component_id: u32,
  remote_ufrag: String,
  remote_pwd: String,
  initial_candidates: Vec<Candidate>,
  candidate_rx: Option<mpsc::UnboundedReceiver<LocalIceCandidate>>,
  handlers: Vec<glib::SignalHandlerId>,
  watch: Arc<Watch>,
  timeouts: IceTimeouts,
  watchdog: Option<JoinHandle<()>>,
  tunnels: Vec<TlsTunnel>,
}

impl Drop for IceTransport {
  /// Without [`close`](IceTransport::close) the allocations are still released, if the context is iterated for
  /// long enough; the tunnels they are released through live until then.
  fn drop(&mut self) {
    self.stop_watching();
    let tunnels = std::mem::take(&mut self.tunnels);
    self.agent.close_async(&self.context, move || drop(tunnels));
  }
}

impl IceTransport {
  /// Sets the agent up on `context`, which must be iterated for as long as the session lives. Nothing is
  /// gathered before [`gather`](Self::gather). Failing to resolve a STUN or TURN server only costs us the
  /// candidates it would have provided.
  pub(super) async fn new(
    context: &glib::MainContext,
    services: &[Service],
    remote: &RemoteIce,
    tls_insecure: bool,
  ) -> Result<Self> {
    let agent = nice::Agent::new_full(
      context,
      nice::Compatibility::Rfc5245,
      nice::AgentOption::CONSENT_FRESHNESS,
    );
    agent.set_ice_tcp(false);
    agent.set_upnp(false);
    agent.set_controlling_mode(false);
    let stream_id = agent.add_stream(1);
    let component_id = 1;

    if let Some(stun) = services.iter().find(|service| service.r#type == "stun") {
      match resolve(&stun.host, stun.port.unwrap_or(DEFAULT_STUN_PORT)).await {
        Ok(addrs) => {
          if let Some(addr) = addrs.iter().find(|addr| addr.is_ipv4()).or(addrs.first()) {
            debug!("STUN server: {}", addr);
            agent.set_stun_server(Some(&addr.ip().to_string()));
            agent.set_stun_server_port(addr.port() as u32);
          }
        },
        Err(e) => warn!("not using STUN server {}: {:#}", stun.host, e),
      }
    }

    let mut tunnels = Vec::new();
    for service in services {
      if let Err(e) = add_relay(&agent, stream_id, component_id, service, tls_insecure, &mut tunnels).await {
        warn!("not using {} server {}: {:#}", service.r#type, service.host, e);
      }
    }

    if !agent.set_remote_credentials(stream_id, &remote.ufrag, &remote.pwd) {
      bail!("nice_agent_set_remote_credentials failed");
    }

    let watch = Arc::new(Watch {
      state: Mutex::default(),
      wake: Notify::new(),
      callback: Mutex::default(),
    });
    let (candidate_tx, candidate_rx) = mpsc::unbounded_channel::<LocalIceCandidate>();
    // Dropped when gathering is done, which closes the channel.
    let candidate_tx = Arc::new(Mutex::new(Some(candidate_tx)));
    let mut handlers = Vec::new();
    {
      let candidate_tx = candidate_tx.clone();
      handlers.push(agent.connect_new_candidate_full(move |agent, candidate| {
        if candidate.stream_id() != stream_id {
          return;
        }
        let Some(addr) = candidate.addr() else { return };
        let Ok(foundation) = candidate.foundation() else { return };
        let local = LocalIceCandidate {
          component_id: candidate.component_id(),
          foundation: foundation.to_owned(),
          addr,
          priority: candidate.priority(),
          candidate_type: candidate.type_(),
          rel_addr: related_address(agent, candidate),
        };
        if let Some(tx) = candidate_tx.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
          let _ = tx.send(local);
        }
      }));
    }
    {
      let watch = watch.clone();
      handlers.push(agent.connect_candidate_gathering_done(move |_, done_stream_id| {
        if done_stream_id == stream_id {
          debug!("ICE candidate gathering done");
          candidate_tx.lock().unwrap_or_else(|e| e.into_inner()).take();
          watch.update(|state| state.gathering_done = Some(Instant::now()));
        }
      }));
    }
    {
      let watch = watch.clone();
      handlers.push(
        agent.connect_component_state_changed(move |_, changed_stream_id, changed_component_id, state| {
          if (changed_stream_id, changed_component_id) == (stream_id, component_id) {
            watch.report(state);
          }
        }),
      );
    }

    Ok(Self {
      agent,
      context: context.clone(),
      stream_id,
      component_id,
      remote_ufrag: remote.ufrag.clone(),
      remote_pwd: remote.pwd.clone(),
      initial_candidates: remote.candidates.clone(),
      candidate_rx: Some(candidate_rx),
      handlers,
      watch,
      timeouts: IceTimeouts::default(),
      watchdog: None,
      tunnels,
    })
  }

  #[cfg(test)]
  pub(super) fn with_timeouts(mut self, timeouts: IceTimeouts) -> Self {
    self.timeouts = timeouts;
    self
  }

  pub(super) fn agent(&self) -> &nice::Agent {
    &self.agent
  }

  pub(super) fn stream_id(&self) -> u32 {
    self.stream_id
  }

  pub(super) fn component_id(&self) -> u32 {
    self.component_id
  }

  pub(super) fn local_credentials(&self) -> Result<(String, String)> {
    self
      .agent
      .local_credentials(self.stream_id)
      .ok_or_else(|| anyhow::anyhow!("no local ICE credentials"))
  }

  /// `f` is called with every state change of the component, and with `Failed` when the component did not get
  /// anywhere in time (see [`IceTimeouts`]). It runs on whichever thread libnice emits the change from, which is
  /// the agent's main context or a streaming thread (`nicesrc` reads from the agent while holding its stream
  /// lock), so it must not block and has to cope with racing against the teardown of the session. It is not called
  /// once the transport is [closed](Self::close) or dropped.
  pub(super) fn on_component_state(&mut self, f: impl Fn(nice::ComponentState) + Send + Sync + 'static) {
    *self.watch.callback.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(f));
  }

  /// Starts gathering, hands the remote candidates of the offer to the agent, and collects local candidates
  /// until gathering is done or `timeout` has passed. In the latter case the rest can be had from the returned
  /// receiver as they arrive. The time to connect (see [`IceTimeouts`]) runs from here.
  pub(super) async fn gather(
    &mut self,
    timeout: Duration,
  ) -> (
    Vec<LocalIceCandidate>,
    Option<mpsc::UnboundedReceiver<LocalIceCandidate>>,
  ) {
    let Some(mut candidates) = self.candidate_rx.take() else {
      return (Vec::new(), None);
    };
    self
      .watch
      .update(|state| state.gathering_started = Some(Instant::now()));
    self.watchdog = Some(tokio::spawn(watchdog(self.watch.clone(), self.timeouts)));
    if !self.agent.gather_candidates(self.stream_id) {
      warn!("nice_agent_gather_candidates failed");
      self.watch.update(|state| state.gather_failed = true);
      return (Vec::new(), None);
    }
    let initial = std::mem::take(&mut self.initial_candidates);
    self.add_candidates(&self.remote_ufrag, &self.remote_pwd, &initial);

    let mut gathered = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
      match tokio::time::timeout_at(deadline, candidates.recv()).await {
        Ok(Some(candidate)) => gathered.push(candidate),
        Ok(None) => {
          debug!("gathered {} local ICE candidates", gathered.len());
          return (gathered, None);
        },
        Err(_) => {
          warn!(
            "ICE gathering not done after {:?}; continuing with {} candidates",
            timeout,
            gathered.len()
          );
          return (gathered, Some(candidates));
        },
      }
    }
  }

  /// A transport-info may leave out the credentials; those of the offer apply then.
  pub(super) fn add_remote_candidates(&self, transport: &IceUdpTransport) -> Result<()> {
    let ufrag = transport.ufrag.as_deref().unwrap_or(&self.remote_ufrag);
    let pwd = transport.pwd.as_deref().unwrap_or(&self.remote_pwd);
    self.add_candidates(ufrag, pwd, &transport.candidates);
    Ok(())
  }

  fn add_candidates(&self, ufrag: &str, pwd: &str, candidates: &[Candidate]) {
    let remote_candidates = remote_candidates(self.stream_id, ufrag, pwd, candidates);
    if remote_candidates.is_empty() {
      return;
    }
    let refs: Vec<_> = remote_candidates.iter().collect();
    let added = self
      .agent
      .set_remote_candidates(self.stream_id, self.component_id, &refs);
    if added < refs.len() as i32 {
      warn!("only {} of {} remote candidates were added", added, refs.len());
    }
    if added > 0 {
      self.watch.update(|state| state.remote_added = Some(Instant::now()));
    }
  }

  fn stop_watching(&mut self) {
    self.watch.close();
    if let Some(watchdog) = self.watchdog.take() {
      watchdog.abort();
    }
    for handler in self.handlers.drain(..) {
      self.agent.disconnect(handler);
    }
  }

  /// Closes the agent, which frees the allocations on TURN servers, and waits for the servers to say it is done,
  /// for `timeout` at most. The tunnels to `turns:` servers live until then.
  pub(super) async fn close(mut self, timeout: Duration) {
    self.stop_watching();
    let (done_tx, done_rx) = oneshot::channel();
    if !self.agent.close_async(&self.context, move || {
      let _ = done_tx.send(());
    }) {
      return;
    }
    match tokio::time::timeout(timeout, done_rx).await {
      Ok(_) => debug!("the ICE agent is closed"),
      Err(_) => warn!(
        "the ICE agent was not closed after {:?}, its TURN allocations may be left to expire",
        timeout
      ),
    }
  }
}

/// The candidates libnice can use: UDP ones, because ICE-TCP is off.
pub(super) fn remote_candidates(
  stream_id: u32,
  ufrag: &str,
  pwd: &str,
  candidates: &[Candidate],
) -> Vec<nice::Candidate> {
  candidates
    .iter()
    .filter(|candidate| {
      let udp = candidate.protocol.eq_ignore_ascii_case("udp");
      if !udp {
        debug!(
          "skipping remote {} candidate {}:{}",
          candidate.protocol, candidate.ip, candidate.port
        );
      }
      udp
    })
    .map(|candidate| {
      let mut remote = nice::Candidate::new(match candidate.type_ {
        jingle_ice_udp::Type::Host => nice::CandidateType::Host,
        jingle_ice_udp::Type::Prflx => nice::CandidateType::PeerReflexive,
        jingle_ice_udp::Type::Srflx => nice::CandidateType::ServerReflexive,
        jingle_ice_udp::Type::Relay => nice::CandidateType::Relayed,
      });
      remote.set_stream_id(stream_id);
      remote.set_component_id(candidate.component as u32);
      remote.set_foundation(&candidate.foundation);
      remote.set_addr(SocketAddr::new(candidate.ip, candidate.port));
      remote.set_priority(candidate.priority);
      remote.set_transport(nice::CandidateTransport::Udp);
      remote.set_username(ufrag);
      remote.set_password(pwd);
      remote
    })
    .collect()
}

/// libnice does not expose the base of a candidate, but writes it into the `raddr`/`rport` of its SDP line.
fn related_address(agent: &nice::Agent, candidate: &nice::Candidate) -> Option<SocketAddr> {
  if candidate.type_() == nice::CandidateType::Host {
    return None;
  }
  let sdp = agent.generate_local_candidate_sdp(&mut candidate.clone())?;
  parse_related_address(&sdp)
}

pub(super) fn parse_related_address(sdp: &str) -> Option<SocketAddr> {
  let mut tokens = sdp.split_whitespace();
  let mut ip = None;
  let mut port = None;
  while let Some(token) = tokens.next() {
    match token {
      "raddr" => ip = tokens.next(),
      "rport" => port = tokens.next(),
      _ => {},
    }
  }
  Some(SocketAddr::new(ip?.parse().ok()?, port?.parse().ok()?))
}

async fn resolve(host: &str, port: u16) -> Result<Vec<SocketAddr>> {
  let addrs: Vec<_> = tokio::time::timeout(RESOLVE_TIMEOUT, lookup_host((host, port)))
    .await
    .context("timed out resolving the address")??
    .collect();
  if addrs.is_empty() {
    bail!("{host} resolved to no addresses");
  }
  Ok(addrs)
}

async fn add_relay(
  agent: &nice::Agent,
  stream_id: u32,
  component_id: u32,
  service: &Service,
  tls_insecure: bool,
  tunnels: &mut Vec<TlsTunnel>,
) -> Result<()> {
  let username = service.username.as_deref().unwrap_or_default();
  let password = service.password.as_deref().unwrap_or_default();
  let set_relay = |addr: SocketAddr, relay_type| {
    debug!("TURN {:?} {}: {}", relay_type, service.host, addr);
    if !agent.set_relay_info(
      stream_id,
      component_id,
      &addr.ip().to_string(),
      addr.port() as u32,
      username,
      password,
      relay_type,
    ) {
      warn!(
        "nice_agent_set_relay_info failed for {} ({:?})",
        service.host, relay_type
      );
    }
  };

  match (service.r#type.as_str(), service.transport.as_deref()) {
    ("turn", transport) => {
      let relay_type = if transport == Some("tcp") {
        nice::RelayType::Tcp
      }
      else {
        nice::RelayType::Udp
      };
      for addr in resolve(&service.host, service.port.unwrap_or(DEFAULT_TURN_PORT)).await? {
        set_relay(addr, relay_type);
      }
    },
    ("turns", _) => {
      let addrs = resolve(&service.host, service.port.unwrap_or(DEFAULT_TURNS_PORT)).await?;
      let target = addrs
        .iter()
        .find(|addr| addr.is_ipv4())
        .or(addrs.first())
        .copied()
        .ok_or_else(|| anyhow::anyhow!("no address"))?;
      // Once per tunnel, not per connection. Reading the system's root certificates blocks.
      let client = Arc::new(
        tokio::task::spawn_blocking(move || TlsClient::new(tls_insecure))
          .await
          .context("the TLS setup panicked")??,
      );
      let host = service.host.clone();
      let tunnel = TlsTunnel::start(target, move |tcp| {
        let (client, host) = (client.clone(), host.clone());
        async move { client.connect(&host, tcp).await }
      })
      .await?;
      set_relay(tunnel.local_addr(), nice::RelayType::Tcp);
      tunnels.push(tunnel);
    },
    _ => {},
  }
  Ok(())
}
