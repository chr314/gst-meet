use std::{
  sync::{mpsc, Arc},
  thread,
  time::Duration,
};

use glib::prelude::ObjectExt as _;

use crate::{Agent, Compatibility, ComponentState};

const TIMEOUT: Duration = Duration::from_secs(15);

struct Peer {
  agent: Agent,
  stream_id: u32,
  states: mpsc::Receiver<ComponentState>,
  received: mpsc::Receiver<Vec<u8>>,
  /// Dropped together with the receive closure, when the agent is finalized.
  closure_marker: Arc<()>,
}

fn peer(ctx: &glib::MainContext, controlling: bool) -> Peer {
  let agent = Agent::new(ctx, Compatibility::Rfc5245);
  agent.set_controlling_mode(controlling);
  let stream_id = agent.add_stream(1);

  let (states_tx, states) = mpsc::channel();
  let states_tx = std::sync::Mutex::new(states_tx);
  agent.connect_component_state_changed(move |_agent, _stream_id, _component_id, state| {
    let _ = states_tx.lock().unwrap().send(state);
  });

  let (received_tx, received) = mpsc::channel();
  let closure_marker = Arc::new(());
  let marker = closure_marker.clone();
  assert!(
    agent.attach_recv(stream_id, 1, ctx, move |_agent, _stream_id, _component_id, data| {
      let _marker = &marker;
      let _ = received_tx.send(data.to_vec());
    })
  );

  Peer {
    agent,
    stream_id,
    states,
    received,
    closure_marker,
  }
}

fn connect(a: &Peer, b: &Peer) -> bool {
  for (local, remote) in [(a, b), (b, a)] {
    let (ufrag, pwd) = local.agent.local_credentials(local.stream_id).unwrap();
    assert!(remote.agent.set_remote_credentials(remote.stream_id, &ufrag, &pwd));
  }
  assert!(a.agent.gather_candidates(a.stream_id));
  assert!(b.agent.gather_candidates(b.stream_id));
  for (local, remote) in [(a, b), (b, a)] {
    let mut candidates = local.agent.local_candidates(local.stream_id, 1);
    if candidates.is_empty() {
      return false;
    }
    let remote_candidates: Vec<_> = candidates
      .iter_mut()
      .map(|candidate| {
        let sdp = local.agent.generate_local_candidate_sdp(candidate).unwrap();
        remote.agent.parse_remote_candidate_sdp(remote.stream_id, &sdp).unwrap()
      })
      .collect();
    let refs: Vec<_> = remote_candidates.iter().collect();
    assert_eq!(
      remote.agent.set_remote_candidates(remote.stream_id, 1, &refs),
      refs.len() as i32
    );
  }
  true
}

fn wait_ready(peer: &Peer) {
  let mut seen = Vec::new();
  loop {
    let state = peer
      .states
      .recv_timeout(TIMEOUT)
      .unwrap_or_else(|_| panic!("not ready, states seen: {:?}", seen));
    seen.push(state);
    match state {
      ComponentState::Ready => return,
      ComponentState::Failed => panic!("connectivity checks failed, states seen: {:?}", seen),
      _ => {},
    }
  }
}

#[test]
fn agents_connect_and_exchange_data() {
  let ctx = glib::MainContext::new();
  let main_loop = glib::MainLoop::new(Some(&ctx), false);
  let loop_thread = {
    let (ctx, main_loop) = (ctx.clone(), main_loop.clone());
    thread::spawn(move || ctx.with_thread_default(|| main_loop.run()).unwrap())
  };

  let a = peer(&ctx, true);
  let b = peer(&ctx, false);
  if !connect(&a, &b) {
    eprintln!("no local candidates on this machine, skipping");
    main_loop.quit();
    loop_thread.join().unwrap();
    return;
  }
  wait_ready(&a);
  wait_ready(&b);
  assert_eq!(a.agent.component_state(a.stream_id, 1), ComponentState::Ready);

  // The selected pair is lent by libnice; every call must hand out independent copies.
  for _ in 0..50 {
    let (local, remote) = a.agent.get_selected_pair(a.stream_id, 1).expect("a pair is selected");
    assert_eq!(local.stream_id(), a.stream_id);
    assert!(local.addr().is_some() && remote.addr().is_some());
    drop((local, remote));
  }
  assert!(a.agent.get_selected_pair(a.stream_id, 2).is_none());

  assert_eq!(b.agent.send(b.stream_id, 1, 5, "hello"), 5);
  assert_eq!(a.received.recv_timeout(TIMEOUT).unwrap(), b"hello");
  assert_eq!(a.agent.send(a.stream_id, 1, 3, "abc"), 3);
  assert_eq!(b.received.recv_timeout(TIMEOUT).unwrap(), b"abc");

  main_loop.quit();
  loop_thread.join().unwrap();
  let (marker_a, marker_b) = (a.closure_marker.clone(), b.closure_marker.clone());
  assert_eq!(Arc::strong_count(&marker_a), 3, "agent, test and this clone");
  drop((a.agent, b.agent));
  assert_eq!(
    Arc::strong_count(&marker_a),
    2,
    "the receive closure is released with the agent"
  );
  assert_eq!(Arc::strong_count(&marker_b), 2);
}

#[test]
fn receive_closures_are_kept_until_the_agent_is_gone_and_failures_are_forgotten() {
  let ctx = glib::MainContext::new();
  let agent = Agent::new(&ctx, Compatibility::Rfc5245);
  let stream_id = agent.add_stream(1);
  let marker = Arc::new(());

  for _ in 0..3 {
    let marker = marker.clone();
    assert!(agent.attach_recv(stream_id, 1, &ctx, move |_, _, _, _| {
      let _ = &marker;
    }));
  }
  let before = marker.clone();
  assert_eq!(Arc::strong_count(&marker), 5);

  // No such component: libnice refuses and the closure is not kept.
  let refused = marker.clone();
  assert!(!agent.attach_recv(stream_id, 9, &ctx, move |_, _, _, _| {
    let _ = &refused;
  }));
  assert_eq!(Arc::strong_count(&marker), 5);

  drop(agent);
  assert_eq!(Arc::strong_count(&marker), 2);
  drop(before);
}

#[test]
fn new_full_applies_its_options() {
  let ctx = glib::MainContext::new();
  let agent = Agent::new_full(
    &ctx,
    Compatibility::Rfc5245,
    crate::AgentOption::CONSENT_FRESHNESS | crate::AgentOption::ICE_TRICKLE,
  );
  assert!(agent.property::<bool>("consent-freshness"));
  assert!(agent.property::<bool>("ice-trickle"));
  assert!(!Agent::new(&ctx, Compatibility::Rfc5245).property::<bool>("consent-freshness"));
}

#[test]
fn close_async_reports_on_the_given_context_once_and_removes_the_streams() {
  let ctx = glib::MainContext::new();
  let main_loop = glib::MainLoop::new(Some(&ctx), false);
  let loop_thread = {
    let (ctx, main_loop) = (ctx.clone(), main_loop.clone());
    thread::spawn(move || {
      ctx
        .with_thread_default(|| {
          main_loop.run();
          thread::current().id()
        })
        .unwrap()
    })
  };

  let agent = Agent::new(&ctx, Compatibility::Rfc5245);
  let stream_id = agent.add_stream(1);
  assert!(agent.local_credentials(stream_id).is_some());

  // Called from a thread that does not iterate `ctx`: the report must still be delivered by its loop, not by the
  // (never iterated) global default context.
  let (tx, rx) = mpsc::channel();
  let marker = Arc::new(());
  {
    let marker = marker.clone();
    assert!(agent.close_async(&ctx, move || {
      let _marker = &marker;
      let _ = tx.send(thread::current().id());
    }));
  }
  let reported = rx.recv_timeout(TIMEOUT);
  main_loop.quit();
  let loop_thread_id = loop_thread.join().unwrap();
  assert_eq!(reported, Ok(loop_thread_id));
  assert!(rx.try_recv().is_err(), "reported once");
  assert!(agent.local_credentials(stream_id).is_none(), "the stream is gone");

  // A second close is refused and its callback dropped, not leaked.
  let second = marker.clone();
  assert!(!agent.close_async(&ctx, move || {
    let _ = &second;
  }));
  drop(agent);
  assert_eq!(Arc::strong_count(&marker), 1);
}
