use std::{
  io,
  net::SocketAddr,
  str::FromStr,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
  },
  thread,
  time::{Duration, Instant},
};

use gstreamer::prelude::*;
use jitsi_xmpp_parsers::jingle_dtls_srtp::Fingerprint as SignaledFingerprint;
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  sync::mpsc,
};
use xmpp_parsers::{hashes::Algo, jingle_dtls_srtp::Setup};

use super::support::*;
use crate::pipeline::{
  dtls::{gate_decoder, pick_fingerprint, DtlsIdentity},
  Fingerprint, HashAlgo,
};

fn certificate_der(identity: &DtlsIdentity) -> Vec<u8> {
  pem::parse_many(&identity.pem)
    .unwrap()
    .into_iter()
    .find(|block| block.tag() == "CERTIFICATE")
    .unwrap()
    .into_contents()
}

fn fingerprint_of(identity: &DtlsIdentity, algo: HashAlgo) -> Fingerprint {
  Fingerprint {
    algo,
    value: algo.digest(&certificate_der(identity)),
  }
}

fn signaled(hash: Algo, len: usize, setup: Option<Setup>) -> SignaledFingerprint {
  SignaledFingerprint {
    hash,
    setup,
    value: vec![len as u8; len],
  }
}

#[test]
fn the_strongest_supported_fingerprint_is_picked_whatever_the_order() {
  let sha1 = signaled(Algo::Sha_1, 20, Some(Setup::Actpass));
  let sha256 = signaled(Algo::Sha_256, 32, Some(Setup::Actpass));
  let sha512 = signaled(Algo::Sha_512, 64, Some(Setup::Actpass));
  let sha384 = signaled(Algo::Unknown("sha-384".to_owned()), 48, Some(Setup::Actpass));

  let picked = |fingerprints: &[&SignaledFingerprint]| {
    let owned: Vec<_> = fingerprints.iter().map(|f| (*f).clone()).collect();
    pick_fingerprint(&owned).unwrap().0
  };
  assert_eq!(picked(&[&sha1, &sha256]).algo, HashAlgo::Sha256);
  assert_eq!(picked(&[&sha256, &sha1]).algo, HashAlgo::Sha256);
  assert_eq!(picked(&[&sha256, &sha512, &sha1]).algo, HashAlgo::Sha512);
  assert_eq!(picked(&[&sha512, &sha384, &sha256]).algo, HashAlgo::Sha512);
  assert_eq!(picked(&[&sha384, &sha256]).algo, HashAlgo::Sha384);
  assert_eq!(picked(&[&sha1]).algo, HashAlgo::Sha1);
  assert_eq!(picked(&[&sha256, &sha1]).value, sha256.value);
}

#[test]
fn fingerprints_that_cannot_be_checked_are_skipped_and_the_role_comes_from_any_that_names_one() {
  let md5 = signaled(Algo::Unknown("md5".to_owned()), 16, Some(Setup::Active));
  let sha3 = signaled(Algo::Sha3_256, 32, None);
  let truncated = signaled(Algo::Sha_512, 20, None);
  let sha256 = signaled(Algo::Sha_256, 32, None);

  let (picked, setup) = pick_fingerprint(&[md5.clone(), sha3.clone(), truncated.clone(), sha256.clone()]).unwrap();
  assert_eq!(picked.algo, HashAlgo::Sha256, "the stronger ones are unusable");
  assert_eq!(setup, Some(Setup::Active), "the role is named by another element");

  let (_, setup) = pick_fingerprint(std::slice::from_ref(&sha256)).unwrap();
  assert_eq!(setup, None);

  let error = pick_fingerprint(&[md5, sha3, truncated]).unwrap_err().to_string();
  assert!(error.contains("none of the DTLS fingerprints"), "{error}");
  assert!(error.contains("md5") && error.contains("20 bytes"), "{error}");
  let error = pick_fingerprint(&[]).unwrap_err().to_string();
  assert!(error.contains("no DTLS fingerprint"), "{error}");
}

#[test]
fn an_offer_with_several_fingerprints_is_verified_against_the_strongest() {
  use jitsi_xmpp_parsers::jingle::Jingle;
  use xmpp_parsers::ns;

  use crate::{
    conference::ReceiveMode,
    jingle::{parse_offer, OfferConfig},
    tests::xml::*,
  };

  init();
  let sha256 = fingerprint();
  let sha512 = vec!["CD"; 64].join(":");
  let sha1 = vec!["01"; 20].join(":");
  let iq = session_initiate("1", &Offer::default()).replace(
    &format!(">{sha256}</fingerprint>"),
    &format!(
      ">{sha256}</fingerprint>\
       <fingerprint xmlns='urn:xmpp:jingle:apps:dtls:0' hash='sha-1' setup='actpass'>{sha1}</fingerprint>\
       <fingerprint xmlns='urn:xmpp:jingle:apps:dtls:0' hash='sha-512' setup='actpass'>{sha512}</fingerprint>"
    ),
  );
  assert_eq!(iq.matches("<fingerprint").count(), 3 * 2, "one per content");
  let element = parse(&iq);
  let jingle = Jingle::try_from(element.get_child("jingle", ns::JINGLE).unwrap().clone()).unwrap();
  let video_codecs = ["vp8".to_owned()];
  let config = OfferConfig {
    video_codecs: &video_codecs,
    receive_mode: ReceiveMode::Encoded,
  };
  let offer = parse_offer(&jingle, &config, None).unwrap();
  assert_eq!(offer.ice.fingerprint.algo, HashAlgo::Sha512);
  assert_eq!(offer.ice.fingerprint.value, vec![0xCD; 64]);
  assert_eq!(offer.ice.setup, Setup::Actpass);
}

/// Sends a buffer built by `make` every 20 ms into `sink`, from a thread of its own.
struct Pusher {
  stop: Arc<AtomicBool>,
  finished: std::sync::mpsc::Receiver<()>,
}

impl Pusher {
  fn start(sink: &gstreamer::Pad, caps: &str, make: impl Fn(u16) -> Vec<u8> + Send + 'static) -> Self {
    let src = gstreamer::Pad::builder(gstreamer::PadDirection::Src).build();
    src.link(sink).unwrap();
    src.set_active(true).unwrap();
    src.push_event(gstreamer::event::StreamStart::new("transport-test"));
    src.push_event(gstreamer::event::Caps::new(&gstreamer::Caps::from_str(caps).unwrap()));
    src.push_event(gstreamer::event::Segment::new(&gstreamer::FormattedSegment::<
      gstreamer::ClockTime,
    >::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let (done, finished) = std::sync::mpsc::channel();
    thread::spawn({
      let stop = stop.clone();
      move || {
        let mut sequence = 0u16;
        while !stop.load(Ordering::SeqCst) {
          let _ = src.push(gstreamer::Buffer::from_slice(make(sequence)));
          sequence = sequence.wrapping_add(1);
          thread::sleep(Duration::from_millis(20));
        }
        let _ = src.set_active(false);
        let _ = done.send(());
      }
    });
    Self { stop, finished }
  }

  fn rtp(sink: &gstreamer::Pad, ssrc: u32) -> Self {
    Self::start(
      sink,
      "application/x-rtp, media=audio, payload=111, clock-rate=48000, encoding-name=OPUS",
      move |sequence| {
        let mut packet = vec![0x80, 111];
        packet.extend(sequence.to_be_bytes());
        packet.extend((sequence as u32 * 960).to_be_bytes());
        packet.extend(ssrc.to_be_bytes());
        packet.extend([0xAB; 20]);
        packet
      },
    )
  }

  /// An empty receiver report.
  fn rtcp(sink: &gstreamer::Pad, ssrc: u32) -> Self {
    Self::start(sink, "application/x-rtcp", move |_| {
      let mut packet = vec![0x80, 201, 0, 1];
      packet.extend(ssrc.to_be_bytes());
      packet
    })
  }
}

impl Drop for Pusher {
  /// A push that is stuck in an element (when the thing under test is broken) is left behind rather than waited
  /// for, so that the test fails instead of hanging.
  fn drop(&mut self) {
    self.stop.store(true, Ordering::SeqCst);
    let _ = self.finished.recv_timeout(Duration::from_secs(2));
  }
}

#[derive(Clone, Default)]
struct Counters {
  rtp: Arc<AtomicUsize>,
  rtcp: Arc<AtomicUsize>,
}

impl Counters {
  fn total(&self) -> usize {
    self.rtp.load(Ordering::SeqCst) + self.rtcp.load(Ordering::SeqCst)
  }
}

/// Two DTLS-SRTP ends facing each other. Both send RTP and RTCP. End A checks the certificate of end B against
/// `expected`, as the session does; end B takes whatever it gets.
struct Pair {
  // The pushers stop before the pipeline does.
  _pushers: Vec<Pusher>,
  _pipeline: NullOnDrop,
  /// What A's decoder lets out.
  at_a: Counters,
  /// What B's decoder produces, i.e. what A's encoder let go of.
  at_b: Counters,
  mismatches: Arc<Mutex<Vec<String>>>,
}

fn dtls_pair(a_is_client: bool, a: &DtlsIdentity, b: &DtlsIdentity, expected: Fingerprint) -> Pair {
  init();
  let pipeline = gstreamer::Pipeline::new();
  // Connection ids are process wide: tests run side by side.
  let (id_a, id_b) = (uuid::Uuid::new_v4().to_string(), uuid::Uuid::new_v4().to_string());
  let encoder = |id: &str, is_client: bool| {
    gstreamer::ElementFactory::make("dtlssrtpenc")
      .property("connection-id", id)
      .property("is-client", is_client)
      .build()
      .unwrap()
  };
  let decoder = |id: &str, identity: &DtlsIdentity| {
    gstreamer::ElementFactory::make("dtlssrtpdec")
      .property("connection-id", id)
      .property("pem", &identity.pem)
      .build()
      .unwrap()
  };
  let (enc_a, dec_a) = (encoder(&id_a, a_is_client), decoder(&id_a, a));
  let (enc_b, dec_b) = (encoder(&id_b, !a_is_client), decoder(&id_b, b));
  pipeline.add_many([&enc_a, &dec_a, &enc_b, &dec_b]).unwrap();
  enc_a.link_pads(Some("src"), &dec_b, Some("sink")).unwrap();
  enc_b.link_pads(Some("src"), &dec_a, Some("sink")).unwrap();

  let counted = |decoder: &gstreamer::Element| {
    let counters = Counters::default();
    for (name, counter) in [("rtp_src", &counters.rtp), ("rtcp_src", &counters.rtcp)] {
      let sink = gstreamer::ElementFactory::make("fakesink")
        .property("sync", false)
        .property("async", false)
        .build()
        .unwrap();
      pipeline.add(&sink).unwrap();
      decoder.link_pads(Some(name), &sink, None).unwrap();
      let counter = counter.clone();
      sink
        .static_pad("sink")
        .unwrap()
        .add_probe(gstreamer::PadProbeType::BUFFER, move |_, _| {
          counter.fetch_add(1, Ordering::SeqCst);
          gstreamer::PadProbeReturn::Ok
        });
    }
    counters
  };
  let (at_a, at_b) = (counted(&dec_a), counted(&dec_b));

  let mismatches = Arc::new(Mutex::new(Vec::new()));
  let gate = gate_decoder(&dec_a, expected, {
    let mismatches = mismatches.clone();
    move |reason| mismatches.lock().unwrap().push(reason)
  })
  .unwrap();
  // The encoder pads exist (and are gated) before anything flows.
  let sinks = |encoder: &gstreamer::Element| {
    (
      encoder.request_pad_simple("rtp_sink_0").unwrap(),
      encoder.request_pad_simple("rtcp_sink_0").unwrap(),
    )
  };
  let ((rtp_a, rtcp_a), (rtp_b, rtcp_b)) = (sinks(&enc_a), sinks(&enc_b));
  gate.gate_encoder_sink(&enc_a, "rtp_sink_0").unwrap();
  gate.gate_encoder_sink(&enc_a, "rtcp_sink_0").unwrap();

  pipeline.set_state(gstreamer::State::Playing).unwrap();
  let pushers = vec![
    Pusher::rtp(&rtp_a, 0xA0A0),
    Pusher::rtcp(&rtcp_a, 0xA0A0),
    Pusher::rtp(&rtp_b, 0xB0B0),
    Pusher::rtcp(&rtcp_b, 0xB0B0),
  ];
  Pair {
    _pipeline: NullOnDrop(pipeline),
    at_a,
    at_b,
    mismatches,
    _pushers: pushers,
  }
}

const HANDSHAKE: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn media_flows_both_ways_once_the_certificate_matches() {
  for a_is_client in [true, false] {
    let (a, b) = (DtlsIdentity::generate().unwrap(), DtlsIdentity::generate().unwrap());
    let pair = dtls_pair(a_is_client, &a, &b, fingerprint_of(&b, HashAlgo::Sha384));
    wait_until("media in both directions", HANDSHAKE, || {
      pair.at_a.rtp.load(Ordering::SeqCst) > 3
        && pair.at_a.rtcp.load(Ordering::SeqCst) > 3
        && pair.at_b.rtp.load(Ordering::SeqCst) > 3
        && pair.at_b.rtcp.load(Ordering::SeqCst) > 3
    })
    .await;
    assert!(pair.mismatches.lock().unwrap().is_empty());
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_is_sent_to_a_peer_whose_certificate_does_not_match() {
  for a_is_client in [true, false] {
    let (a, b, impostor) = (
      DtlsIdentity::generate().unwrap(),
      DtlsIdentity::generate().unwrap(),
      DtlsIdentity::generate().unwrap(),
    );
    // The offer promised another certificate than the one B presents.
    let pair = dtls_pair(a_is_client, &a, &b, fingerprint_of(&impostor, HashAlgo::Sha256));
    wait_until("the mismatch to be reported", HANDSHAKE, || {
      !pair.mismatches.lock().unwrap().is_empty()
    })
    .await;
    // The handshake is through on both ends by now (a decoder takes any certificate), so both are keyed; give
    // anything that is let through plenty of time to show.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(pair.mismatches.lock().unwrap().len(), 1, "reported once");
    assert_eq!(pair.at_b.total(), 0, "A sent RTP or RTCP to the impostor");
    assert_eq!(pair.at_a.total(), 0, "A used what the impostor sent");
  }
}

#[test]
fn the_transport_bin_holds_back_what_is_sent_until_the_peer_is_verified() {
  use crate::pipeline::transport::{build_transport_bin, TransportParams};

  init();
  let agent = nice_gst_meet::Agent::new(&glib::MainContext::new(), nice_gst_meet::Compatibility::Rfc5245);
  let stream_id = agent.add_stream(1);
  let identity = DtlsIdentity::generate().unwrap();
  // A server: its encoder is not held back, so the send bin can run without ICE.
  let transport = build_transport_bin(TransportParams {
    agent: &agent,
    stream_id,
    component_id: 1,
    pem: &identity.pem,
    is_client: false,
    peer_fingerprint: Fingerprint {
      algo: HashAlgo::Sha256,
      value: vec![0; 32],
    },
    on_fingerprint_mismatch: Box::new(|_| {}),
    #[cfg(feature = "log-rtp")]
    log_rtp: false,
    #[cfg(feature = "log-rtp")]
    log_rtcp: false,
  })
  .unwrap();
  let send = transport.bins.send;
  let encoder = send
    .children()
    .into_iter()
    .find(|child| child.factory().is_some_and(|factory| factory.name() == "dtlssrtpenc"))
    .unwrap();

  // Added after the gate, so that it sees only what the gate lets pass.
  let passed = Counters::default();
  for (name, counter) in [("rtp_sink_0", &passed.rtp), ("rtcp_sink_0", &passed.rtcp)] {
    let pad = encoder.pads().into_iter().find(|pad| pad.name() == name).unwrap();
    let counter = counter.clone();
    pad.add_probe(gstreamer::PadProbeType::BUFFER, move |_, _| {
      counter.fetch_add(1, Ordering::SeqCst);
      gstreamer::PadProbeReturn::Ok
    });
  }
  let pipeline = gstreamer::Pipeline::new();
  pipeline.add(&send).unwrap();
  let _null = NullOnDrop(pipeline.clone());
  pipeline.set_state(gstreamer::State::Playing).unwrap();

  let _pushers = [
    Pusher::rtp(&send.static_pad("rtp_send_sink").unwrap(), 1),
    Pusher::rtcp(&send.static_pad("rtcp_send_sink").unwrap(), 1),
  ];
  thread::sleep(Duration::from_millis(500));
  assert_eq!(
    passed.total(),
    0,
    "media reached the encoder before the peer was verified"
  );
}

fn held_encoder() -> (NullOnDrop, gstreamer::Element) {
  init();
  let pipeline = gstreamer::Pipeline::new();
  // The encoder is paired with a decoder by the connection id, and does not start without one.
  let connection_id = uuid::Uuid::new_v4().to_string();
  let encoder = gstreamer::ElementFactory::make("dtlssrtpenc")
    .property("connection-id", &connection_id)
    .property("is-client", true)
    .build()
    .unwrap();
  let decoder = gstreamer::ElementFactory::make("dtlssrtpdec")
    .property("connection-id", &connection_id)
    .property("pem", &DtlsIdentity::generate().unwrap().pem)
    .build()
    .unwrap();
  let (send, receive) = (gstreamer::Bin::new(), gstreamer::Bin::new());
  send.add(&encoder).unwrap();
  receive.add(&decoder).unwrap();
  pipeline.add_many([&send, &receive]).unwrap();
  encoder.request_pad_simple("rtp_sink_0").unwrap();
  encoder.set_locked_state(true);
  pipeline.set_state(gstreamer::State::Playing).unwrap();
  assert_eq!(encoder.current_state(), gstreamer::State::Null);
  (NullOnDrop(pipeline), encoder)
}

#[test]
fn the_held_encoder_starts_when_released_and_only_once() {
  use crate::pipeline::transport::EncoderRelease;

  let (pipeline, encoder) = held_encoder();
  let release = EncoderRelease::new(&encoder);
  release.release();
  assert_eq!(encoder.current_state(), gstreamer::State::Playing);
  assert!(!encoder.is_locked_state());

  // Whoever stops it afterwards is not undone by a second report of ICE (Connected, then Ready).
  pipeline.0.set_state(gstreamer::State::Null).unwrap();
  release.release();
  assert_eq!(encoder.current_state(), gstreamer::State::Null);
}

#[test]
fn a_closed_release_never_starts_the_encoder() {
  use crate::pipeline::transport::EncoderRelease;

  let (_pipeline, encoder) = held_encoder();
  let release = EncoderRelease::new(&encoder);
  release.close();
  release.release();
  assert_eq!(encoder.current_state(), gstreamer::State::Null);
  assert!(encoder.is_locked_state());
}

#[test]
fn closing_waits_for_a_release_in_progress_and_stops_later_ones() {
  use crate::pipeline::transport::EncoderRelease;

  for _ in 0..50 {
    let (pipeline, encoder) = held_encoder();
    let release = Arc::new(EncoderRelease::new(&encoder));
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let releasing = thread::spawn({
      let (release, barrier) = (release.clone(), barrier.clone());
      move || {
        barrier.wait();
        release.release();
      }
    });
    barrier.wait();
    release.close();
    // Teardown, as `MediaSession::shutdown` does it after `close`: nothing may start the encoder from here on.
    pipeline.0.set_state(gstreamer::State::Null).unwrap();
    releasing.join().unwrap();
    assert_eq!(encoder.current_state(), gstreamer::State::Null);
  }
}

mod ice {
  use nice_gst_meet as nice;
  use xmpp_parsers::jingle_ice_udp::{Candidate, Type};

  use super::*;
  use crate::{
    jingle::local_candidate_to_xmpp,
    pipeline::{
      ice::{remote_candidates, IceTimeouts, IceTransport},
      main_loop::MainContext,
      RemoteIce,
    },
  };

  fn udp_candidate(addr: SocketAddr) -> Candidate {
    Candidate {
      component: 1,
      foundation: "1".to_owned(),
      generation: 0,
      id: "c".to_owned(),
      ip: addr.ip(),
      port: addr.port(),
      priority: 2_130_706_431,
      protocol: "udp".to_owned(),
      type_: Type::Host,
      rel_addr: None,
      rel_port: None,
      network: None,
    }
  }

  fn tcp_candidate() -> Candidate {
    Candidate {
      protocol: "tcp".to_owned(),
      ..udp_candidate("10.0.0.1:9".parse().unwrap())
    }
  }

  fn remote(candidates: Vec<Candidate>) -> RemoteIce {
    RemoteIce {
      candidates,
      ..offer(vec![]).ice
    }
  }

  /// The states the component reported, with the time since the transport was set up.
  #[derive(Clone)]
  struct States(Arc<Mutex<Vec<(Duration, nice::ComponentState)>>>, Instant);

  impl States {
    fn new() -> Self {
      Self(Arc::default(), Instant::now())
    }

    fn all(&self) -> Vec<nice::ComponentState> {
      self.0.lock().unwrap().iter().map(|(_, state)| *state).collect()
    }

    fn count(&self, wanted: nice::ComponentState) -> usize {
      self.all().into_iter().filter(|state| *state == wanted).count()
    }

    fn when(&self, wanted: nice::ComponentState) -> Option<Duration> {
      self
        .0
        .lock()
        .unwrap()
        .iter()
        .find(|(_, state)| *state == wanted)
        .map(|(at, _)| *at)
    }

    fn connected(&self) -> bool {
      self.count(nice::ComponentState::Connected) + self.count(nice::ComponentState::Ready) > 0
    }
  }

  async fn transport(main: &MainContext, candidates: Vec<Candidate>, timeouts: IceTimeouts) -> (IceTransport, States) {
    init();
    let mut transport = IceTransport::new(main.context(), &[], &remote(candidates), false)
      .await
      .unwrap()
      .with_timeouts(timeouts);
    let states = States::new();
    transport.on_component_state({
      let states = states.clone();
      move |state| {
        let at = states.1.elapsed();
        states.0.lock().unwrap().push((at, state));
      }
    });
    (transport, states)
  }

  fn timeouts(connect: Duration, no_pairs: Duration) -> IceTimeouts {
    IceTimeouts { connect, no_pairs }
  }

  const SECOND: Duration = Duration::from_secs(1);

  #[test]
  fn the_verdict_follows_what_is_known_about_the_component() {
    use crate::pipeline::ice::{Verdict, WatchState};

    let t0 = Instant::now();
    let at = |seconds: u64| t0 + Duration::from_secs(seconds);
    let limits = IceTimeouts {
      connect: Duration::from_secs(30),
      no_pairs: Duration::from_secs(5),
    };
    let started = WatchState {
      gathering_started: Some(t0),
      ..Default::default()
    };
    let is_fail = |verdict: Verdict| matches!(verdict, Verdict::Fail(_));

    // Nothing to watch before gathering starts, and nothing once the component has got where it was going.
    assert_eq!(WatchState::default().verdict(at(100), &limits), Verdict::Done);
    for finished in [
      WatchState {
        connected: true,
        ..Default::default()
      },
      WatchState {
        failed: true,
        ..Default::default()
      },
      WatchState {
        closed: true,
        ..Default::default()
      },
    ] {
      let state = WatchState {
        gathering_started: Some(t0),
        ..finished
      };
      assert_eq!(state.verdict(at(100), &limits), Verdict::Done);
    }

    // Gathering still going: only the deadline counts.
    assert_eq!(started.verdict(at(10), &limits), Verdict::Wait(at(30)));
    assert!(is_fail(started.verdict(at(30), &limits)));

    // Gathering done, no check list: the grace runs from the end of gathering, and each remote candidate that
    // comes in restarts it. Never beyond the deadline.
    let quiet = WatchState {
      gathering_done: Some(at(2)),
      ..started_state(t0)
    };
    assert_eq!(quiet.verdict(at(3), &limits), Verdict::Wait(at(7)));
    assert!(is_fail(quiet.verdict(at(7), &limits)));
    let trickled = WatchState {
      remote_added: Some(at(6)),
      ..quiet
    };
    assert_eq!(trickled.verdict(at(7), &limits), Verdict::Wait(at(11)));
    assert!(is_fail(trickled.verdict(at(11), &limits)));
    let late = WatchState {
      gathering_done: Some(at(2)),
      remote_added: Some(at(28)),
      ..started_state(t0)
    };
    assert_eq!(
      late.verdict(at(29), &limits),
      Verdict::Wait(at(30)),
      "the deadline caps the grace"
    );

    // Checks running: libnice reports the failure itself, and the deadline is the backstop.
    let checking = WatchState {
      gathering_done: Some(at(2)),
      checking: true,
      ..started_state(t0)
    };
    assert_eq!(checking.verdict(at(20), &limits), Verdict::Wait(at(30)));
    assert!(is_fail(checking.verdict(at(31), &limits)));

    // A gather that could not start is a failure now.
    let broken = WatchState {
      gather_failed: true,
      ..started_state(t0)
    };
    assert!(is_fail(broken.verdict(at(0), &limits)));
  }

  fn started_state(at: Instant) -> crate::pipeline::ice::WatchState {
    crate::pipeline::ice::WatchState {
      gathering_started: Some(at),
      ..Default::default()
    }
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn an_offer_of_candidates_we_cannot_use_fails_after_the_grace_and_not_the_deadline() {
    let main = MainContext::new(None).unwrap();
    // Only a TCP candidate, which is filtered out: libnice never has a pair, and would stay silent for good.
    let (mut transport, states) = transport(&main, vec![tcp_candidate()], timeouts(60 * SECOND, SECOND / 2)).await;
    let started = Instant::now();
    transport.gather(5 * SECOND).await;
    wait_until("the failure", 20 * SECOND, || {
      states.count(nice::ComponentState::Failed) > 0
    })
    .await;
    assert!(started.elapsed() < 20 * SECOND);
    tokio::time::sleep(SECOND).await;
    assert_eq!(states.count(nice::ComponentState::Failed), 1, "reported once");
    assert!(!states.connected());
    transport.close(SECOND).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn an_offer_without_candidates_fails_when_none_arrive() {
    let main = MainContext::new(None).unwrap();
    let (mut transport, states) = transport(&main, vec![], timeouts(60 * SECOND, SECOND)).await;
    transport.gather(5 * SECOND).await;
    wait_until("the failure", 20 * SECOND, || {
      states.count(nice::ComponentState::Failed) > 0
    })
    .await;
    transport.close(SECOND).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn a_peer_that_never_answers_fails_at_the_connect_deadline() {
    let main = MainContext::new(None).unwrap();
    // The discard port; nobody answers. Whether a pair exists depends on the host's interfaces, the grace is out of
    // the picture.
    let candidate = udp_candidate("127.0.0.1:9".parse().unwrap());
    let (mut transport, states) = transport(&main, vec![candidate], timeouts(2 * SECOND, 60 * SECOND)).await;
    transport.gather(5 * SECOND).await;
    wait_until("the failure", 20 * SECOND, || {
      states.count(nice::ComponentState::Failed) > 0
    })
    .await;
    // Late, because the clock starts with the gathering.
    let at = states.when(nice::ComponentState::Failed).unwrap();
    assert!(at >= 2 * SECOND, "failed after {at:?}");
    transport.close(SECOND).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn gathering_that_cannot_start_fails_at_once() {
    let main = MainContext::new(None).unwrap();
    let (mut transport, states) = transport(&main, vec![], timeouts(60 * SECOND, 60 * SECOND)).await;
    transport.agent().remove_stream(transport.stream_id());
    let (candidates, late) = transport.gather(5 * SECOND).await;
    assert!(candidates.is_empty() && late.is_none());
    wait_until("the failure", 10 * SECOND, || {
      states.count(nice::ComponentState::Failed) > 0
    })
    .await;
    assert!(states.when(nice::ComponentState::Failed).unwrap() < 10 * SECOND);
    transport.close(SECOND).await;
  }

  /// Our transport, which is controlled, against a plain agent on the same context. `trickle` offers the peer's
  /// candidates only after gathering, as a transport-info would.
  async fn connect(trickle: bool, timeouts: IceTimeouts) -> Option<(IceTransport, States, nice::Agent, MainContext)> {
    let main = MainContext::new(None).unwrap();
    let context = main.context().clone();
    let peer = nice::Agent::new(&context, nice::Compatibility::Rfc5245);
    peer.set_controlling_mode(true);
    let peer_stream = peer.add_stream(1);
    assert!(peer.attach_recv(peer_stream, 1, &context, |_, _, _, _| {}));
    let (peer_ufrag, peer_pwd) = peer.local_credentials(peer_stream).unwrap();
    assert!(peer.gather_candidates(peer_stream));
    let peer_candidates: Vec<Candidate> = peer
      .local_candidates(peer_stream, 1)
      .iter()
      .filter_map(|candidate| {
        Some(Candidate {
          priority: candidate.priority(),
          ..udp_candidate(candidate.addr()?)
        })
      })
      .collect();
    if peer_candidates.is_empty() {
      eprintln!("no local candidates on this machine, skipping");
      return None;
    }

    let remote = RemoteIce {
      ufrag: peer_ufrag,
      pwd: peer_pwd,
      candidates: if trickle { vec![] } else { peer_candidates.clone() },
      ..offer(vec![]).ice
    };
    let mut ice = IceTransport::new(&context, &[], &remote, false)
      .await
      .unwrap()
      .with_timeouts(timeouts);
    let states = States::new();
    ice.on_component_state({
      let states = states.clone();
      move |state| {
        let at = states.1.elapsed();
        states.0.lock().unwrap().push((at, state));
      }
    });
    // Nothing reads from the agent without a receiver; the session has `nicesrc` for that.
    assert!(ice
      .agent()
      .attach_recv(ice.stream_id(), ice.component_id(), &context, |_, _, _, _| {}));
    let (ufrag, pwd) = ice.local_credentials().unwrap();
    assert!(peer.set_remote_credentials(peer_stream, &ufrag, &pwd));

    let (ours, _) = ice.gather(5 * SECOND).await;
    let ours: Vec<_> = ours.iter().map(local_candidate_to_xmpp).collect();
    let refs = remote_candidates(peer_stream, &ufrag, &pwd, &ours);
    let refs: Vec<_> = refs.iter().collect();
    assert!(peer.set_remote_candidates(peer_stream, 1, &refs) > 0);
    if trickle {
      tokio::time::sleep(SECOND / 2).await;
      let mut late = jitsi_xmpp_parsers::jingle_ice_udp::Transport::new();
      late.candidates = peer_candidates;
      ice.add_remote_candidates(&late).unwrap();
    }
    Some((ice, states, peer, main))
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn a_connecting_session_is_not_failed_by_the_deadlines() {
    let Some((transport, states, _peer, _main)) = connect(false, timeouts(3 * SECOND, SECOND)).await else {
      return;
    };
    wait_until("ICE to connect", 15 * SECOND, || states.connected()).await;
    // Well past both deadlines.
    tokio::time::sleep(4 * SECOND).await;
    assert_eq!(states.count(nice::ComponentState::Failed), 0, "{:?}", states.all());
    transport.close(SECOND).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn candidates_that_arrive_within_the_grace_keep_the_session_alive() {
    // The offer had none; they come with a transport-info, before the grace is over.
    let Some((transport, states, _peer, _main)) = connect(true, timeouts(30 * SECOND, 4 * SECOND)).await else {
      return;
    };
    wait_until("ICE to connect", 15 * SECOND, || states.connected()).await;
    assert_eq!(states.count(nice::ComponentState::Failed), 0, "{:?}", states.all());
    transport.close(SECOND).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn closing_reports_back_and_silences_the_callbacks() {
    let main = MainContext::new(None).unwrap();
    // The grace is shorter than the test: a failure that were not cancelled by the close would show.
    let (mut transport, states) = transport(&main, vec![], timeouts(60 * SECOND, SECOND)).await;
    transport.gather(5 * SECOND).await;
    let started = Instant::now();
    transport.close(10 * SECOND).await;
    assert!(started.elapsed() < 5 * SECOND, "nothing to wait for without TURN");
    tokio::time::sleep(2 * SECOND).await;
    assert_eq!(states.count(nice::ComponentState::Failed), 0, "{:?}", states.all());
  }
}

/// Just enough of a TURN server (RFC 5766, over UDP, long-term credentials) to allocate and to be told that the
/// allocation is released.
mod fake_turn {
  use std::{
    net::{SocketAddr, UdpSocket},
    sync::{
      atomic::{AtomicBool, Ordering},
      Arc, Mutex,
    },
    thread,
    time::Duration,
  };

  const COOKIE: u32 = 0x2112_A442;
  const REALM: &str = "turn.test";
  const NONCE: &str = "0123456789abcdef";
  pub(super) const USERNAME: &str = "user";
  pub(super) const PASSWORD: &str = "password";

  pub(super) struct Server {
    pub(super) addr: SocketAddr,
    pub(super) events: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
  }

  impl Server {
    /// Listens on the address this host would use to reach the network, which libnice, that ignores loopback, can
    /// reach from its own addresses. `None` if the host has no such address.
    pub(super) fn start() -> Option<Self> {
      let routable = UdpSocket::bind("0.0.0.0:0").ok()?;
      routable.connect("192.0.2.1:9").ok()?;
      let socket = UdpSocket::bind((routable.local_addr().ok()?.ip(), 0)).ok()?;
      socket.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
      let addr = socket.local_addr().unwrap();
      let events = Arc::new(Mutex::new(Vec::new()));
      let stop = Arc::new(AtomicBool::new(false));
      let thread = thread::spawn({
        let (events, stop) = (events.clone(), stop.clone());
        move || {
          let mut buffer = [0u8; 2048];
          while !stop.load(Ordering::SeqCst) {
            let Ok((n, from)) = socket.recv_from(&mut buffer) else {
              continue;
            };
            if let Some((reply, event)) = respond(&buffer[..n], from) {
              if let Some(event) = event {
                events.lock().unwrap().push(event);
              }
              let _ = socket.send_to(&reply, from);
            }
          }
        }
      });
      Some(Self {
        addr,
        events,
        stop,
        thread: Some(thread),
      })
    }

    pub(super) fn saw(&self, event: &str) -> bool {
      self.events.lock().unwrap().iter().any(|seen| seen == event)
    }
  }

  impl Drop for Server {
    fn drop(&mut self) {
      self.stop.store(true, Ordering::SeqCst);
      if let Some(thread) = self.thread.take() {
        let _ = thread.join();
      }
    }
  }

  fn attributes(message: &[u8]) -> Vec<(u16, &[u8])> {
    let mut found = Vec::new();
    let mut at = 20;
    while at + 4 <= message.len() {
      let kind = u16::from_be_bytes([message[at], message[at + 1]]);
      let len = u16::from_be_bytes([message[at + 2], message[at + 3]]) as usize;
      let Some(value) = message.get(at + 4..at + 4 + len) else {
        break;
      };
      found.push((kind, value));
      at += 4 + len.div_ceil(4) * 4;
    }
    found
  }

  fn attribute(out: &mut Vec<u8>, kind: u16, value: &[u8]) {
    out.extend(kind.to_be_bytes());
    out.extend((value.len() as u16).to_be_bytes());
    out.extend(value);
    out.resize(out.len().div_ceil(4) * 4, 0);
  }

  fn xor_address(addr: SocketAddr) -> Vec<u8> {
    let SocketAddr::V4(addr) = addr else {
      unreachable!("the test server is IPv4");
    };
    let mut value = vec![0, 1];
    value.extend((addr.port() ^ (COOKIE >> 16) as u16).to_be_bytes());
    value.extend((u32::from(*addr.ip()) ^ COOKIE).to_be_bytes());
    value
  }

  fn message(kind: u16, transaction: &[u8], body: Vec<u8>) -> Vec<u8> {
    let mut out = kind.to_be_bytes().to_vec();
    out.extend((body.len() as u16).to_be_bytes());
    out.extend(COOKIE.to_be_bytes());
    out.extend(transaction);
    out.extend(body);
    out
  }

  /// Appends MESSAGE-INTEGRITY, keyed as the long-term credential mechanism says.
  fn with_integrity(mut out: Vec<u8>) -> Vec<u8> {
    let key = md5(format!("{USERNAME}:{REALM}:{PASSWORD}").as_bytes());
    let length = (out.len() - 20 + 24) as u16;
    out[2..4].copy_from_slice(&length.to_be_bytes());
    let tag = ring::hmac::sign(
      &ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, &key),
      &out,
    );
    attribute(&mut out, 0x0008, tag.as_ref());
    out
  }

  /// The answer to `request` and what to note about it.
  fn respond(request: &[u8], from: SocketAddr) -> Option<(Vec<u8>, Option<String>)> {
    if request.len() < 20 || request[4..8] != COOKIE.to_be_bytes() {
      return None;
    }
    let kind = u16::from_be_bytes([request[0], request[1]]);
    let transaction = &request[8..20];
    let attributes = attributes(request);
    let find = |wanted: u16| {
      attributes
        .iter()
        .find(|(kind, _)| *kind == wanted)
        .map(|(_, value)| *value)
    };
    match kind {
      // Allocate: the first request has no credentials and is turned down with the realm and a nonce.
      0x0003 if find(0x0006).is_none() => {
        let mut body = Vec::new();
        attribute(&mut body, 0x0009, &[0, 0, 4, 1, b'U', b'n', b'a', b'u', b't', b'h']);
        attribute(&mut body, 0x0014, REALM.as_bytes());
        attribute(&mut body, 0x0015, NONCE.as_bytes());
        Some((message(0x0113, transaction, body), None))
      },
      0x0003 => {
        let mut body = Vec::new();
        attribute(&mut body, 0x0016, &xor_address("127.0.0.1:50000".parse().unwrap()));
        attribute(&mut body, 0x0020, &xor_address(from));
        attribute(&mut body, 0x000D, &600u32.to_be_bytes());
        Some((
          with_integrity(message(0x0103, transaction, body)),
          Some("allocated".to_owned()),
        ))
      },
      // Refresh; a lifetime of zero is the release.
      0x0004 => {
        let lifetime = find(0x000D).map(|value| u32::from_be_bytes(value.try_into().unwrap_or_default()));
        let mut body = Vec::new();
        attribute(&mut body, 0x000D, &lifetime.unwrap_or(600).to_be_bytes());
        let event = match lifetime {
          Some(0) => "released",
          _ => "refreshed",
        };
        Some((
          with_integrity(message(0x0104, transaction, body)),
          Some(event.to_owned()),
        ))
      },
      _ => None,
    }
  }

  /// RFC 1321; the long-term credential key is the MD5 of `user:realm:password`.
  fn md5(data: &[u8]) -> [u8; 16] {
    const SHIFTS: [u32; 64] = [
      7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14,
      20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6,
      10, 15, 21,
    ];
    let constants: Vec<u32> = (0..64)
      .map(|i| ((i as f64 + 1.0).sin().abs() * 4_294_967_296.0) as u32)
      .collect();
    let mut padded = data.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
      padded.push(0);
    }
    padded.extend(((data.len() as u64) * 8).to_le_bytes());
    let (mut a0, mut b0, mut c0, mut d0) = (0x6745_2301u32, 0xefcd_ab89u32, 0x98ba_dcfeu32, 0x1032_5476u32);
    for chunk in padded.chunks(64) {
      let words: Vec<u32> = chunk
        .chunks(4)
        .map(|word| u32::from_le_bytes(word.try_into().unwrap()))
        .collect();
      let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
      for i in 0..64 {
        let (f, g) = match i / 16 {
          0 => ((b & c) | (!b & d), i),
          1 => ((d & b) | (!d & c), (5 * i + 1) % 16),
          2 => (b ^ c ^ d, (3 * i + 5) % 16),
          _ => (c ^ (b | !d), (7 * i) % 16),
        };
        let rotated = a
          .wrapping_add(f)
          .wrapping_add(constants[i])
          .wrapping_add(words[g])
          .rotate_left(SHIFTS[i]);
        (a, d, c) = (d, c, b);
        b = b.wrapping_add(rotated);
      }
      a0 = a0.wrapping_add(a);
      b0 = b0.wrapping_add(b);
      c0 = c0.wrapping_add(c);
      d0 = d0.wrapping_add(d);
    }
    let mut digest = [0u8; 16];
    for (out, word) in digest.chunks_mut(4).zip([a0, b0, c0, d0]) {
      out.copy_from_slice(&word.to_le_bytes());
    }
    digest
  }

  #[test]
  fn md5_matches_the_reference_digests() {
    assert_eq!(hex::encode(md5(b"")), "d41d8cd98f00b204e9800998ecf8427e");
    assert_eq!(hex::encode(md5(b"abc")), "900150983cd24fb0d6963f7d28e17f72");
    assert_eq!(
      hex::encode(md5(
        b"12345678901234567890123456789012345678901234567890123456789012345678901234567890"
      )),
      "57edf4a22be3c955ac49da2e2107b67a"
    );
  }
}

/// A transport that has allocated on a fake TURN server. `None` on a machine that cannot do that.
async fn allocated_transport() -> Option<(
  fake_turn::Server,
  crate::pipeline::main_loop::MainContext,
  crate::pipeline::ice::IceTransport,
)> {
  use nice_gst_meet as nice;

  use crate::{
    pipeline::{ice::IceTransport, main_loop::MainContext},
    xmpp::extdisco::Service,
  };

  init();
  let Some(server) = fake_turn::Server::start() else {
    eprintln!("no network address on this machine, skipping");
    return None;
  };
  let main = MainContext::new(None).unwrap();
  let service = Service {
    r#type: "turn".to_owned(),
    host: server.addr.ip().to_string(),
    port: Some(server.addr.port()),
    transport: None,
    username: Some(fake_turn::USERNAME.to_owned()),
    password: Some(fake_turn::PASSWORD.to_owned()),
  };
  let remote = crate::pipeline::RemoteIce {
    candidates: vec![],
    ..offer(vec![]).ice
  };
  let mut transport = IceTransport::new(main.context(), &[service], &remote, false)
    .await
    .unwrap();
  // Nothing reads from the agent without a receiver; the session has `nicesrc` for that.
  assert!(transport.agent().attach_recv(
    transport.stream_id(),
    transport.component_id(),
    main.context(),
    |_, _, _, _| {}
  ));
  let (candidates, _) = transport.gather(Duration::from_secs(10)).await;
  if !candidates
    .iter()
    .any(|candidate| candidate.candidate_type == nice::CandidateType::Host)
  {
    eprintln!("no local candidates on this machine, skipping");
    return None;
  }
  assert!(
    candidates
      .iter()
      .any(|candidate| candidate.candidate_type == nice::CandidateType::Relayed),
    "libnice did not accept the allocation: {candidates:?}"
  );
  assert!(server.saw("allocated") && !server.saw("released"));
  Some((server, main, transport))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_the_transport_gives_the_turn_allocation_back() {
  let Some((server, _main, transport)) = allocated_transport().await else {
    return;
  };
  let started = Instant::now();
  transport.close(Duration::from_secs(10)).await;
  assert!(
    server.saw("released"),
    "the allocation was not released, the server saw {:?}",
    server.events.lock().unwrap()
  );
  // libnice stops listening once the stream is gone, so it is the retransmissions running out that it waits for,
  // about two seconds; the wait ends there.
  assert!(
    started.elapsed() < Duration::from_secs(5),
    "closing took {:?}",
    started.elapsed()
  );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_is_dropped_gives_the_turn_allocation_back_too() {
  let Some((server, _main, transport)) = allocated_transport().await else {
    return;
  };
  drop(transport);
  wait_until("the release", Duration::from_secs(10), || server.saw("released")).await;
}

mod tunnel {
  use tracing::{field::Visit, Level};

  use super::*;
  use crate::pipeline::turn_tls::{Accept, TlsTunnel};

  /// Collects what is logged on the thread that installs it.
  #[derive(Clone, Default)]
  struct Log(Arc<Mutex<Vec<(Level, String)>>>);

  impl Log {
    fn contains(&self, level: Level, text: &str) -> bool {
      self
        .0
        .lock()
        .unwrap()
        .iter()
        .any(|(logged, message)| *logged == level && message.contains(text))
    }
  }

  struct Message(String);

  impl Visit for Message {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
      if field.name() == "message" {
        self.0 = format!("{value:?}");
      }
    }
  }

  impl tracing::Subscriber for Log {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
      true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
      tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
      let mut message = Message(String::new());
      event.record(&mut message);
      self.0.lock().unwrap().push((*event.metadata().level(), message.0));
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
  }

  /// Runs `test` on a runtime whose tasks all run on this thread, which is where the log is captured.
  fn with_log(test: impl std::future::Future<Output = ()>) -> Log {
    let log = Log::default();
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap();
    tracing::subscriber::with_default(log.clone(), || runtime.block_on(test));
    log
  }

  async fn shouting_server() -> SocketAddr {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
      while let Ok((mut connection, _)) = server.accept().await {
        tokio::spawn(async move {
          let mut buffer = [0u8; 64];
          while let Ok(n) = connection.read(&mut buffer).await {
            if n == 0 {
              break;
            }
            buffer[..n].make_ascii_uppercase();
            if connection.write_all(&buffer[..n]).await.is_err() {
              break;
            }
          }
        });
      }
    });
    addr
  }

  async fn shout(stream: &mut TcpStream, text: &str) -> String {
    stream.write_all(text.as_bytes()).await.unwrap();
    let mut answer = vec![0u8; text.len()];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut answer))
      .await
      .expect("the relay answers")
      .unwrap();
    String::from_utf8(answer).unwrap()
  }

  /// Hands out the connections the test provides, and the errors it provides in between.
  struct Scripted(mpsc::UnboundedReceiver<io::Result<TcpStream>>);

  impl Accept for Scripted {
    async fn accept(&mut self) -> io::Result<TcpStream> {
      match self.0.recv().await {
        Some(result) => result,
        None => std::future::pending().await,
      }
    }
  }

  /// A connected pair of TCP streams: the first end is what the tunnel accepts, the second what libnice holds.
  async fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
    let (accepted, _) = listener.accept().await.unwrap();
    (accepted, client)
  }

  #[test]
  fn an_accept_error_does_not_end_the_tunnel_or_the_relays_it_has() {
    let log = with_log(async {
      let target = shouting_server().await;
      let (tx, rx) = mpsc::unbounded_channel();
      let tunnel = TlsTunnel::serve(
        Scripted(rx),
        "127.0.0.1:1".parse().unwrap(),
        target,
        Duration::from_secs(5),
        |tcp| async move { Ok(tcp) },
      );

      let (accepted, mut first) = pair().await;
      tx.send(Ok(accepted)).unwrap();
      assert_eq!(shout(&mut first, "one").await, "ONE");

      // What a process out of file descriptors gets, twice in a row.
      for _ in 0..2 {
        tx.send(Err(io::Error::from_raw_os_error(24))).unwrap();
      }
      let (accepted, mut second) = pair().await;
      tx.send(Ok(accepted)).unwrap();
      assert_eq!(
        shout(&mut second, "two").await,
        "TWO",
        "new connections are relayed after the errors"
      );
      assert_eq!(
        shout(&mut first, "again").await,
        "AGAIN",
        "the relay from before went on"
      );
      drop(tunnel);
    });
    assert!(
      log.contains(Level::WARN, "failed to accept"),
      "{:?}",
      log.0.lock().unwrap()
    );
  }

  #[test]
  fn a_failed_tls_handshake_is_logged_at_warn_with_its_cause() {
    let log = with_log(async {
      let target = shouting_server().await;
      let tunnel = TlsTunnel::start(target, |tcp| async move {
        let _ = &tcp;
        Err::<TcpStream, _>(anyhow::anyhow!("the certificate is not trusted"))
      })
      .await
      .unwrap();
      let mut libnice = TcpStream::connect(tunnel.local_addr()).await.unwrap();
      let mut rest = Vec::new();
      let _ = tokio::time::timeout(Duration::from_secs(5), libnice.read_to_end(&mut rest)).await;
      assert!(rest.is_empty());
    });
    assert!(
      log.contains(Level::WARN, "the certificate is not trusted"),
      "{:?}",
      log.0.lock().unwrap()
    );
  }

  #[test]
  fn a_handshake_that_never_finishes_is_given_up_on_and_logged() {
    let log = with_log(async {
      let target = shouting_server().await;
      let (tx, rx) = mpsc::unbounded_channel();
      let tunnel = TlsTunnel::serve(
        Scripted(rx),
        "127.0.0.1:1".parse().unwrap(),
        target,
        Duration::from_millis(200),
        |tcp| async move {
          let _ = &tcp;
          std::future::pending::<anyhow::Result<TcpStream>>().await
        },
      );
      let (accepted, mut libnice) = pair().await;
      tx.send(Ok(accepted)).unwrap();
      let mut rest = Vec::new();
      tokio::time::timeout(Duration::from_secs(5), libnice.read_to_end(&mut rest))
        .await
        .expect("the connection of libnice is closed once the setup is given up on")
        .unwrap();
      drop(tunnel);
    });
    assert!(
      log.contains(Level::WARN, "was not established within"),
      "{:?}",
      log.0.lock().unwrap()
    );
  }

  #[test]
  fn a_server_that_cannot_be_reached_is_logged_at_warn() {
    let log = with_log(async {
      // Nothing listens there any more.
      let closed = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
      };
      let tunnel = TlsTunnel::start(closed, |tcp| async move { Ok(tcp) }).await.unwrap();
      let mut libnice = TcpStream::connect(tunnel.local_addr()).await.unwrap();
      let mut rest = Vec::new();
      let _ = tokio::time::timeout(Duration::from_secs(5), libnice.read_to_end(&mut rest)).await;
    });
    assert!(
      log.contains(Level::WARN, "could not connect"),
      "{:?}",
      log.0.lock().unwrap()
    );
  }

  #[cfg(any(feature = "tls-rustls-native-roots", feature = "tls-rustls-webpki-roots"))]
  mod over_tls {
    use std::sync::Arc;

    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

    use super::*;
    use crate::tls::TlsClient;

    /// A TLS server for `localhost` whose certificate was issued by a CA of its own, which is returned.
    async fn tls_server() -> (SocketAddr, CertificateDer<'static>) {
      let ca_key = rcgen::KeyPair::generate().unwrap();
      let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
      ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
      ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "transport test CA");
      let ca = ca_params.self_signed(&ca_key).unwrap();
      let issuer = rcgen::Issuer::new(ca_params, ca_key);
      let leaf_key = rcgen::KeyPair::generate().unwrap();
      let leaf = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
        .unwrap()
        .signed_by(&leaf_key, &issuer)
        .unwrap();

      let config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
          vec![leaf.der().clone()],
          PrivatePkcs8KeyDer::from(leaf_key.serialize_der()).into(),
        )
        .unwrap();
      let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let addr = listener.local_addr().unwrap();
      tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
          let acceptor = acceptor.clone();
          tokio::spawn(async move {
            let Ok(mut tls) = acceptor.accept(tcp).await else {
              return;
            };
            let mut buffer = [0u8; 64];
            while let Ok(n) = tls.read(&mut buffer).await {
              if n == 0 {
                break;
              }
              buffer[..n].make_ascii_uppercase();
              if tls.write_all(&buffer[..n]).await.is_err() {
                break;
              }
            }
          });
        }
      });
      (addr, ca.der().clone())
    }

    #[test]
    fn one_client_serves_every_connection_of_the_tunnel() {
      let log = with_log(async {
        let (server, ca) = tls_server().await;
        // Built once for the tunnel, as `add_relay` does; the connections only borrow it.
        let client = Arc::new(TlsClient::trusting(vec![ca]).unwrap());
        let tunnel = TlsTunnel::start(server, move |tcp| {
          let client = client.clone();
          async move { client.connect("localhost", tcp).await }
        })
        .await
        .unwrap();
        for text in ["allocate", "refresh", "channelbind"] {
          let mut libnice = TcpStream::connect(tunnel.local_addr()).await.unwrap();
          assert_eq!(shout(&mut libnice, text).await, text.to_uppercase());
        }
      });
      assert!(!log.contains(Level::WARN, ""), "{:?}", log.0.lock().unwrap());
    }

    #[test]
    fn a_server_the_client_does_not_trust_is_reported_at_warn() {
      let log = with_log(async {
        let (server, _ca) = tls_server().await;
        let client = Arc::new(TlsClient::trusting(vec![]).unwrap());
        let tunnel = TlsTunnel::start(server, move |tcp| {
          let client = client.clone();
          async move { client.connect("localhost", tcp).await }
        })
        .await
        .unwrap();
        let mut libnice = TcpStream::connect(tunnel.local_addr()).await.unwrap();
        let mut rest = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), libnice.read_to_end(&mut rest)).await;
        assert!(rest.is_empty());
      });
      assert!(
        log.contains(Level::WARN, "TLS handshake with localhost failed"),
        "{:?}",
        log.0.lock().unwrap()
      );
    }
  }
}
