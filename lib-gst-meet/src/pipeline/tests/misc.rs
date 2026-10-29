use std::{collections::BTreeMap, net::SocketAddr, sync::mpsc, time::Duration};

use gstreamer::prelude::*;
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::TcpListener,
};
use xmpp_parsers::jingle_ice_udp::{Candidate, Type};

use super::support::*;
use crate::{
  conference::ReceiveMode,
  pipeline::{
    bus::{classify_error, ErrorTarget},
    ice::{parse_related_address, remote_candidates, IceTransport},
    main_loop::MainContext,
    offer::participant_id_for_owner,
    owner::{self, Owner, StreamPart},
    rtp::RtxSsrcMap,
    streams::FailureOrigin,
    turn_tls::TlsTunnel,
  },
  xmpp::extdisco::Service,
};

#[test]
fn errors_are_attributed_to_the_closest_owner() {
  init();
  let pipeline = gstreamer::Pipeline::new();
  let identity = || gstreamer::ElementFactory::make("identity").build().unwrap();

  let session_bin = gstreamer::Bin::new();
  owner::mark(&session_bin, Owner::Session { session: 3 });
  let in_session = identity();
  let dtls = gstreamer::ElementFactory::make("dtlssrtpenc").build().unwrap();
  session_bin.add_many([&in_session, &dtls]).unwrap();
  pipeline.add(&session_bin).unwrap();
  assert_eq!(
    classify_error(&pipeline, in_session.upcast_ref()),
    ErrorTarget::Session {
      session: 3,
      dtls: false
    }
  );
  assert_eq!(
    classify_error(&pipeline, dtls.upcast_ref()),
    ErrorTarget::Session { session: 3, dtls: true }
  );

  let chain = gstreamer::Bin::new();
  owner::mark(
    &chain,
    Owner::Stream {
      session: 3,
      stream_id: 9,
      part: StreamPart::Chain(4),
    },
  );
  let in_chain = identity();
  chain.add(&in_chain).unwrap();
  pipeline.add(&chain).unwrap();
  assert_eq!(
    classify_error(&pipeline, in_chain.upcast_ref()),
    ErrorTarget::Stream {
      session: 3,
      stream_id: 9,
      origin: FailureOrigin::Chain(4)
    }
  );

  let (consumer, _) = recording_consumer();
  owner::mark(
    &consumer,
    Owner::Stream {
      session: 3,
      stream_id: 9,
      part: StreamPart::Consumer,
    },
  );
  pipeline.add(&consumer).unwrap();
  let inner_sink = consumer.downcast_ref::<gstreamer::Bin>().unwrap().children()[0].clone();
  assert_eq!(
    classify_error(&pipeline, inner_sink.upcast_ref()),
    ErrorTarget::Stream {
      session: 3,
      stream_id: 9,
      origin: FailureOrigin::Consumer
    }
  );

  // Nobody's: an element of the application's.
  let shared = identity();
  pipeline.add(&shared).unwrap();
  assert_eq!(classify_error(&pipeline, shared.upcast_ref()), ErrorTarget::Shared);

  // An element that has left the pipeline is no longer anybody's, unless it was marked.
  pipeline.remove(&shared).unwrap();
  assert_eq!(classify_error(&pipeline, shared.upcast_ref()), ErrorTarget::Detached);
  pipeline.remove(&chain).unwrap();
  assert_eq!(
    classify_error(&pipeline, in_chain.upcast_ref()),
    ErrorTarget::Stream {
      session: 3,
      stream_id: 9,
      origin: FailureOrigin::Chain(4)
    }
  );
}

#[test]
fn participants_are_the_resource_of_the_owner() {
  assert_eq!(participant_id_for_owner("jvb".to_owned()).unwrap(), None);
  assert_eq!(
    participant_id_for_owner("abcd1234".to_owned()).unwrap().as_deref(),
    Some("abcd1234")
  );
  assert_eq!(
    participant_id_for_owner("room@conference.example.com/abcd1234".to_owned())
      .unwrap()
      .as_deref(),
    Some("abcd1234")
  );
}

fn candidate(protocol: &str, port: u16, type_: Type) -> Candidate {
  Candidate {
    component: 1,
    foundation: "1".to_owned(),
    generation: 0,
    id: "id".to_owned(),
    ip: "192.0.2.1".parse().unwrap(),
    port,
    priority: 100,
    protocol: protocol.to_owned(),
    type_,
    rel_addr: Some("10.0.0.1".parse().unwrap()),
    rel_port: Some(9),
    network: None,
  }
}

#[test]
fn only_udp_candidates_are_handed_to_libnice() {
  init();
  let remote = remote_candidates(
    1,
    "ufrag",
    "pwd",
    &[
      candidate("udp", 10000, Type::Host),
      candidate("tcp", 4443, Type::Host),
      candidate("UDP", 10001, Type::Srflx),
      candidate("ssltcp", 443, Type::Relay),
    ],
  );
  let addrs: Vec<_> = remote.iter().map(|c| c.addr().unwrap().to_string()).collect();
  assert_eq!(addrs, ["192.0.2.1:10000", "192.0.2.1:10001"]);
  for candidate in &remote {
    assert_eq!(candidate.transport(), nice_gst_meet::CandidateTransport::Udp);
    assert_eq!(candidate.username().unwrap(), "ufrag");
    assert_eq!(candidate.password().unwrap(), "pwd");
    assert_eq!(candidate.stream_id(), 1);
  }
  assert_eq!(remote[1].type_(), nice_gst_meet::CandidateType::ServerReflexive);
}

#[test]
fn the_base_of_a_candidate_is_read_from_its_sdp_line() {
  let line = "candidate:2 1 UDP 1677722111 203.0.113.7 40000 typ srflx raddr 10.0.0.5 rport 50000";
  assert_eq!(
    parse_related_address(line),
    Some("10.0.0.5:50000".parse::<SocketAddr>().unwrap())
  );
  assert_eq!(
    parse_related_address("candidate:1 1 UDP 2013266431 10.0.0.5 50000 typ host"),
    None
  );
  assert_eq!(parse_related_address("typ relay raddr garbage rport 1"), None);
}

fn service(kind: &str, host: &str) -> Service {
  Service {
    r#type: kind.to_owned(),
    host: host.to_owned(),
    port: None,
    transport: None,
    username: Some("user".to_owned()),
    password: Some("password".to_owned()),
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn servers_that_cannot_be_resolved_do_not_stop_the_ice_setup() {
  init();
  let remote = offer(vec![]).ice;
  let context = glib::MainContext::new();
  let transport = IceTransport::new(
    &context,
    &[
      service("stun", "stun.invalid"),
      service("turn", "turn.invalid"),
      service("turns", "turns.invalid"),
    ],
    &remote,
    false,
  )
  .await
  .expect("an unresolvable STUN or TURN server is not fatal");
  let (ufrag, pwd) = transport.local_credentials().unwrap();
  assert!(!ufrag.is_empty() && !pwd.is_empty());
  assert!(transport.agent().property::<bool>("consent-freshness"));
  assert!(!transport.agent().is_ice_tcp());
  assert!(!transport.agent().is_controlling_mode());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_turn_tunnel_relays_between_libnice_and_the_server_until_it_is_dropped() {
  let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let target = server.local_addr().unwrap();
  // The "TLS" server upper-cases what it is sent.
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

  let tunnel = TlsTunnel::start(target, |tcp| async move { Ok(tcp) }).await.unwrap();
  let mut client = tokio::net::TcpStream::connect(tunnel.local_addr()).await.unwrap();
  client.write_all(b"allocate").await.unwrap();
  let mut answer = [0u8; 8];
  client.read_exact(&mut answer).await.unwrap();
  assert_eq!(&answer, b"ALLOCATE");

  // A second connection is relayed independently.
  let mut second = tokio::net::TcpStream::connect(tunnel.local_addr()).await.unwrap();
  second.write_all(b"refresh").await.unwrap();
  let mut answer = [0u8; 7];
  second.read_exact(&mut answer).await.unwrap();
  assert_eq!(&answer, b"REFRESH");

  drop(tunnel);
  let closed = tokio::time::timeout(Duration::from_secs(5), async {
    let mut rest = Vec::new();
    client.read_to_end(&mut rest).await
  })
  .await;
  assert!(
    matches!(closed, Ok(Ok(_)) | Ok(Err(_))),
    "the relayed connection should end with the tunnel"
  );
}

#[test]
fn our_own_main_context_is_iterated_by_a_thread_of_ours() {
  init();
  let mut main = MainContext::new(None).unwrap();
  let (tx, rx) = mpsc::channel();
  main.context().invoke(move || {
    let _ = tx.send(std::thread::current().name().map(str::to_owned));
  });
  let thread = rx
    .recv_timeout(Duration::from_secs(5))
    .expect("the context is not iterated");
  assert_eq!(thread.as_deref(), Some("gst-meet-glib"));
  main.stop();
  // Stopped for good: nothing runs on it any more.
  let (tx, rx) = mpsc::channel();
  main.context().invoke(move || {
    let _ = tx.send(());
  });
  assert!(rx.recv_timeout(Duration::from_millis(300)).is_err());
}

#[test]
fn a_main_context_of_the_application_is_left_alone() {
  init();
  let context = glib::MainContext::new();
  let mut main = MainContext::new(Some(context.clone())).unwrap();
  assert!(main.context() == &context);
  main.stop();
  // The application iterates it, so it still can be.
  let (tx, rx) = mpsc::channel();
  context.invoke(move || {
    let _ = tx.send(());
  });
  assert!(context.iteration(false));
  rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn the_ssrc_map_of_rtprtxreceive_follows_the_sources() {
  init();
  let associations: BTreeMap<u32, u32> = [(1000, 1001), (2000, 2001)].into();
  let structure = RtxSsrcMap::structure(&associations);
  assert_eq!(structure.name(), "application/x-rtp-ssrc-map");
  assert_eq!(structure.get::<u32>("1000").unwrap(), 1001);
  assert_eq!(structure.get::<u32>("2000").unwrap(), 2001);

  // The element takes it in the shape we build, before and after it was handed the map.
  let receiver = gstreamer::ElementFactory::make("rtprtxreceive").build().unwrap();
  let map = RtxSsrcMap::default();
  map.insert(3000, 3001);
  map.attach(&receiver);
  map.insert(4000, 4001);
  map.remove(3000);
  map.remove(5000);
}

#[tokio::test]
async fn video_codecs_that_cannot_be_received_are_not_advertised() {
  let present = |factory: &str| gstreamer::ElementFactory::find(factory).is_some();
  let rig = Rig::with_video_codecs(ReceiveMode::Encoded, &["av1", "vp9", "bogus", "vp8", "h264"]);
  let mut expected = Vec::new();
  if present("rtpav1depay") && present("av1parse") {
    expected.push("av1");
  }
  expected.extend(["vp9", "vp8", "h264"]);
  assert_eq!(rig.media.receivable_video_codecs(), expected);

  // In `Decoded` mode it takes decoders instead of parsers.
  let rig = Rig::with_video_codecs(ReceiveMode::Decoded, &["vp8", "bogus"]);
  assert_eq!(rig.media.receivable_video_codecs(), ["vp8"]);
}
