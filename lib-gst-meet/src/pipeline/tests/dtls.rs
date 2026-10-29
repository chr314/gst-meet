use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
  },
  time::Duration,
};

use gstreamer::prelude::*;
use xmpp_parsers::hashes::Algo;

use super::support::*;
use crate::pipeline::{
  dtls::{gate_decoder, verify_fingerprint, DtlsIdentity},
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

const ALGOS: [HashAlgo; 5] = [
  HashAlgo::Sha1,
  HashAlgo::Sha224,
  HashAlgo::Sha256,
  HashAlgo::Sha384,
  HashAlgo::Sha512,
];

#[test]
fn a_certificate_is_checked_with_the_hash_algorithm_of_the_offer() {
  let identity = DtlsIdentity::generate().unwrap();
  let der = certificate_der(&identity);
  let other = DtlsIdentity::generate().unwrap();
  for algo in ALGOS {
    let expected = Fingerprint {
      algo,
      value: algo.digest(&der),
    };
    verify_fingerprint(&expected, &identity.pem).unwrap_or_else(|e| panic!("{algo:?}: {e:#}"));
    let message = verify_fingerprint(&expected, &other.pem).unwrap_err().to_string();
    assert!(message.contains(&format!("{algo:?}")), "{message}");
  }
}

#[test]
fn digests_have_the_length_of_their_algorithm() {
  let lengths: Vec<_> = ALGOS.iter().map(|algo| algo.digest(b"x").len()).collect();
  assert_eq!(lengths, [20, 28, 32, 48, 64]);
  // The well known SHA-256 of the empty string, so that the digests are not just the right length.
  assert_eq!(
    hex::encode(HashAlgo::Sha256.digest(b"")),
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  );
  assert_eq!(
    hex::encode(HashAlgo::Sha1.digest(b"")),
    "da39a3ee5e6b4b0d3255bfef95601890afd80709"
  );
}

#[test]
fn certificates_that_are_not_certificates_do_not_verify() {
  let expected = Fingerprint {
    algo: HashAlgo::Sha256,
    value: vec![0; 32],
  };
  assert!(verify_fingerprint(&expected, "").is_err());
  assert!(verify_fingerprint(&expected, "not a pem").is_err());
  let key_only = DtlsIdentity::generate()
    .unwrap()
    .pem
    .replace("CERTIFICATE", "SOMETHING ELSE");
  assert!(verify_fingerprint(&expected, &key_only).is_err());
}

#[test]
fn fingerprint_hashes_come_from_the_xmpp_names() {
  for (algo, expected) in [
    (Algo::Sha_1, HashAlgo::Sha1),
    (Algo::Sha_256, HashAlgo::Sha256),
    (Algo::Sha_512, HashAlgo::Sha512),
    (Algo::Unknown("sha-224".to_owned()), HashAlgo::Sha224),
    (Algo::Unknown("SHA-384".to_owned()), HashAlgo::Sha384),
  ] {
    assert_eq!(HashAlgo::from_xmpp(&algo).unwrap(), expected, "{algo:?}");
  }
  assert!(HashAlgo::from_xmpp(&Algo::Unknown("md5".to_owned())).is_err());
  assert!(HashAlgo::from_xmpp(&Algo::Sha3_256).is_err());
}

/// What reaches the decoded RTP output of the receiving end of a real DTLS-SRTP connection, and what the mismatch
/// callback was told.
struct Handshake {
  pipeline: gstreamer::Pipeline,
  received: Arc<AtomicUsize>,
  mismatches: Arc<Mutex<Vec<String>>>,
}

impl Drop for Handshake {
  fn drop(&mut self) {
    let _ = self.pipeline.set_state(gstreamer::State::Null);
  }
}

/// A client sends RTP to a server through `dtlssrtpenc` and `dtlssrtpdec`, directly linked, and the server's
/// decoder is gated on `expected`.
fn handshake(client: &DtlsIdentity, server: &DtlsIdentity, expected: Fingerprint) -> Handshake {
  init();
  let pipeline = gstreamer::Pipeline::new();

  // Connection ids are process wide: tests run side by side.
  let (client_id, server_id) = (uuid::Uuid::new_v4().to_string(), uuid::Uuid::new_v4().to_string());
  let encoder = |connection_id: &str, is_client: bool| {
    gstreamer::ElementFactory::make("dtlssrtpenc")
      .property("connection-id", connection_id)
      .property("is-client", is_client)
      .build()
      .unwrap()
  };
  let decoder = |connection_id: &str, identity: &DtlsIdentity| {
    gstreamer::ElementFactory::make("dtlssrtpdec")
      .property("connection-id", connection_id)
      .property("pem", &identity.pem)
      .build()
      .unwrap()
  };
  let make = |factory: &str| gstreamer::ElementFactory::make(factory).build().unwrap();
  let (client_enc, client_dec) = (encoder(&client_id, true), decoder(&client_id, client));
  let (server_enc, server_dec) = (encoder(&server_id, false), decoder(&server_id, server));
  let source = gstreamer::ElementFactory::make("audiotestsrc")
    .property("is-live", true)
    .build()
    .unwrap();
  let (convert, opus, pay) = (make("audioconvert"), make("opusenc"), make("rtpopuspay"));
  let sink = gstreamer::ElementFactory::make("fakesink")
    .property("sync", false)
    .property("async", false)
    .build()
    .unwrap();
  pipeline
    .add_many([
      &client_enc,
      &client_dec,
      &server_enc,
      &server_dec,
      &source,
      &convert,
      &opus,
      &pay,
      &sink,
    ])
    .unwrap();
  gstreamer::Element::link_many([&source, &convert, &opus, &pay]).unwrap();
  pay.link_pads(None, &client_enc, Some("rtp_sink_0")).unwrap();
  client_enc.link_pads(Some("src"), &server_dec, Some("sink")).unwrap();
  server_enc.link_pads(Some("src"), &client_dec, Some("sink")).unwrap();
  server_dec.link_pads(Some("rtp_src"), &sink, None).unwrap();
  // The server has nothing to send, but its encoder carries the handshake.
  let _ = server_enc.request_pad_simple("rtp_sink_0").unwrap();

  let received = Arc::new(AtomicUsize::new(0));
  let mismatches = Arc::new(Mutex::new(Vec::new()));
  gate_decoder(&server_dec, expected, {
    let mismatches = mismatches.clone();
    move |reason| mismatches.lock().unwrap().push(reason)
  })
  .unwrap();
  {
    // After the gate: only what it let through is counted.
    let received = received.clone();
    sink
      .static_pad("sink")
      .unwrap()
      .add_probe(gstreamer::PadProbeType::BUFFER, move |_, _| {
        received.fetch_add(1, Ordering::SeqCst);
        gstreamer::PadProbeReturn::Ok
      });
  }
  pipeline.set_state(gstreamer::State::Playing).unwrap();
  Handshake {
    pipeline,
    received,
    mismatches,
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn media_flows_once_the_peer_certificate_matches_its_fingerprint() {
  let (client, server) = (DtlsIdentity::generate().unwrap(), DtlsIdentity::generate().unwrap());
  let expected = Fingerprint {
    algo: HashAlgo::Sha384,
    value: HashAlgo::Sha384.digest(&certificate_der(&client)),
  };
  let handshake = handshake(&client, &server, expected);
  wait_until("decrypted media", Duration::from_secs(20), || {
    handshake.received.load(Ordering::SeqCst) > 5
  })
  .await;
  assert!(handshake.mismatches.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_is_let_through_when_the_peer_certificate_does_not_match() {
  let (client, server) = (DtlsIdentity::generate().unwrap(), DtlsIdentity::generate().unwrap());
  let expected = Fingerprint {
    algo: HashAlgo::Sha256,
    value: HashAlgo::Sha256.digest(&certificate_der(&server)),
  };
  let handshake = handshake(&client, &server, expected);
  wait_until("the mismatch to be reported", Duration::from_secs(20), || {
    !handshake.mismatches.lock().unwrap().is_empty()
  })
  .await;
  tokio::time::sleep(Duration::from_secs(1)).await;
  assert_eq!(handshake.received.load(Ordering::SeqCst), 0);
  assert_eq!(
    handshake.mismatches.lock().unwrap().len(),
    1,
    "the mismatch is reported once"
  );
}

#[test]
fn the_transport_is_paired_by_a_unique_connection_id_and_a_client_waits_for_ice() {
  use crate::pipeline::transport::{build_transport_bin, TransportParams};

  init();
  let agent = nice_gst_meet::Agent::new(&glib::MainContext::new(), nice_gst_meet::Compatibility::Rfc5245);
  let stream_id = agent.add_stream(1);
  let identity = DtlsIdentity::generate().unwrap();
  let transport = |is_client| {
    build_transport_bin(TransportParams {
      agent: &agent,
      stream_id,
      component_id: 1,
      pem: &identity.pem,
      is_client,
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
    .unwrap()
  };
  let connection_id = |bins: &crate::pipeline::transport::TransportBins, factory: &str, receive: bool| {
    let bin = if receive { &bins.receive } else { &bins.send };
    bin
      .children()
      .into_iter()
      .find(|child| child.factory().is_some_and(|f| f.name() == factory))
      .unwrap()
      .property::<String>("connection-id")
  };

  let client = transport(true);
  let server = transport(false);
  for bins in [&client.bins, &server.bins] {
    assert!(bins.receive.static_pad("rtp_recv_src").is_some() && bins.receive.static_pad("rtcp_recv_src").is_some());
    assert!(bins.send.static_pad("rtp_send_sink").is_some() && bins.send.static_pad("rtcp_send_sink").is_some());
    assert_eq!(
      connection_id(bins, "dtlssrtpenc", false),
      connection_id(bins, "dtlssrtpdec", true)
    );
  }
  assert_ne!(
    connection_id(&client.bins, "dtlssrtpenc", false),
    connection_id(&server.bins, "dtlssrtpenc", false)
  );

  // The client's encoder, which starts the handshake, is held back until ICE has connected.
  let encoder = client.locked_encoder.expect("a client holds back its encoder");
  assert!(encoder.is_locked_state());
  assert!(server.locked_encoder.is_none());
}
