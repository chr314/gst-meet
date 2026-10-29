use xmpp_parsers::{
  jingle_ice_udp::Candidate,
  ns::{JINGLE_DTLS, JINGLE_ICE_UDP},
};

use crate::{jingle_dtls_srtp::Fingerprint, ns::JITSI_COLIBRI};

generate_element!(
  /// Wrapper element for an ICE-UDP transport.
  #[derive(Default)]
  Transport, "transport", JINGLE_ICE_UDP,
  attributes: [
    /// A Password as defined in ICE-CORE.
    pwd: Option<String> = "pwd",

    /// A User Fragment as defined in ICE-CORE.
    ufrag: Option<String> = "ufrag",
  ],
  children: [
    /// List of candidates for this ICE-UDP session.
    candidates: Vec<Candidate> = ("candidate", JINGLE_ICE_UDP) => Candidate,

    /// Fingerprints of the key used for the DTLS handshake, one per hash function (RFC 8122 section 5).
    fingerprints: Vec<Fingerprint> = ("fingerprint", JINGLE_DTLS) => Fingerprint,

    /// Details of the Colibri WebSockets, one per JVB websocket domain.
    web_sockets: Vec<WebSocket> = ("web-socket", JITSI_COLIBRI) => WebSocket
  ]
);

impl Transport {
  /// Create a new ICE-UDP transport.
  pub fn new() -> Transport {
    Default::default()
  }

  /// Add a candidate to this transport.
  pub fn add_candidate(mut self, candidate: Candidate) -> Self {
    self.candidates.push(candidate);
    self
  }

  /// Add a DTLS-SRTP fingerprint to this transport.
  pub fn with_fingerprint(mut self, fingerprint: Fingerprint) -> Self {
    self.fingerprints.push(fingerprint);
    self
  }
}

generate_element!(
  /// Colibri WebSocket details
  WebSocket, "web-socket", JITSI_COLIBRI,
  attributes: [
      /// The WebSocket URL
      url: Required<String> = "url",
  ]
);

#[cfg(test)]
mod tests {
  use minidom::Element;
  use xmpp_parsers::{hashes::Algo, jingle_dtls_srtp::Setup};

  use super::*;

  #[test]
  fn parses_several_web_sockets() {
    let elem: Element = "<transport xmlns='urn:xmpp:jingle:transports:ice-udp:1' ufrag='u' pwd='p'>\
        <web-socket xmlns='http://jitsi.org/protocol/colibri' url='wss://a.example/colibri-ws/1'/>\
        <web-socket xmlns='http://jitsi.org/protocol/colibri' url='wss://b.example/colibri-ws/1'/>\
      </transport>"
      .parse()
      .unwrap();
    let transport = Transport::try_from(elem).unwrap();
    assert_eq!(
      transport
        .web_sockets
        .iter()
        .map(|ws| ws.url.as_str())
        .collect::<Vec<_>>(),
      ["wss://a.example/colibri-ws/1", "wss://b.example/colibri-ws/1"]
    );
  }

  const FINGERPRINT_256: &str =
    "00:01:02:03:04:05:06:07:08:09:0A:0B:0C:0D:0E:0F:10:11:12:13:14:15:16:17:18:19:1A:1B:1C:1D:1E:1F";

  #[test]
  fn parses_several_fingerprints_in_order() {
    let elem: Element = format!(
      "<transport xmlns='urn:xmpp:jingle:transports:ice-udp:1' ufrag='u' pwd='p'>\
         <fingerprint xmlns='urn:xmpp:jingle:apps:dtls:0' hash='sha-256' setup='actpass'>{FINGERPRINT_256}</fingerprint>\
         <fingerprint xmlns='urn:xmpp:jingle:apps:dtls:0' hash='sha-1' setup='actpass'>AA:BB</fingerprint>\
         <fingerprint xmlns='urn:xmpp:jingle:apps:dtls:0' hash='sha-512'>CC:DD</fingerprint>\
       </transport>"
    )
    .parse()
    .unwrap();
    let transport = Transport::try_from(elem).unwrap();
    let hashes: Vec<_> = transport.fingerprints.iter().map(|f| f.hash.clone()).collect();
    assert_eq!(hashes, [Algo::Sha_256, Algo::Sha_1, Algo::Sha_512]);
    assert_eq!(transport.fingerprints[0].value.len(), 32);
    assert_eq!(transport.fingerprints[1].value, [0xAA, 0xBB]);
    assert_eq!(transport.fingerprints[2].setup, None);
  }

  #[test]
  fn serializes_every_fingerprint() {
    let fingerprint = |hash, value: &[u8]| Fingerprint {
      hash,
      setup: Some(Setup::Active),
      value: value.to_vec(),
    };
    let transport = Transport::new()
      .with_fingerprint(fingerprint(Algo::Sha_256, &[1, 2]))
      .with_fingerprint(fingerprint(Algo::Sha_512, &[3, 4]));
    let elem = Element::from(transport);
    let hashes: Vec<_> = elem
      .children()
      .map(|child| child.attr("hash").unwrap().to_owned())
      .collect();
    assert_eq!(hashes, ["sha-256", "sha-512"]);
    let parsed = Transport::try_from(elem).unwrap();
    assert_eq!(parsed.fingerprints.len(), 2);
    assert_eq!(parsed.fingerprints[1].value, [3, 4]);
  }

  #[test]
  fn parses_no_fingerprint() {
    let elem: Element = "<transport xmlns='urn:xmpp:jingle:transports:ice-udp:1'/>"
      .parse()
      .unwrap();
    assert!(Transport::try_from(elem).unwrap().fingerprints.is_empty());
  }

  #[test]
  fn parses_no_web_socket() {
    let elem: Element = "<transport xmlns='urn:xmpp:jingle:transports:ice-udp:1'/>"
      .parse()
      .unwrap();
    assert!(Transport::try_from(elem).unwrap().web_sockets.is_empty());
  }
}
