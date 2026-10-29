use std::collections::BTreeSet;

use once_cell::sync::Lazy;
use xmpp_parsers::{
  caps::{self, Caps},
  disco::{DiscoInfoResult, Identity},
  ecaps2::{self, ECaps2},
  hashes::{Algo, Hash},
  ns,
};

pub(crate) const DISCO_NODE: &str = "https://github.com/avstack/gst-meet";

static DISCO_INFO: Lazy<DiscoInfoResult> = Lazy::new(|| DiscoInfoResult {
  node: None,
  identities: vec![Identity::new("client", "bot", "en", "gst-meet")],
  features: [
    ns::DISCO_INFO,
    ns::JINGLE_RTP_AUDIO,
    ns::JINGLE_RTP_VIDEO,
    ns::JINGLE_ICE_UDP,
    ns::JINGLE_DTLS,
    "urn:ietf:rfc:5888", // BUNDLE
    "urn:ietf:rfc:5761", // RTCP-MUX
    "urn:ietf:rfc:4588", // RTX
    "http://jitsi.org/tcc",
    "http://jitsi.org/source-name",
    "http://jitsi.org/json-encoded-sources",
    "http://jitsi.org/receive-multiple-video-streams",
  ]
  .into_iter()
  .map(String::from)
  .collect::<BTreeSet<_>>(),
  extensions: vec![],
});

static COMPUTED_CAPS_HASH: Lazy<Hash> = Lazy::new(|| {
  caps::hash_caps(&caps::compute_disco(&DISCO_INFO), Algo::Sha_1).expect("our capabilities can be hashed")
});

static COMPUTED_ECAPS2_HASH: Lazy<Hash> = Lazy::new(|| {
  ecaps2::hash_ecaps2(
    &ecaps2::compute_disco(&DISCO_INFO).expect("our capabilities can be encoded"),
    Algo::Sha_256,
  )
  .expect("our capabilities can be hashed")
});

/// The XEP-0115 and XEP-0390 capabilities for our presence.
pub(crate) fn presence_caps() -> [minidom::Element; 2] {
  [
    Caps::new(DISCO_NODE, COMPUTED_CAPS_HASH.clone()).into(),
    ECaps2::new(vec![COMPUTED_ECAPS2_HASH.clone()]).into(),
  ]
}

/// The answer to a disco#info query for `node`, if it is a node we have.
pub(crate) fn info(node: Option<&str>) -> Option<DiscoInfoResult> {
  let mut info = DISCO_INFO.clone();
  if let Some(node) = node {
    if node != format!("{DISCO_NODE}#{}", COMPUTED_CAPS_HASH.to_base64()) {
      return None;
    }
    info.node = Some(node.to_owned());
  }
  Some(info)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn does_not_claim_remb() {
    let info = info(None).unwrap();
    assert!(!info.features.iter().any(|feature| feature.contains("remb")));
    assert!(info.features.contains("http://jitsi.org/tcc"));
  }

  #[test]
  fn answers_for_its_own_caps_node_only() {
    let node = format!("{DISCO_NODE}#{}", COMPUTED_CAPS_HASH.to_base64());
    assert_eq!(info(Some(&node)).unwrap().node.as_deref(), Some(node.as_str()));
    assert!(info(Some("https://example.org#abc")).is_none());
    assert!(info(Some(DISCO_NODE)).is_none());
  }
}
