use minidom::Element;
use xmpp_parsers::ns;

use super::{harness::Harness, xml::*};
use crate::ConferenceLeftReason;

fn error_of(reply: &Element) -> &Element {
  assert_eq!(reply.attr("type"), Some("error"), "{reply:?}");
  reply.get_child("error", ns::DEFAULT_NS).unwrap()
}

fn condition(reply: &Element) -> String {
  error_of(reply)
    .children()
    .find(|child| child.ns() == ns::XMPP_STANZAS && child.name() != "text")
    .map(|condition| condition.name().to_owned())
    .unwrap()
}

#[tokio::test]
async fn jingle_of_the_focus_for_our_session_is_acknowledged() {
  let mut harness = Harness::join().await;
  harness.server.receive(&jingle("info-1", FOCUS, "session-info", "sid-1", "<ringing xmlns='urn:xmpp:jingle:apps:rtp:info:1'/>")).await;
  let reply = harness.server.result_for("info-1").await;
  assert_eq!(reply.attr("type"), Some("result"));
  assert_eq!(reply.attr("to"), Some(FOCUS));
  assert_eq!(reply.attr("from"), Some(MY_JID));
  assert_eq!(reply.children().count(), 0);
}

#[tokio::test]
async fn jingle_that_does_not_parse_is_a_bad_request() {
  let mut harness = Harness::join().await;
  // No sid.
  harness
    .server
    .receive(&format!(
      "<iq xmlns='jabber:client' type='set' id='bad-1' from='{FOCUS}'><jingle xmlns='urn:xmpp:jingle:1' action='source-add'/></iq>"
    ))
    .await;
  let reply = harness.server.result_for("bad-1").await;
  assert_eq!(condition(&reply), "bad-request");
  assert_eq!(error_of(&reply).attr("type"), Some("modify"));

  // An action nobody has heard of.
  harness.server.receive(&jingle("bad-2", FOCUS, "frobnicate", "sid-1", "")).await;
  assert_eq!(condition(&harness.server.result_for("bad-2").await), "bad-request");

  // An IQ without any payload.
  harness
    .server
    .receive(&format!("<iq xmlns='jabber:client' type='set' id='bad-3' from='{FOCUS}'/>"))
    .await;
  assert_eq!(condition(&harness.server.result_for("bad-3").await), "bad-request");
}

#[tokio::test]
async fn jingle_for_an_unknown_session_is_an_error_that_says_so() {
  let mut harness = Harness::join().await;
  for (id, action) in [
    ("unknown-1", "source-add"),
    ("unknown-2", "source-remove"),
    ("unknown-3", "transport-info"),
    ("unknown-4", "session-terminate"),
    ("unknown-5", "transport-replace"),
  ] {
    harness.server.receive(&jingle(id, FOCUS, action, "sid-other", "")).await;
    let reply = harness.server.result_for(id).await;
    assert_eq!(condition(&reply), "item-not-found", "{action}");
    assert_eq!(error_of(&reply).attr("type"), Some("cancel"));
    assert!(error_of(&reply).has_child("unknown-session", "urn:xmpp:jingle:errors:1"), "{action}: {reply:?}");
  }
  assert_eq!(harness.media.count("session 1: shutdown"), 0);
}

#[tokio::test]
async fn jingle_actions_we_do_not_implement_are_refused() {
  let mut harness = Harness::join().await;
  for (id, action) in [
    ("action-1", "content-add"),
    ("action-2", "description-info"),
    ("action-3", "content-modify"),
    ("action-4", "security-info"),
  ] {
    harness.server.receive(&jingle(id, FOCUS, action, "sid-1", "")).await;
    let reply = harness.server.result_for(id).await;
    assert_eq!(condition(&reply), "feature-not-implemented", "{action}");
    assert_eq!(error_of(&reply).attr("type"), Some("cancel"));
  }
}

#[tokio::test]
async fn a_peer_to_peer_session_is_declined() {
  let mut harness = Harness::join().await;
  let peer = format!("{ROOM}/bob");
  harness
    .server
    .receive(&format!(
      "<iq xmlns='jabber:client' type='set' id='p2p-1' from='{peer}'>\
         <jingle xmlns='urn:xmpp:jingle:1' action='session-initiate' initiator='{peer}' sid='p2p-sid'/></iq>"
    ))
    .await;
  harness.server.expect_ack("p2p-1").await;
  let terminate = harness.server.jingle("session-terminate").await;
  assert_eq!(terminate.attr("to"), Some(peer.as_str()));
  assert!(String::from(&terminate).contains("<decline"));

  // Other Jingle from somebody who is not the focus is not for any session of ours.
  harness.server.receive(&jingle("p2p-2", &peer, "source-add", "sid-1", "")).await;
  assert_eq!(condition(&harness.server.result_for("p2p-2").await), "item-not-found");
  assert_eq!(harness.media.count("session 1: shutdown"), 0);
}

#[tokio::test]
async fn an_iq_nobody_knows_is_service_unavailable() {
  let mut harness = Harness::join().await;
  harness
    .server
    .receive(&format!(
      "<iq xmlns='jabber:client' type='get' id='q-1' from='{ROOM}/bob'><query xmlns='jabber:iq:version'/></iq>"
    ))
    .await;
  let reply = harness.server.result_for("q-1").await;
  assert_eq!(condition(&reply), "service-unavailable");
  assert_eq!(error_of(&reply).attr("type"), Some("cancel"));
  harness
    .server
    .receive(&format!(
      "<iq xmlns='jabber:client' type='set' id='q-2' from='{ROOM}/bob'><thing xmlns='urn:example'/></iq>"
    ))
    .await;
  assert_eq!(condition(&harness.server.result_for("q-2").await), "service-unavailable");
}

#[tokio::test]
async fn disco_info_lists_what_we_can_do_and_not_remb() {
  let mut harness = Harness::join().await;
  harness
    .server
    .receive(&format!(
      "<iq xmlns='jabber:client' type='get' id='d-1' from='{ROOM}/bob'><query xmlns='http://jabber.org/protocol/disco#info'/></iq>"
    ))
    .await;
  let reply = harness.server.result_for("d-1").await;
  assert_eq!(reply.attr("type"), Some("result"));
  let features: Vec<String> = reply
    .get_child("query", ns::DISCO_INFO)
    .unwrap()
    .children()
    .filter(|child| child.name() == "feature")
    .map(|feature| feature.attr("var").unwrap().to_owned())
    .collect();
  assert!(features.contains(&"http://jitsi.org/tcc".to_owned()), "{features:?}");
  assert!(features.contains(&"http://jitsi.org/source-name".to_owned()));
  assert!(!features.iter().any(|feature| feature.contains("remb")), "{features:?}");

  // The caps node of our presence is answered, others are not.
  let presence = harness.conference.shared.lock().presence.payloads(false);
  let caps = presence.iter().find(|payload| payload.name() == "c").unwrap();
  let node = format!("{}#{}", caps.attr("node").unwrap(), caps.attr("ver").unwrap());
  harness
    .server
    .receive(&format!(
      "<iq xmlns='jabber:client' type='get' id='d-2' from='{ROOM}/bob'><query xmlns='http://jabber.org/protocol/disco#info' node='{node}'/></iq>"
    ))
    .await;
  let reply = harness.server.result_for("d-2").await;
  assert_eq!(reply.attr("type"), Some("result"));
  assert_eq!(reply.get_child("query", ns::DISCO_INFO).unwrap().attr("node"), Some(node.as_str()));
  harness
    .server
    .receive(&format!(
      "<iq xmlns='jabber:client' type='get' id='d-3' from='{ROOM}/bob'><query xmlns='http://jabber.org/protocol/disco#info' node='urn:other#abc'/></iq>"
    ))
    .await;
  assert_eq!(condition(&harness.server.result_for("d-3").await), "item-not-found");
}

#[tokio::test]
async fn muting_sends_the_presence_without_the_muc_extension() {
  let mut harness = Harness::join().await;
  harness.conference.set_muted(crate::MediaType::Audio, true).await.unwrap();
  let presence = harness.server.presence().await;
  assert_eq!(presence.attr("to"), Some(format!("{ROOM}/{ME}").as_str()));
  assert!(!presence.has_child("x", ns::MUC), "{presence:?}");
  assert_eq!(presence.get_child("audiomuted", ns::DEFAULT_NS).unwrap().text(), "true");
  assert_eq!(presence.get_child("videomuted", ns::DEFAULT_NS).unwrap().text(), "false");
  assert_eq!(
    serde_json::from_str::<serde_json::Value>(&presence.get_child("SourceInfo", ns::DEFAULT_NS).unwrap().text()).unwrap(),
    serde_json::json!({ "abcd1234-a0": { "muted": true }, "abcd1234-v0": { "muted": false } })
  );
  assert_eq!(presence.get_child("jitsi_participant_codecList", ns::DEFAULT_NS).unwrap().text(), "vp8");
}

#[tokio::test]
async fn a_presence_of_an_unknown_participant_leaving_is_ignored() {
  let harness = Harness::join().await;
  harness.record_events().await;
  harness.events.wait_for("participant bob").await;
  harness
    .server
    .receive(&format!("<presence xmlns='jabber:client' type='unavailable' from='{ROOM}/nobody'/>"))
    .await;
  harness
    .server
    .receive(&format!("<presence xmlns='jabber:client' type='unavailable' from='{ROOM}/focus'/>"))
    .await;
  // Bob changes his name: an update, not a new participant.
  harness
    .server
    .receive(&format!(
      "<presence xmlns='jabber:client' from='{ROOM}/bob'><nick xmlns='http://jabber.org/protocol/nick'>Robert</nick></presence>"
    ))
    .await;
  harness
    .server
    .receive(&format!("<presence xmlns='jabber:client' type='unavailable' from='{ROOM}/bob'/>"))
    .await;
  harness.events.wait_for("participant left bob").await;
  let participants: Vec<String> = harness
    .events
    .all()
    .into_iter()
    .filter(|event| event.starts_with("participant"))
    .collect();
  assert_eq!(participants, ["participant bob", "participant left bob"]);
}

#[tokio::test]
async fn the_nickname_of_a_known_participant_is_updated() {
  let harness = Harness::join().await;
  let nick = || {
    harness
      .conference
      .shared
      .lock()
      .participants
      .get("bob")
      .and_then(|entry| entry.participant.nick.clone())
  };
  assert_eq!(nick().as_deref(), Some("bob nick"));
  harness
    .server
    .receive(&format!(
      "<presence xmlns='jabber:client' from='{ROOM}/bob'><nick xmlns='http://jabber.org/protocol/nick'>Robert</nick></presence>"
    ))
    .await;
  assert_eq!(nick().as_deref(), Some("Robert"));
}

#[tokio::test]
async fn the_reason_we_left_the_room_is_reported() {
  let cases = [
    (
      "<x xmlns='http://jabber.org/protocol/muc#user'><item affiliation='outcast' role='none'><reason>Spam</reason></item><status code='110'/><status code='301'/></x>",
      ConferenceLeftReason::Kicked { reason: Some("Spam".to_owned()) },
    ),
    (
      "<x xmlns='http://jabber.org/protocol/muc#user'><status code='110'/><status code='332'/></x>",
      ConferenceLeftReason::RoomDestroyed { reason: None },
    ),
    (
      "<x xmlns='http://jabber.org/protocol/muc#user'><destroy><reason>Meeting over</reason></destroy></x>",
      ConferenceLeftReason::RoomDestroyed { reason: Some("Meeting over".to_owned()) },
    ),
  ];
  for (x, expected) in cases {
    let harness = Harness::join().await;
    harness.record_events().await;
    harness
      .server
      .receive(&format!("<presence xmlns='jabber:client' type='unavailable' from='{ROOM}/{ME}'>{x}</presence>"))
      .await;
    harness.events.wait_for(&format!("left {expected:?}")).await;
    harness.media.wait_for_log("session 1: shutdown").await;
  }
}

#[tokio::test]
async fn losing_the_connection_ends_the_conference() {
  let harness = Harness::join().await;
  harness.record_events().await;
  harness.server.connection.end().await;
  harness.events.wait_for(&format!("left {:?}", ConferenceLeftReason::ConnectionLost)).await;
  harness.media.wait_for_log("session 1: shutdown").await;
  harness.conference.clone().leave().await.unwrap();
}
