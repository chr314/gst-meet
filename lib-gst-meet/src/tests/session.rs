use std::time::Duration;

use xmpp_parsers::ns;

use super::{harness::Harness, xml::*};
use crate::{
  pipeline::{ConnectionState, SessionFailure},
  ConferenceLeftReason,
};

fn offer_with<'a>(sid: &'a str, video: &'a [Participant<'a>]) -> Offer<'a> {
  Offer {
    sid,
    video,
    ..Offer::default()
  }
}

fn text(element: &minidom::Element) -> String {
  String::from(element)
}

#[tokio::test]
async fn a_session_terminate_waits_for_the_reinvite() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  harness.events.wait_for("participant bob").await;

  // Jicofo moves us: it ends the session and offers a new one.
  harness
    .server
    .receive(&jingle("terminate-1", FOCUS, "session-terminate", "sid-1", "<reason><success/><text>moving</text></reason>"))
    .await;
  harness.server.expect_ack("terminate-1").await;
  harness.media.wait_for_log("session 1: shutdown").await;
  harness.events.wait_for("session ended restarting=true").await;

  let accept = harness
    .server
    .offer_session("initiate-2", &offer_with("sid-2", &[("bob", 2000), ("carol", 3000)]))
    .await;
  assert_eq!(accept.get_child("jingle", ns::JINGLE).unwrap().attr("sid"), Some("sid-2"));
  harness.media.wait_for_log("session 2: start sid=sid-2 sources=[1000, 2000, 3000] ice_servers=1 fingerprint=Sha256 candidates=1").await;

  // The consumer sees the difference between the two sessions' sources, not a conference that starts over.
  harness.events.wait_for("source added 3000").await;
  harness.events.wait_for("session started bridge-1").await;
  let events = harness.events.all();
  assert_eq!(events.iter().filter(|event| *event == "source added 2000").count(), 0);
  assert!(!events.iter().any(|event| event.starts_with("left")), "{events:?}");
}

#[tokio::test]
async fn no_reinvite_in_time_gives_up_the_conference() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  harness
    .server
    .receive(&jingle("terminate-1", FOCUS, "session-terminate", "sid-1", "<reason><success/></reason>"))
    .await;
  harness.server.expect_ack("terminate-1").await;
  harness.events.wait_for(&format!("left {:?}", ConferenceLeftReason::NoMediaSession)).await;
  harness.conference.clone().leave().await.unwrap();
}

#[tokio::test]
async fn a_reinvite_in_time_keeps_the_conference() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  harness
    .server
    .receive(&jingle("terminate-1", FOCUS, "session-terminate", "sid-1", "<reason><success/></reason>"))
    .await;
  harness.server.expect_ack("terminate-1").await;
  harness.server.offer_session("initiate-2", &offer_with("sid-2", &[("bob", 2000)])).await;
  // Past the time the first session's absence would have been given up on.
  tokio::time::sleep(Duration::from_millis(1300)).await;
  assert!(!harness.events.all().iter().any(|event| event.starts_with("left")));
}

#[tokio::test]
async fn failed_ice_ends_the_session_and_asks_for_a_new_one() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  (harness.media.hooks.session_failed)(1, SessionFailure::Ice);

  let xml = text(&harness.server.jingle("session-terminate").await);
  assert!(xml.contains("<connectivity-error"), "{xml}");
  assert!(xml.contains("ICE failed"), "{xml}");
  assert!(xml.contains("<bridge-session xmlns='http://jitsi.org/protocol/focus' id='bridge-1' restart='true'/>"), "{xml}");
  assert!(!xml.contains("ice-state"), "{xml}");
  harness.media.wait_for_log("session 1: shutdown").await;
  harness.events.wait_for("session ended restarting=true").await;

  harness.server.offer_session("initiate-2", &offer_with("sid-2", &[("bob", 2000)])).await;
  harness.media.wait_for_log("session 2: start sid=sid-2 sources=[1000, 2000] ice_servers=1 fingerprint=Sha256 candidates=1").await;
}

#[tokio::test]
async fn the_transport_accept_has_the_transport_and_no_sources() {
  let mut harness = Harness::join().await;
  harness
    .server
    .receive(&transport_replace("replace-1", &offer_with("sid-1", &[("bob", 2000)])))
    .await;
  harness.server.expect_ack("replace-1").await;
  let accept = harness.server.jingle("transport-accept").await;
  let jingle = accept.get_child("jingle", ns::JINGLE).unwrap();
  let xml = text(jingle);

  // The focus knows our sources from the session-accept: it takes the ones in a transport-accept as new, finds our
  // SSRCs used already and answers bad-request.
  for forbidden in ["<source", "ssrc", "<description", "payload-type", "name='abcd1234"] {
    assert!(!xml.contains(forbidden), "{forbidden} in {xml}");
  }
  let contents: Vec<_> = jingle.children().filter(|child| child.name() == "content").collect();
  assert_eq!(contents.len(), 2);
  for content in contents {
    let transport = content.get_child("transport", "urn:xmpp:jingle:transports:ice-udp:1").expect("a transport");
    assert_eq!(transport.attr("ufrag"), Some("localufrag"));
    assert!(transport.has_child("fingerprint", "urn:xmpp:jingle:apps:dtls:0"));
    assert_eq!(transport.children().filter(|child| child.name() == "candidate").count(), 1);
  }
}

#[tokio::test]
async fn a_transport_replace_replaces_the_media_session() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  harness
    .server
    .receive(&transport_replace("replace-1", &offer_with("sid-1", &[("bob", 2000), ("carol", 3000)])))
    .await;
  harness.server.expect_ack("replace-1").await;
  let accept = harness.server.jingle("transport-accept").await;
  harness.server.answer(&accept).await;

  let log = harness.media.log();
  let shutdown = log.iter().position(|entry| entry == "session 1: shutdown").unwrap();
  let start = log.iter().position(|entry| entry.starts_with("session 2: start")).unwrap();
  assert!(shutdown < start, "the old session goes first: {log:?}");
  harness.events.wait_for("source added 3000").await;
  harness.events.wait_for("session started bridge-1").await;
}

#[tokio::test]
async fn a_transport_replace_that_fails_is_rejected_and_a_restart_asked_for() {
  let mut harness = Harness::join().await;
  harness.media.fail_next_start("no candidates");
  harness
    .server
    .receive(&transport_replace("replace-1", &offer_with("sid-1", &[("bob", 2000)])))
    .await;
  harness.server.expect_ack("replace-1").await;
  let reject = harness.server.jingle("transport-reject").await;
  assert_eq!(reject.get_child("jingle", ns::JINGLE).unwrap().attr("sid"), Some("sid-1"));
  let terminate = text(&harness.server.jingle("session-terminate").await);
  assert!(terminate.contains("restart='true'"), "{terminate}");
  assert!(terminate.contains("no candidates"), "{terminate}");
}

#[tokio::test]
async fn a_transport_replace_for_another_session_is_rejected() {
  let mut harness = Harness::join().await;
  // The session is known to the focus under its own sid; this one is not.
  harness
    .server
    .receive(&transport_replace("replace-1", &offer_with("sid-other", &[("bob", 2000)])))
    .await;
  let reply = harness.server.result_for("replace-1").await;
  assert_eq!(reply.attr("type"), Some("error"));
  assert_eq!(harness.media.count("session 1: shutdown"), 0);
}

#[tokio::test]
async fn a_dtls_failure_ends_the_session_and_asks_for_a_new_one() {
  let mut harness = Harness::join().await;
  (harness.media.hooks.session_failed)(1, SessionFailure::Dtls("handshake failed".to_owned()));
  let terminate = harness.server.jingle("session-terminate").await;
  let xml = text(&terminate);
  assert!(xml.contains("<connectivity-error"), "{xml}");
  assert!(xml.contains("handshake failed"), "{xml}");
  assert!(xml.contains("<bridge-session xmlns='http://jitsi.org/protocol/focus' id='bridge-1' restart='true'/>"), "{xml}");
  harness.media.wait_for_log("session 1: shutdown").await;

  // A failure of an older session is nobody's business any more.
  (harness.media.hooks.session_failed)(1, SessionFailure::Dtls("again".to_owned()));
  harness.server.assert_quiet().await;

  harness.server.offer_session("initiate-2", &offer_with("sid-2", &[("bob", 2000)])).await;
  harness.media.wait_for_log("session 2: start sid=sid-2 sources=[1000, 2000] ice_servers=1 fingerprint=Sha256 candidates=1").await;
}

#[tokio::test]
async fn a_fingerprint_mismatch_is_fatal() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  (harness.media.hooks.session_failed)(1, SessionFailure::Security("fingerprint mismatch".to_owned()));
  let xml = text(&harness.server.jingle("session-terminate").await);
  assert!(xml.contains("<security-error"), "{xml}");
  assert!(xml.contains("restart='false'"), "{xml}");
  harness
    .events
    .wait_for(&format!("left {:?}", ConferenceLeftReason::Fatal("fingerprint mismatch".to_owned())))
    .await;
  harness.media.wait_for_log("session 1: shutdown").await;
}

#[tokio::test]
async fn too_many_restarts_are_fatal() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  for round in 1..=5u64 {
    (harness.media.hooks.session_failed)(round, SessionFailure::Dtls(format!("failure {round}")));
    let xml = text(&harness.server.jingle("session-terminate").await);
    assert!(xml.contains("restart='true'"), "{xml}");
    harness
      .server
      .offer_session(&format!("initiate-{}", round + 1), &offer_with(&format!("sid-{}", round + 1), &[("bob", 2000)]))
      .await;
    harness.media.wait_for_log(&format!("session {}: start sid=sid-{} sources=[1000, 2000] ice_servers=1 fingerprint=Sha256 candidates=1", round + 1, round + 1)).await;
  }
  (harness.media.hooks.session_failed)(6, SessionFailure::Dtls("failure 6".to_owned()));
  let events = harness.events.clone();
  tokio::time::timeout(Duration::from_secs(5), async move {
    loop {
      if events.all().iter().any(|event| event.starts_with("left Fatal(") && event.contains("restarts")) {
        break;
      }
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("the sixth restart is given up");
  harness.server.assert_quiet().await;
}

#[tokio::test]
async fn a_session_that_cannot_be_started_again_asks_for_a_restart() {
  let mut harness = Harness::join().await;
  harness.media.fail_next_start("out of ports");
  harness.server.receive(&session_initiate("initiate-2", &offer_with("sid-2", &[("bob", 2000)]))).await;
  harness.server.expect_ack("initiate-2").await;
  let xml = text(&harness.server.jingle("session-terminate").await);
  assert!(xml.contains("failed-application"), "{xml}");
  assert!(xml.contains("restart='true'"), "{xml}");
  assert!(xml.contains("out of ports"), "{xml}");
}

#[tokio::test]
async fn sources_come_and_go_with_the_focus_and_the_participants() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  harness.events.wait_for("participant bob").await;

  harness.server.receive(&source_update("add-1", "source-add", "sid-1", "carol", 3000, Some(3100))).await;
  harness.server.expect_ack("add-1").await;
  harness.media.wait_for_log("session 1: sources added [3000, 3100]").await;
  harness.events.wait_for("source added 3100").await;
  let names: Vec<_> = harness
    .conference
    .remote_sources()
    .await
    .into_iter()
    .filter_map(|source| source.source_name)
    .collect();
  assert_eq!(names, ["bob-a0", "bob-v0", "carol-a0", "carol-v0"]);

  harness.server.receive(&source_update("remove-1", "source-remove", "sid-1", "carol", 3000, Some(3100))).await;
  harness.server.expect_ack("remove-1").await;
  harness.media.wait_for_log("session 1: sources removed [3000, 3100]").await;
  harness.events.wait_for("source removed 3100").await;

  // A participant that leaves takes its sources along, and the media layer is told.
  harness
    .server
    .receive(&format!("<presence xmlns='jabber:client' type='unavailable' from='{ROOM}/bob'/>"))
    .await;
  harness.events.wait_for("participant left bob").await;
  harness.events.wait_for("source removed 1000").await;
  harness.media.wait_for_log("session 1: participant left bob").await;
  assert!(harness.conference.remote_sources().await.is_empty());
}

#[tokio::test]
async fn candidates_that_come_late_are_trickled_after_the_accept() {
  let mut harness = Harness::join().await;
  harness.media.late_candidate();
  let info = harness.server.jingle("transport-info").await;
  let xml = text(info.get_child("jingle", ns::JINGLE).unwrap());
  assert!(xml.contains("port='10001'"), "{xml}");
  assert!(xml.contains("ufrag='localufrag'"), "{xml}");
  assert_eq!(xml.matches("<content").count(), 1, "one content is enough for a bundle: {xml}");
}

#[tokio::test]
async fn candidates_of_the_focus_go_to_the_media_session() {
  let mut harness = Harness::join().await;
  harness
    .server
    .receive(&jingle(
      "info-1",
      FOCUS,
      "transport-info",
      "sid-1",
      "<content creator='initiator' name='audio'><transport xmlns='urn:xmpp:jingle:transports:ice-udp:1' ufrag='f' pwd='p'>\
         <candidate xmlns='urn:xmpp:jingle:transports:ice-udp:1' component='1' foundation='2' generation='0' id='c2' ip='10.0.0.2' port='10001' priority='1' protocol='udp' type='host'/>\
       </transport></content>",
    ))
    .await;
  harness.server.expect_ack("info-1").await;
  harness.media.wait_for_log("session 1: remote candidates 1").await;
}

#[tokio::test]
async fn the_media_session_reports_its_connection() {
  let harness = Harness::join().await;
  harness.record_events().await;
  (harness.media.hooks.connection_state)(1, ConnectionState::Connected);
  harness.events.wait_for("session Connected").await;
  // The state of a session that has been replaced is not the state of the conference.
  (harness.media.hooks.connection_state)(7, ConnectionState::Failed);
  (harness.media.hooks.connection_state)(1, ConnectionState::Checking);
  harness.events.wait_for("session Checking").await;
  assert!(!harness.events.all().contains(&"session Failed".to_owned()));
}

#[tokio::test]
async fn a_fatal_media_error_ends_the_conference() {
  let harness = Harness::join().await;
  harness.record_events().await;
  (harness.media.hooks.fatal)("the pipeline broke".to_owned());
  harness
    .events
    .wait_for(&format!("left {:?}", ConferenceLeftReason::Fatal("the pipeline broke".to_owned())))
    .await;
  harness.media.wait_for_log("session 1: shutdown").await;
}

#[tokio::test]
async fn the_streams_can_be_drained() {
  let harness = Harness::join().await;
  harness.conference.drain_streams(Duration::from_secs(1)).await.unwrap();
  harness.media.wait_for_log("session 1: drain 1s").await;
}

#[tokio::test]
async fn leaving_ends_the_session_and_the_room_and_stops_everything() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  harness.conference.clone().leave().await.unwrap();

  let terminate = harness.server.jingle("session-terminate").await;
  let xml = text(&terminate);
  assert!(xml.contains("<success"), "{xml}");
  assert!(xml.contains("<bridge-session xmlns='http://jitsi.org/protocol/focus' id='bridge-1' restart='false'/>"), "{xml}");
  let unavailable = harness.server.presence().await;
  assert_eq!(unavailable.attr("type"), Some("unavailable"));
  assert_eq!(unavailable.attr("to"), Some(format!("{ROOM}/{ME}").as_str()));

  let log = harness.media.log();
  let session = log.iter().position(|entry| entry == "session 1: shutdown").unwrap();
  let media = log.iter().position(|entry| entry == "media: shutdown").unwrap();
  assert!(session < media);
  assert!(harness.events.all().contains(&"session ended restarting=false".to_owned()));
  assert!(harness.events.all().contains(&format!("left {:?}", ConferenceLeftReason::Left)));

  // The stanza filter is gone: the conference does not handle what comes from the room any more.
  harness
    .server
    .receive(&jingle("late-1", FOCUS, "source-add", "sid-1", ""))
    .await;
  let reply = harness.server.result_for("late-1").await;
  assert_eq!(reply.attr("type"), Some("error"));
  assert!(reply.get_child("error", ns::DEFAULT_NS).unwrap().has_child("service-unavailable", ns::XMPP_STANZAS));
  harness.conference.clone().leave().await.unwrap();
  assert_eq!(harness.media.count("media: shutdown"), 1);
}

#[test]
fn the_defaults_of_the_configuration_are_those_of_the_specification() {
  let config = crate::JitsiConferenceConfig::builder()
    .muc(ROOM.parse().unwrap())
    .focus(FOCUS_ACCOUNT.parse().unwrap())
    .nick("Rec".to_owned())
    .build();
  assert_eq!(config.join_timeout, Duration::from_secs(60));
  assert_eq!(config.ice_gathering_timeout, Duration::from_secs(5));
  assert_eq!(config.stream_drain_timeout, Duration::from_secs(3));
  assert_eq!(config.media_session_timeout, Duration::from_secs(60));
  assert!(config.glib_main_context.is_none());
}

#[tokio::test]
async fn a_late_failure_does_not_end_the_session_the_focus_has_offered_since() {
  let mut harness = Harness::join().await;
  // The failure of session 1 is queued for the Jingle actor, and so are the end of that session and the offer of
  // the next, which the stanza filter has seen by the time the actor gets to the failure.
  (harness.media.hooks.session_failed)(1, SessionFailure::Dtls("late".to_owned()));
  harness
    .server
    .receive(&jingle("terminate-1", FOCUS, "session-terminate", "sid-1", "<reason><success/></reason>"))
    .await;
  harness
    .server
    .receive(&session_initiate("initiate-2", &offer_with("sid-2", &[("bob", 2000)])))
    .await;
  harness.server.expect_ack("terminate-1").await;
  harness.server.expect_ack("initiate-2").await;
  let accept = harness.server.jingle("session-accept").await;
  harness.server.answer(&accept).await;

  // Session 2 is the focus' session: it can add sources.
  harness.server.receive(&source_update("add-2", "source-add", "sid-2", "carol", 3000, None)).await;
  harness.server.expect_ack("add-2").await;
  harness.media.wait_for_log("session 2: sources added [3000]").await;
  // And nobody asked the focus to end the session that the focus had ended already.
  harness.server.assert_quiet().await;
}

#[tokio::test]
async fn a_session_that_cannot_start_does_not_end_the_session_the_focus_has_offered_since() {
  let mut harness = Harness::join().await;
  harness.media.fail_next_start("out of ports");
  harness
    .server
    .receive(&session_initiate("initiate-2", &offer_with("sid-2", &[("bob", 2000)])))
    .await;
  harness
    .server
    .receive(&session_initiate("initiate-3", &offer_with("sid-3", &[("bob", 2000)])))
    .await;
  harness.server.expect_ack("initiate-2").await;
  harness.server.expect_ack("initiate-3").await;
  let accept = harness.server.jingle("session-accept").await;
  assert_eq!(accept.get_child("jingle", ns::JINGLE).unwrap().attr("sid"), Some("sid-3"));
  harness.server.answer(&accept).await;

  harness.server.receive(&source_update("add-3", "source-add", "sid-3", "carol", 3000, None)).await;
  harness.server.expect_ack("add-3").await;
  harness.media.wait_for_log("session 2: sources added [3000]").await;
  harness.server.assert_quiet().await;
}

#[tokio::test]
async fn sessions_are_not_drained_unless_the_application_asks() {
  let mut harness = Harness::join().await;
  harness
    .server
    .receive(&jingle("terminate-1", FOCUS, "session-terminate", "sid-1", "<reason><success/></reason>"))
    .await;
  harness.server.expect_ack("terminate-1").await;
  harness.server.offer_session("initiate-2", &offer_with("sid-2", &[("bob", 2000)])).await;
  harness
    .media
    .wait_for_log("session 2: start sid=sid-2 sources=[1000, 2000] ice_servers=1 fingerprint=Sha256 candidates=1")
    .await;
  assert!(harness.media.log().iter().all(|entry| !entry.contains("drain")), "{:?}", harness.media.log());
}

#[tokio::test]
async fn a_session_that_starts_after_the_drain_is_drained_before_anything_can_come_in() {
  let mut harness = Harness::join().await;
  harness.conference.drain_streams(Duration::from_secs(1)).await.unwrap();
  harness.media.wait_for_log("session 1: drain 1s").await;

  // The focus moves us to another session ...
  harness
    .server
    .receive(&jingle("terminate-1", FOCUS, "session-terminate", "sid-1", "<reason><success/></reason>"))
    .await;
  harness.server.expect_ack("terminate-1").await;
  harness.server.offer_session("initiate-2", &offer_with("sid-2", &[("bob", 2000)])).await;
  // ... and the next one is a transport-replace.
  harness
    .server
    .receive(&transport_replace("replace-1", &offer_with("sid-2", &[("bob", 2000)])))
    .await;
  harness.server.expect_ack("replace-1").await;
  let accept = harness.server.jingle("transport-accept").await;
  harness.server.answer(&accept).await;

  let log = harness.media.log();
  for session in [2, 3] {
    let start = log.iter().position(|entry| entry.starts_with(&format!("session {session}: start"))).unwrap();
    // Right after the start: no stream was activated, no sink factory called.
    assert_eq!(log[start + 1], format!("session {session}: drain 0ns"), "{log:?}");
  }
}

/// A session of the bridge named `bridge`, offered under the sid of the first one.
fn replace_offer<'a>(bridge: &'a str) -> Offer<'a> {
  Offer {
    bridge_session_id: Some(bridge),
    ..offer_with("sid-1", &[("bob", 2000)])
  }
}

async fn replace_transport(harness: &mut Harness, bridge: &str) {
  harness.server.receive(&transport_replace("replace-1", &replace_offer(bridge))).await;
  harness.server.expect_ack("replace-1").await;
  let accept = harness.server.jingle("transport-accept").await;
  harness.server.answer(&accept).await;
  harness
    .media
    .wait_for_log("session 2: start sid=sid-1 sources=[1000, 2000] ice_servers=1 fingerprint=Sha256 candidates=1")
    .await;
}

#[tokio::test]
async fn a_restart_after_a_transport_replace_names_the_new_bridge_session() {
  let mut harness = Harness::join().await;
  replace_transport(&mut harness, "bridge-2").await;

  (harness.media.hooks.session_failed)(2, SessionFailure::Dtls("handshake failed".to_owned()));
  let xml = text(&harness.server.jingle("session-terminate").await);
  assert!(xml.contains("<bridge-session xmlns='http://jitsi.org/protocol/focus' id='bridge-2' restart='true'/>"), "{xml}");
}

#[tokio::test]
async fn leaving_after_a_transport_replace_names_the_new_bridge_session() {
  let mut harness = Harness::join().await;
  replace_transport(&mut harness, "bridge-2").await;
  harness.conference.clone().leave().await.unwrap();
  let xml = text(&harness.server.jingle("session-terminate").await);
  assert!(xml.contains("<bridge-session xmlns='http://jitsi.org/protocol/focus' id='bridge-2' restart='false'/>"), "{xml}");
}

#[tokio::test]
async fn a_transport_replace_that_fails_to_parse_still_moves_the_session_to_the_new_bridge_session() {
  let mut harness = Harness::join().await;
  // No fingerprint in the new transport: the replace cannot be taken. The focus has allocated on the new bridge
  // all the same, and that is the session whose restart it will take.
  let broken = transport_replace("replace-1", &replace_offer("bridge-2")).replace("hash='sha-256'", "hash='md5'");
  harness.server.receive(&broken).await;
  harness.server.expect_ack("replace-1").await;
  harness.server.jingle("transport-reject").await;
  let xml = text(&harness.server.jingle("session-terminate").await);
  assert!(xml.contains("id='bridge-2' restart='true'"), "{xml}");
}

async fn wait_for_fatal(harness: &Harness, part: &str) -> String {
  tokio::time::timeout(Duration::from_secs(5), async {
    loop {
      if let Some(event) = harness
        .events
        .all()
        .into_iter()
        .find(|event| event.starts_with("left Fatal(") && event.contains(part))
      {
        return event;
      }
      tokio::time::sleep(Duration::from_millis(5)).await;
    }
  })
  .await
  .unwrap_or_else(|_| panic!("no fatal end containing {part:?} in {:?}", harness.events.all()))
}

#[tokio::test]
async fn a_restart_that_the_focus_rate_limits_ends_the_conference_with_the_reason() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  (harness.media.hooks.session_failed)(1, SessionFailure::Dtls("handshake failed".to_owned()));
  let terminate = harness.server.jingle("session-terminate").await;
  // Jicofo ends the session all the same, and does not offer another: there is nothing to ask again about.
  harness.server.answer_error(&terminate, "resource-constraint", "rate-limited").await;
  let event = wait_for_fatal(&harness, "rate-limited").await;
  assert!(event.contains("new media session"), "{event}");
  harness.server.assert_quiet().await;
}

#[tokio::test]
async fn a_restart_that_names_an_unknown_session_leaves_the_new_session_to_come() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  (harness.media.hooks.session_failed)(1, SessionFailure::Dtls("handshake failed".to_owned()));
  let terminate = harness.server.jingle("session-terminate").await;
  // The focus has moved on without our knowing, or thinks of another bridge session: it may still invite us.
  harness
    .server
    .answer_error(&terminate, "item-not-found", "jingle session no longer active")
    .await;
  tokio::time::sleep(Duration::from_millis(100)).await;
  assert!(!harness.events.all().iter().any(|event| event.starts_with("left")), "{:?}", harness.events.all());
  harness.server.offer_session("initiate-2", &offer_with("sid-2", &[("bob", 2000)])).await;
  harness.media.wait_for_log("session 2: start sid=sid-2 sources=[1000, 2000] ice_servers=1 fingerprint=Sha256 candidates=1").await;
  tokio::time::sleep(Duration::from_millis(1200)).await;
  assert!(!harness.events.all().iter().any(|event| event.starts_with("left")), "{:?}", harness.events.all());
}

#[tokio::test]
async fn a_restart_that_the_focus_does_not_take_and_does_not_follow_up_ends_the_conference() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  (harness.media.hooks.session_failed)(1, SessionFailure::Dtls("handshake failed".to_owned()));
  let terminate = harness.server.jingle("session-terminate").await;
  harness.server.answer_error(&terminate, "item-not-found", "no such bridge session").await;
  harness.events.wait_for(&format!("left {:?}", ConferenceLeftReason::NoMediaSession)).await;
}

#[tokio::test]
async fn a_refusal_that_comes_after_the_new_session_does_not_end_the_conference() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  (harness.media.hooks.session_failed)(1, SessionFailure::Dtls("handshake failed".to_owned()));
  let terminate = harness.server.jingle("session-terminate").await;
  harness.server.offer_session("initiate-2", &offer_with("sid-2", &[("bob", 2000)])).await;
  harness.events.wait_for("session started bridge-1").await;
  // The answer to the old session's restart is late.
  harness.server.answer_error(&terminate, "resource-constraint", "rate-limited").await;
  tokio::time::sleep(Duration::from_millis(100)).await;
  assert!(!harness.events.all().iter().any(|event| event.starts_with("left")), "{:?}", harness.events.all());
}

