use std::time::Duration;

use xmpp_parsers::ns;

use super::{
  harness::{config, try_join, try_join_failing_to_start, Harness, Server},
  xml::*,
};
use crate::{ConferenceLeftReason, JoinError, MediaType};

fn join_error(result: anyhow::Result<crate::JitsiConference>) -> JoinError {
  result
    .expect_err("the join fails")
    .downcast_ref::<JoinError>()
    .expect("the error is a JoinError")
    .clone()
}

#[tokio::test]
async fn joins_and_answers_the_offer() {
  let harness = Harness::join().await;
  let media = &harness.media;

  // The media layer was given what the focus offered, and external services were asked for.
  let started = media.log().into_iter().find(|entry| entry.contains("start")).unwrap();
  assert_eq!(
    started,
    "session 1: start sid=sid-1 sources=[1000, 2000] ice_servers=1 fingerprint=Sha256 candidates=1"
  );

  let jingle = harness.accept.get_child("jingle", ns::JINGLE).unwrap();
  assert_eq!(jingle.attr("sid"), Some("sid-1"));
  assert_eq!(jingle.attr("initiator"), Some(FOCUS_ACCOUNT));
  assert_eq!(jingle.attr("responder"), Some(MY_JID));
  assert_eq!(harness.accept.attr("to"), Some(FOCUS));

  let contents: Vec<_> = jingle.children().filter(|child| child.name() == "content").collect();
  assert_eq!(contents.len(), 2);
  let xml = String::from(jingle);
  // Our sources, with the RTX of the video one, the codecs we chose and the transport the media layer made.
  for expected in [
    "name='abcd1234-a0'",
    "name='abcd1234-v0'",
    "ssrc='100'",
    "ssrc='200'",
    "ssrc='201'",
    "semantics='FID'",
    "name='opus'",
    "name='VP8'",
    "name='rtx'",
    "ufrag='localufrag'",
    "hash='sha-256'",
    "setup='active'",
    "port='10000'",
    "semantics='BUNDLE'",
    "urn:ietf:params:rtp-hdrext:ssrc-audio-level",
  ] {
    assert!(xml.contains(expected), "{expected} is missing in {xml}");
  }
  assert!(!xml.contains("remb"), "{xml}");

  let conference = &harness.conference;
  assert_eq!(conference.endpoint_id(), ME);
  let sources = conference.remote_sources().await;
  assert_eq!(sources.len(), 2);
  assert_eq!(sources[0].source_name.as_deref(), Some("bob-a0"));
  assert_eq!(sources[0].participant_id.as_deref(), Some("bob"));
  assert_eq!(sources[1].media_type, MediaType::Video);
}

#[tokio::test]
async fn a_participant_that_was_there_before_us_is_announced_to_the_callback() {
  let harness = Harness::join_with(config(), &["bob", "carol"], &Offer::default()).await;
  harness.record_events().await;
  harness.events.wait_for("participant carol").await;
  harness.events.wait_for("participant bob").await;
  let focus_is_no_participant = harness.events.all().iter().all(|event| !event.contains("focus"));
  assert!(focus_is_no_participant);
}

#[tokio::test]
async fn a_refused_join_is_a_join_error_and_cleans_up() {
  let (result, mut server, media) = try_join(config(), |mut server: Server| async move {
    let request = server
      .next_matching("the conference request", |stanza| stanza.get_child("conference", NS_FOCUS).is_some())
      .await;
    server.receive(&conference_ready(request.attr("id").unwrap(), true)).await;
    server.presence().await;
    server
      .receive(&format!(
        "<presence xmlns='jabber:client' type='error' from='{ROOM}/{ME}'>\
           <error type='auth'><not-authorized xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error></presence>"
      ))
      .await;
    server
  })
  .await;

  assert_eq!(join_error(result), JoinError::PasswordRequired);
  media.wait_for_log("media: shutdown").await;
  // We never were in the room: there is nothing to leave.
  server.assert_quiet().await;
}

#[tokio::test]
async fn the_focus_that_wants_authentication_is_not_asked_again() {
  let (result, mut server, _) = try_join(config(), |mut server: Server| async move {
    let request = server
      .next_matching("the conference request", |stanza| stanza.get_child("conference", NS_FOCUS).is_some())
      .await;
    server.answer_error(&request, "not-authorized", "").await;
    server
  })
  .await;
  assert_eq!(join_error(result), JoinError::AuthenticationRequired);
  server.assert_quiet().await;
}

#[tokio::test]
async fn the_focus_that_is_not_ready_is_asked_again() {
  let (result, _server, _) = try_join(config(), |mut server: Server| async move {
    let request = server
      .next_matching("the conference request", |stanza| stanza.get_child("conference", NS_FOCUS).is_some())
      .await;
    server.receive(&conference_ready(request.attr("id").unwrap(), false)).await;
    let asked_again = std::time::Instant::now();
    let request = server
      .next_matching("the second conference request", |stanza| {
        stanza.get_child("conference", NS_FOCUS).is_some()
      })
      .await;
    // The first delay is a second.
    assert!(asked_again.elapsed() >= Duration::from_millis(900), "{:?}", asked_again.elapsed());
    server
      .answer_error(&request, "forbidden", "no token")
      .await;
    server
  })
  .await;
  assert_eq!(join_error(result), JoinError::NotAuthorized(Some("no token".to_owned())));
}

#[tokio::test]
async fn room_creation_restricted_is_retried_from_the_conference_request() {
  let (result, _server, _) = try_join(config(), |mut server: Server| async move {
    let mut attempts = 0;
    loop {
      let request = server
        .next_matching("a conference request", |stanza| stanza.get_child("conference", NS_FOCUS).is_some())
        .await;
      server.receive(&conference_ready(request.attr("id").unwrap(), true)).await;
      server.presence().await;
      attempts += 1;
      if attempts == 2 {
        // The focus has made the room by now.
        server.receive(&self_presence()).await;
        server.receive(&session_initiate("initiate-1", &Offer::default())).await;
        let accept = server.jingle("session-accept").await;
        server.answer(&accept).await;
        return server;
      }
      server
        .receive(&format!(
          "<presence xmlns='jabber:client' type='error' from='{ROOM}/{ME}'>\
             <error type='cancel'><not-allowed xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
               <text xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'>Room creation is restricted</text></error></presence>"
        ))
        .await;
    }
  })
  .await;
  result.expect("the second attempt joins");
}

#[tokio::test]
async fn the_join_times_out_and_leaves_the_room() {
  let config = crate::JitsiConferenceConfig::builder()
    .muc(ROOM.parse().unwrap())
    .focus(FOCUS_ACCOUNT.parse().unwrap())
    .nick("Rec".to_owned())
    .receive_mode(crate::ReceiveMode::Encoded)
    .join_timeout(Duration::from_millis(400))
    .build();
  let (result, mut server, media) = try_join(config, |mut server: Server| async move {
    let request = server
      .next_matching("the conference request", |stanza| stanza.get_child("conference", NS_FOCUS).is_some())
      .await;
    server.receive(&conference_ready(request.attr("id").unwrap(), true)).await;
    server.presence().await;
    server.receive(&self_presence()).await;
    // No session-initiate ever comes.
    server
  })
  .await;
  assert_eq!(join_error(result), JoinError::Timeout);

  let unavailable = server.presence().await;
  assert_eq!(unavailable.attr("type"), Some("unavailable"));
  assert_eq!(unavailable.attr("to"), Some(format!("{ROOM}/{ME}").as_str()));
  media.wait_for_log("media: shutdown").await;
}

#[tokio::test]
async fn a_join_that_times_out_before_the_room_answers_still_leaves_the_room() {
  let config = crate::JitsiConferenceConfig::builder()
    .muc(ROOM.parse().unwrap())
    .focus(FOCUS_ACCOUNT.parse().unwrap())
    .nick("Rec".to_owned())
    .receive_mode(crate::ReceiveMode::Encoded)
    .join_timeout(Duration::from_millis(400))
    .build();
  let (result, mut server, media) = try_join(config, |mut server: Server| async move {
    let request = server
      .next_matching("the conference request", |stanza| stanza.get_child("conference", NS_FOCUS).is_some())
      .await;
    server.receive(&conference_ready(request.attr("id").unwrap(), true)).await;
    server.presence().await;
    // The room has our presence, and its answer is on its way, too late.
    server
  })
  .await;
  assert_eq!(join_error(result), JoinError::Timeout);

  // The occupant that the room made of the joining presence would stay as a ghost.
  let unavailable = server.presence().await;
  assert_eq!(unavailable.attr("type"), Some("unavailable"));
  assert_eq!(unavailable.attr("to"), Some(format!("{ROOM}/{ME}").as_str()));
  media.wait_for_log("media: shutdown").await;
}

#[tokio::test]
async fn a_join_that_fails_before_the_presence_is_sent_has_no_room_to_leave() {
  let (result, mut server, _) = try_join(config(), |mut server: Server| async move {
    let request = server
      .next_matching("the conference request", |stanza| stanza.get_child("conference", NS_FOCUS).is_some())
      .await;
    server.answer_error(&request, "forbidden", "no").await;
    server
  })
  .await;
  assert!(matches!(join_error(result), JoinError::NotAuthorized(_)));
  server.assert_quiet().await;
}

#[tokio::test]
async fn the_connection_ending_fails_the_join() {
  let (result, _server, media) = try_join(config(), |mut server: Server| async move {
    server
      .next_matching("the conference request", |stanza| stanza.get_child("conference", NS_FOCUS).is_some())
      .await;
    server.connection.end().await;
    server
  })
  .await;
  assert_eq!(join_error(result), JoinError::ConnectionClosed);
  media.wait_for_log("media: shutdown").await;
}

#[tokio::test]
async fn an_error_to_the_session_accept_rejects_the_session() {
  let (result, mut server, media) = try_join(config(), |mut server: Server| async move {
    let request = server
      .next_matching("the conference request", |stanza| stanza.get_child("conference", NS_FOCUS).is_some())
      .await;
    server.receive(&conference_ready(request.attr("id").unwrap(), true)).await;
    server.presence().await;
    server.receive(&self_presence()).await;
    server.receive(&session_initiate("initiate-1", &Offer::default())).await;
    let accept = server.jingle("session-accept").await;
    server.answer_error(&accept, "bad-request", "no sources").await;
    server
  })
  .await;
  assert_eq!(
    join_error(result),
    JoinError::SessionRejected("bad-request: no sources".to_owned())
  );
  // Both sides end the session; the room is left.
  let terminate = server.jingle("session-terminate").await;
  assert!(String::from(&terminate).contains("failed-application"));
  media.wait_for_log("session 1: shutdown").await;
}

#[tokio::test]
async fn a_media_session_that_cannot_be_started_fails_the_join() {
  let (result, _server, _) = try_join_failing_to_start(config(), Some("no ICE agent"), |mut server: Server| async move {
    let request = server
      .next_matching("the conference request", |stanza| stanza.get_child("conference", NS_FOCUS).is_some())
      .await;
    server.receive(&conference_ready(request.attr("id").unwrap(), true)).await;
    server.presence().await;
    server.receive(&self_presence()).await;
    server.receive(&session_initiate("initiate-1", &Offer::default())).await;
    let terminate = String::from(&server.jingle("session-terminate").await);
    assert!(terminate.contains("failed-application"), "{terminate}");
    assert!(terminate.contains("no ICE agent"), "{terminate}");
    assert!(!terminate.contains("restart='true'"), "{terminate}");
    server
  })
  .await;
  assert_eq!(
    join_error(result),
    JoinError::SessionFailed("failed to start the media session: no ICE agent".to_owned())
  );
}

#[tokio::test]
async fn leaving_after_a_kick_does_not_leave_the_room_again() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  harness
    .server
    .receive(&format!(
      "<presence xmlns='jabber:client' type='unavailable' from='{ROOM}/{ME}'>\
         <x xmlns='http://jabber.org/protocol/muc#user'><item affiliation='none' role='none'><reason>Out</reason></item>\
         <status code='110'/><status code='307'/></x></presence>"
    ))
    .await;
  harness
    .events
    .wait_for(&format!(
      "left {:?}",
      ConferenceLeftReason::Kicked {
        reason: Some("Out".to_owned())
      }
    ))
    .await;
  harness.media.wait_for_log("session 1: shutdown").await;

  harness.conference.clone().leave().await.unwrap();
  // The session is not terminated towards the focus any more than we are in the room.
  let leftovers = {
    let mut seen = vec![];
    while let Ok(stanza) = tokio::time::timeout(Duration::from_millis(100), harness.server.next()).await {
      seen.push(stanza);
    }
    seen
  };
  assert!(
    leftovers.iter().all(|stanza| !(stanza.is("presence", ns::DEFAULT_NS) && stanza.attr("type") == Some("unavailable"))),
    "{leftovers:?}"
  );
}
