use std::{
  sync::{Arc, Mutex},
  time::Duration,
};

use tokio::{sync::Notify, time::timeout};

use super::{harness::Harness, xml::*};
use crate::{ConferenceLeftReason, StreamEvent, StreamEventKind, StreamInfo};

#[tokio::test]
async fn callbacks_run_in_order_and_may_use_the_conference() {
  let harness = Harness::join().await;
  let events = harness.events.clone();
  let conference = harness.conference.clone();
  harness
    .conference
    .on_participant(move |participant| {
      let (events, conference) = (events.clone(), conference.clone());
      Box::pin(async move {
        let id = participant.muc_jid.resource().to_string();
        events.push(format!("{id} begins"));
        // Everything a callback might want to do; none of it can wait for the callback itself.
        conference.remote_sources().await;
        conference.set_muted(crate::MediaType::Audio, true).await?;
        conference.drain_streams(Duration::from_secs(1)).await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
        events.push(format!("{id} ends"));
        Ok(())
      })
    })
    .await;
  harness.events.wait_for("bob ends").await;

  for who in ["carol", "dave"] {
    harness.server.receive(&occupant_presence(who)).await;
  }
  harness.events.wait_for("dave ends").await;
  assert_eq!(
    harness.events.all(),
    ["bob begins", "bob ends", "carol begins", "carol ends", "dave begins", "dave ends"]
  );
}

#[tokio::test]
async fn a_slow_callback_does_not_hold_up_the_stanzas() {
  let mut harness = Harness::join().await;
  let release = Arc::new(Notify::new());
  harness
    .conference
    .on_participant_left({
      let release = release.clone();
      move |_| {
        let release = release.clone();
        Box::pin(async move {
          release.notified().await;
          Ok(())
        })
      }
    })
    .await;
  harness
    .server
    .receive(&format!("<presence xmlns='jabber:client' type='unavailable' from='{ROOM}/bob'/>"))
    .await;
  // The callback is stuck, and the conference carries on.
  harness.server.receive(&source_update("add-1", "source-add", "sid-1", "carol", 3000, None)).await;
  harness.server.expect_ack("add-1").await;
  assert_eq!(harness.conference.remote_sources().await.len(), 1);
  release.notify_one();
}

#[tokio::test]
async fn the_streams_of_a_participant_wait_for_its_callback() {
  let harness = Harness::join().await;
  let release = Arc::new(Notify::new());
  let events = harness.events.clone();
  harness
    .conference
    .on_participant({
      let release = release.clone();
      move |participant| {
        let (release, events) = (release.clone(), events.clone());
        Box::pin(async move {
          if participant.muc_jid.resource().as_str() == "carol" {
            release.notified().await;
            events.push("carol's bin is built");
          }
          Ok(())
        })
      }
    })
    .await;

  harness.server.receive(&occupant_presence("carol")).await;
  let mut ready = (harness.media.hooks.participant_ready)("carol");
  assert!(timeout(Duration::from_millis(100), &mut ready).await.is_err(), "the callback has not returned");
  release.notify_one();
  timeout(Duration::from_secs(5), ready).await.expect("ready after the callback");
  assert_eq!(harness.events.all(), ["carol's bin is built"]);

  // Bob was there before there was a callback to wait for, and the media layer asking again does not make him new.
  timeout(Duration::from_secs(1), (harness.media.hooks.participant_ready)("bob"))
    .await
    .expect("nothing to wait for");
}

#[tokio::test]
async fn the_media_asking_after_a_participant_never_makes_one() {
  let harness = Harness::join().await;
  harness.record_events().await;
  harness.events.wait_for("participant bob").await;

  // A participant the room has not announced: there is nothing to wait for, and it is not made.
  timeout(Duration::from_secs(5), (harness.media.hooks.participant_ready)("zed"))
    .await
    .unwrap();
  assert!(!harness.conference.shared.lock().participants.contains_key("zed"));

  // Its presence arrives later, and then it is announced, once.
  harness.server.receive(&occupant_presence("zed")).await;
  harness.events.wait_for("participant zed").await;
  harness
    .server
    .receive(&format!("<presence xmlns='jabber:client' type='unavailable' from='{ROOM}/zed'/>"))
    .await;
  harness.events.wait_for("participant left zed").await;
  assert_eq!(harness.events.all().iter().filter(|event| *event == "participant zed").count(), 1);
}

#[tokio::test]
async fn a_participant_that_has_left_is_not_made_again_by_what_was_queued_for_it() {
  let mut harness = Harness::join().await;
  harness.record_events().await;
  harness.events.wait_for("participant bob").await;

  // The media layer is told of sources one command at a time and in its own time: bob has gone by the time it asks
  // after him.
  harness
    .server
    .receive(&format!("<presence xmlns='jabber:client' type='unavailable' from='{ROOM}/bob'/>"))
    .await;
  harness.events.wait_for("participant left bob").await;
  timeout(Duration::from_secs(5), (harness.media.hooks.participant_ready)("bob"))
    .await
    .expect("nothing to wait for");

  // Nothing is announced after the departure, and the room has nobody in it as far as the application is concerned.
  harness.server.receive(&source_update("add-1", "source-add", "sid-1", "bob", 4000, None)).await;
  harness.server.expect_ack("add-1").await;
  timeout(Duration::from_secs(5), (harness.media.hooks.participant_ready)("bob"))
    .await
    .expect("nothing to wait for");
  tokio::time::sleep(Duration::from_millis(50)).await;
  let events = harness.events.all();
  assert_eq!(events.iter().filter(|event| *event == "participant bob").count(), 1, "{events:?}");
  let left = events.iter().position(|event| event == "participant left bob").unwrap();
  assert!(events[left..].iter().all(|event| event != "participant bob"), "{events:?}");
  assert!(harness.conference.shared.lock().participants.is_empty());
}

#[tokio::test]
async fn a_callback_can_leave_the_conference() {
  let harness = Harness::join().await;
  harness.record_events().await;
  let conference = harness.conference.clone();
  let leaving = Arc::new(Notify::new());
  harness
    .conference
    .on_participant({
      let leaving = leaving.clone();
      move |participant| {
        let (conference, leaving) = (conference.clone(), leaving.clone());
        Box::pin(async move {
          if participant.muc_jid.resource().as_str() == "carol" {
            conference.leave().await?;
            leaving.notify_one();
          }
          Ok(())
        })
      }
    })
    .await;
  harness.server.receive(&occupant_presence("carol")).await;
  timeout(Duration::from_secs(10), leaving.notified()).await.expect("leaving from a callback does not wait for itself");
  harness.media.wait_for_log("media: shutdown").await;
  harness.events.wait_for(&format!("left {:?}", ConferenceLeftReason::Left)).await;
}

#[tokio::test]
async fn registering_a_consumer_gives_the_waiting_streams_another_chance() {
  let harness = Harness::join().await;
  let count = || harness.media.count("reevaluate");
  let before = count();

  harness.conference.set_stream_sink_factory(|_| None).await;
  assert_eq!(count(), before + 1);
  harness.conference.on_remote_stream(|_| Box::pin(async { Ok(()) })).await;
  assert_eq!(count(), before + 2);
  harness.conference.add_bin(&gstreamer::Bin::with_name("participant_bob")).await.unwrap();
  assert_eq!(count(), before + 3);
}

#[tokio::test]
async fn a_participant_callback_has_the_streams_looked_at_again_when_it_returns() {
  let harness = Harness::join().await;
  let before = harness.media.count("reevaluate");
  harness.conference.on_participant(|_| Box::pin(async { Ok(()) })).await;
  timeout(Duration::from_secs(5), async {
    while harness.media.count("reevaluate") == before {
      tokio::time::sleep(Duration::from_millis(5)).await;
    }
  })
  .await
  .expect("bob's streams are looked at again");
}

#[tokio::test]
async fn stream_events_and_the_sink_factory_reach_the_application() {
  let harness = Harness::join().await;
  let seen = Arc::new(Mutex::new(vec![]));
  harness
    .conference
    .on_remote_stream({
      let seen = seen.clone();
      move |event| {
        let seen = seen.clone();
        Box::pin(async move {
          seen.lock().unwrap().push((event.stream_id, event.kind));
          Ok(())
        })
      }
    })
    .await;
  harness
    .conference
    .set_stream_sink_factory(|info| (info.ssrc == 1000).then(|| gstreamer::ElementFactory::make("identity").build().unwrap()))
    .await;

  let hooks = &harness.media.hooks;
  assert!((hooks.has_stream_handler)());
  let info = |ssrc| StreamInfo {
    stream_id: 1,
    participant_id: "bob".to_owned(),
    ssrc,
    media_type: crate::MediaType::Audio,
    source_name: None,
  };
  assert!((hooks.sink_factory)(info(1000)).is_some());
  assert!((hooks.sink_factory)(info(2000)).is_none());

  let event = |stream_id, kind| StreamEvent {
    stream_id,
    participant_id: "bob".to_owned(),
    ssrc: 1000,
    media_type: crate::MediaType::Audio,
    source_name: None,
    kind,
    error: None,
  };
  (hooks.stream_event)(event(1, StreamEventKind::Added));
  (hooks.stream_event)(event(2, StreamEventKind::Added));
  (hooks.stream_event)(event(1, StreamEventKind::Removed));
  timeout(Duration::from_secs(5), async {
    while seen.lock().unwrap().len() < 3 {
      tokio::time::sleep(Duration::from_millis(5)).await;
    }
  })
  .await
  .unwrap();
  assert_eq!(
    *seen.lock().unwrap(),
    [
      (1, StreamEventKind::Added),
      (2, StreamEventKind::Added),
      (1, StreamEventKind::Removed)
    ]
  );
}

#[tokio::test]
async fn without_a_stream_handler_there_is_none() {
  let harness = Harness::join().await;
  assert!(!(harness.media.hooks.has_stream_handler)());
  assert!((harness.media.hooks.sink_factory)(StreamInfo {
    stream_id: 1,
    participant_id: "bob".to_owned(),
    ssrc: 1,
    media_type: crate::MediaType::Audio,
    source_name: None,
  })
  .is_none());
}

#[tokio::test]
async fn leaving_releases_a_conference_that_its_callbacks_hold() {
  use crate::{ConferenceLeftReason, MediaSessionEvent, SourceEvent, SyncSample};

  let harness = Harness::join().await;
  let conference = harness.conference.clone();
  let shared = std::sync::Arc::downgrade(&conference.shared);

  // A callback that wants to call back into the conference holds a handle on it.
  macro_rules! holding {
    ($conference:expr, $event:ty) => {{
      let held = $conference.clone();
      move |_: $event| {
        let held = held.clone();
        Box::pin(async move {
          let _ = held.endpoint_id();
          Ok(())
        }) as crate::BoxedResultFuture
      }
    }};
  }
  conference.on_participant(holding!(conference, crate::Participant)).await;
  conference.on_participant_left(holding!(conference, crate::Participant)).await;
  conference.on_colibri_message(holding!(conference, colibri::ColibriMessage)).await;
  conference.on_remote_stream(holding!(conference, StreamEvent)).await;
  conference.on_conference_left(holding!(conference, ConferenceLeftReason)).await;
  conference.on_media_session(holding!(conference, MediaSessionEvent)).await;
  conference.on_source(holding!(conference, SourceEvent)).await;
  conference
    .set_stream_sink_factory({
      let held = conference.clone();
      move |_| {
        let _ = held.endpoint_id();
        None
      }
    })
    .await;
  conference
    .on_sync_sample({
      let held = conference.clone();
      move |_: SyncSample| {
        let _ = held.endpoint_id();
      }
    })
    .await;
  drop(conference);

  harness.conference.clone().leave().await.unwrap();
  // The application's handles are the only ones left, and then none.
  let Harness { conference, server, media, .. } = harness;
  drop(conference);
  timeout(Duration::from_secs(5), async {
    while shared.upgrade().is_some() {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("the conference is dropped when the application has dropped its handles");
  drop((server, media));
}

#[tokio::test]
async fn the_callbacks_of_a_conference_that_was_left_are_not_kept() {
  let harness = Harness::join().await;
  harness.conference.clone().leave().await.unwrap();

  let marker = Arc::new(());
  let conference = &harness.conference;
  conference
    .on_source({
      let marker = marker.clone();
      move |_| {
        let _ = &marker;
        Box::pin(async { Ok(()) })
      }
    })
    .await;
  conference
    .on_participant({
      let marker = marker.clone();
      move |_| {
        let _ = &marker;
        Box::pin(async { Ok(()) })
      }
    })
    .await;
  conference
    .on_sync_sample({
      let marker = marker.clone();
      move |_| {
        let _ = &marker;
      }
    })
    .await;
  conference
    .set_stream_sink_factory({
      let marker = marker.clone();
      move |_| {
        let _ = &marker;
        None
      }
    })
    .await;
  assert_eq!(Arc::strong_count(&marker), 1);
}
