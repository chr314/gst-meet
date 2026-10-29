use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
  },
  time::Duration,
};

use gstreamer::prelude::*;

use super::support::*;
use crate::{
  conference::ReceiveMode,
  pipeline::{RtpSession, StreamEventKind},
  source::MediaType,
};

const DATA: Duration = Duration::from_secs(10);
const DRAIN: Duration = Duration::from_secs(3);

fn session_has_pad(session: &RtpSession, prefix: &str) -> bool {
  session.bin().pads().iter().any(|pad| pad.name().starts_with(prefix))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_announced_before_the_first_packet_activates_one_stream() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);

  let added = rig.expect(StreamEventKind::Added).await;
  assert_eq!((added.ssrc, added.participant_id.as_str()), (1000, "alice"));
  assert_eq!(added.source_name.as_deref(), Some("alice-a0"));
  let calls = rig.factory.calls.lock().unwrap().clone();
  assert_eq!(calls.len(), 1);
  assert_eq!(calls[0].stream_id, added.stream_id);
  assert_eq!(calls[0].source_name.as_deref(), Some("alice-a0"));
  assert_eq!(*rig.observed.participants.lock().unwrap(), ["alice"]);

  let record = rig.factory.record(added.stream_id);
  wait_until("data at the consumer", DATA, || record.buffers() > 5).await;
  rig.expect_no_event(Duration::from_millis(500)).await;
  assert_eq!(rig.factory.calls.lock().unwrap().len(), 1);

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
  let removed = rig.expect(StreamEventKind::Removed).await;
  assert_eq!(removed.stream_id, added.stream_id);
  assert_eq!(removed.error, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_announced_after_the_first_packet_activates_one_stream() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let session = rig.start(vec![]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  wait_until("rtpbin's pad", DATA, || session_has_pad(&session, "stream_1000_111")).await;
  assert!(rig.factory.calls.lock().unwrap().is_empty());

  session
    .sources_added(vec![source(1000, "alice", MediaType::Audio)])
    .await;
  // The same announcement twice does not make a second stream.
  session
    .sources_added(vec![source(1000, "alice", MediaType::Audio)])
    .await;

  let added = rig.expect(StreamEventKind::Added).await;
  assert_eq!(added.source_name.as_deref(), Some("alice-a0"));
  let record = rig.factory.record(added.stream_id);
  wait_until("data at the consumer", DATA, || record.buffers() > 5).await;
  rig.expect_no_event(Duration::from_millis(500)).await;
  assert_eq!(rig.factory.calls.lock().unwrap().len(), 1);

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_camera_switched_off_and_on_makes_a_new_stream() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let video = source(2000, "bob", MediaType::Video);
  let session = rig.start(vec![video.clone()]).await;
  let sender = Sender::vp8(&rig, 2000, 96);

  let first = rig.expect(StreamEventKind::Added).await;
  let first_record = rig.factory.record(first.stream_id);
  wait_until("data at the first consumer", DATA, || first_record.buffers() > 3).await;
  let first_consumer = rig.factory.element(first.stream_id);

  // The packets keep coming while the source is gone.
  session.sources_removed(vec![2000]).await;
  let removed = rig.expect(StreamEventKind::Removed).await;
  assert_eq!(removed.stream_id, first.stream_id);
  assert!(
    first_record.has_eos(),
    "the consumer should have seen the end of its stream"
  );

  session.sources_added(vec![video]).await;
  let second = rig.expect(StreamEventKind::Added).await;
  assert_ne!(second.stream_id, first.stream_id);
  assert_eq!(second.ssrc, 2000);
  let second_record = rig.factory.record(second.stream_id);
  wait_until("data at the second consumer", DATA, || second_record.buffers() > 3).await;
  assert_ne!(rig.factory.element(second.stream_id), first_consumer);
  assert!(!second_record.has_eos());
  assert_eq!(rig.factory.calls.lock().unwrap().len(), 2);

  rig.assert_pipeline_running();
  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pads_stay_linked_when_participants_come_and_go() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let session = rig
    .start(vec![
      source(1000, "alice", MediaType::Audio),
      source(2000, "bob", MediaType::Audio),
    ])
    .await;
  let alice = Sender::opus(&rig, 1000, 111);
  let bob = Sender::opus(&rig, 2000, 111);
  let first = rig.expect(StreamEventKind::Added).await;
  let second = rig.expect(StreamEventKind::Added).await;
  let bob_stream = if first.participant_id == "bob" { first } else { second };

  // Alice leaves, but her client keeps sending for a while.
  session.participant_left("alice").await;
  let removed = rig.expect(StreamEventKind::Removed).await;
  assert_eq!(removed.participant_id, "alice");
  tokio::time::sleep(Duration::from_millis(500)).await;
  rig.assert_pipeline_running();

  let carol = Sender::opus(&rig, 3000, 111);
  tokio::time::sleep(Duration::from_millis(300)).await;
  session
    .sources_added(vec![source(3000, "carol", MediaType::Audio)])
    .await;
  let carol_added = rig.expect(StreamEventKind::Added).await;
  assert_eq!(carol_added.participant_id, "carol");
  let carol_record = rig.factory.record(carol_added.stream_id);
  wait_until("data for carol", DATA, || carol_record.buffers() > 5).await;

  let bob_record = rig.factory.record(bob_stream.stream_id);
  let before = bob_record.buffers();
  wait_until("bob's stream to go on", DATA, || bob_record.buffers() > before + 5).await;

  rig.assert_pipeline_running();
  assert_eq!(parking_fakesinks(&rig.media.pipeline()), Vec::<String>::new());
  assert_eq!(pads_with_failed_flow(&rig.media.pipeline()), Vec::<String>::new());
  assert!(rig.observed.failures.lock().unwrap().is_empty());

  alice.stop();
  bob.stop();
  carol.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_payload_type_switch_keeps_the_stream_and_its_consumer() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);

  let added = rig.expect(StreamEventKind::Added).await;
  let record = rig.factory.record(added.stream_id);
  let consumer = rig.factory.element(added.stream_id);
  wait_until("data at the consumer", DATA, || record.buffers() > 5).await;
  assert_eq!(record.count("stream-start"), 1);

  sender.set_pt(112);
  wait_until("the new chain's stream start", DATA, || {
    record.count("stream-start") == 2
  })
  .await;
  let before = record.buffers();
  wait_until("data from the new chain", DATA, || record.buffers() > before + 5).await;

  assert_eq!(record.count("caps"), 2, "new caps: {:?}", record.events());
  assert!(!record.has_eos(), "{:?}", record.events());
  rig.expect_no_event(Duration::from_millis(500)).await;
  assert_eq!(rig.factory.calls.lock().unwrap().len(), 1);
  assert_eq!(rig.factory.element(added.stream_id), consumer);
  rig.assert_pipeline_running();

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
  let removed = rig.expect(StreamEventKind::Removed).await;
  assert_eq!(removed.stream_id, added.stream_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clearing_an_ssrc_frees_its_pad_and_a_new_source_gets_a_new_one() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let audio = source(1000, "alice", MediaType::Audio);
  let session = rig.start(vec![audio.clone()]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  let first = rig.expect(StreamEventKind::Added).await;
  wait_until("rtpbin's pad", DATA, || session_has_pad(&session, "stream_1000_111")).await;
  let rtpbin = session.bin().by_name("rtpbin").unwrap();
  let has_rtpbin_pad = || rtpbin.pads().iter().any(|pad| pad.name() == "recv_rtp_src_0_1000_111");
  assert!(has_rtpbin_pad());

  sender.stop();
  session.sources_removed(vec![1000]).await;
  rig.expect(StreamEventKind::Removed).await;
  wait_until("rtpbin to free the pad", DATA, || {
    !has_rtpbin_pad()
      && !session_has_pad(&session, "stream_1000_111")
      && session
        .bin()
        .children()
        .iter()
        .all(|child| !child.name().starts_with("barrier_1000"))
  })
  .await;

  let sender = Sender::opus(&rig, 1000, 111);
  session.sources_added(vec![audio]).await;
  let second = rig.expect(StreamEventKind::Added).await;
  assert_ne!(second.stream_id, first.stream_id);
  let record = rig.factory.record(second.stream_id);
  wait_until("data at the new consumer", DATA, || record.buffers() > 5).await;
  assert!(has_rtpbin_pad());

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_can_be_replaced_in_the_same_pipeline() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let pipeline = rig.media.pipeline();
  let first_session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  let first = rig.expect(StreamEventKind::Added).await;
  let first_record = rig.factory.record(first.stream_id);
  wait_until("data at the consumer", DATA, || first_record.buffers() > 5).await;
  let base_time = pipeline.base_time();
  let running_time = pipeline.current_running_time().unwrap();

  rig.media.stop_rtp_session(first_session, DRAIN).await;
  let removed = rig.expect(StreamEventKind::Removed).await;
  assert_eq!(removed.stream_id, first.stream_id);
  assert!(first_record.has_eos());
  assert!(pipeline.by_name("session_1").is_none());
  rig.assert_pipeline_running();

  let second_session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  assert_eq!(second_session.generation(), 2);
  let second = rig.expect(StreamEventKind::Added).await;
  assert!(second.stream_id > first.stream_id);
  let second_record = rig.factory.record(second.stream_id);
  wait_until("data at the new consumer", DATA, || second_record.buffers() > 5).await;

  assert_eq!(pipeline.base_time(), base_time);
  assert!(pipeline.current_running_time().unwrap() > running_time);
  rig.assert_pipeline_running();
  sender.stop();
  rig.media.stop_rtp_session(second_session, DRAIN).await;
}

mod errors {
  use super::*;

  /// A consumer whose sink pad returns an error flow after `after` buffers, without posting anything.
  fn failing_flow_consumer(after: usize) -> gstreamer::Element {
    let seen = Arc::new(AtomicUsize::new(0));
    let sink = gstreamer::Pad::builder(gstreamer::PadDirection::Sink)
      .name("sink")
      .chain_function(move |_, _, _| {
        if seen.fetch_add(1, Ordering::SeqCst) >= after {
          Err(gstreamer::FlowError::NotNegotiated)
        }
        else {
          Ok(gstreamer::FlowSuccess::Ok)
        }
      })
      .event_function(|_, _, _| true)
      .build();
    let bin = gstreamer::Bin::new();
    bin.add_pad(&sink).unwrap();
    bin.upcast()
  }

  /// Alice's consumer is `failing`, Bob's records.
  fn alice_fails(rig: &Rig, failing: impl Fn() -> gstreamer::Element + Send + 'static) {
    let recording = rig.factory.recording();
    rig.factory.set(move |info| {
      if info.ssrc == 1000 {
        Some(failing())
      }
      else {
        recording(info)
      }
    });
  }

  /// The events of Alice's and Bob's streams once Alice's stream has been removed and Bob's added.
  async fn alice_removed_bob_added(rig: &mut Rig) -> (crate::pipeline::StreamEvent, crate::pipeline::StreamEvent) {
    let mut alice_removed = None;
    let mut bob_added = None;
    while alice_removed.is_none() || bob_added.is_none() {
      let event = rig.next_event().await;
      match (event.kind, event.ssrc) {
        (StreamEventKind::Added, 2000) => bob_added = Some(event),
        (StreamEventKind::Removed, 1000) => alice_removed = Some(event),
        (StreamEventKind::Added, 1000) => {},
        _ => panic!("unexpected event {event:?}"),
      }
    }
    (alice_removed.unwrap(), bob_added.unwrap())
  }

  async fn assert_bob_unaffected(rig: &mut Rig, bob: &crate::pipeline::StreamEvent) {
    let record = rig.factory.record(bob.stream_id);
    let before = record.buffers();
    wait_until("bob's stream to go on", DATA, || record.buffers() > before + 5).await;
    rig.expect_no_event(Duration::from_millis(300)).await;
    rig.assert_pipeline_running();
    assert!(rig.observed.failures.lock().unwrap().is_empty());
    assert_eq!(pads_with_failed_flow(&rig.media.pipeline()), Vec::<String>::new());
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_consumer_returning_an_error_flow_retires_only_its_stream() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig
      .start(vec![
        source(1000, "alice", MediaType::Audio),
        source(2000, "bob", MediaType::Audio),
      ])
      .await;
    alice_fails(&rig, || failing_flow_consumer(3));
    let alice = Sender::opus(&rig, 1000, 111);
    let bob = Sender::opus(&rig, 2000, 111);

    let (alice_removed, bob_added) = alice_removed_bob_added(&mut rig).await;
    assert!(alice_removed.error.is_some(), "{alice_removed:?}");
    assert_bob_unaffected(&mut rig, &bob_added).await;
    // The stream that failed is not activated again while its source stays as it is.
    assert_eq!(rig.factory.calls.lock().unwrap().len(), 2);

    alice.stop();
    bob.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_consumer_posting_an_error_retires_only_its_stream() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig
      .start(vec![
        source(1000, "alice", MediaType::Audio),
        source(2000, "bob", MediaType::Audio),
      ])
      .await;
    alice_fails(&rig, || {
      let identity = gstreamer::ElementFactory::make("identity")
        .property("error-after", 3)
        .build()
        .unwrap();
      let sink = gstreamer::ElementFactory::make("fakesink")
        .property("async", false)
        .build()
        .unwrap();
      consumer_bin(&[identity, sink])
    });
    let alice = Sender::opus(&rig, 1000, 111);
    let bob = Sender::opus(&rig, 2000, 111);

    let (alice_removed, bob_added) = alice_removed_bob_added(&mut rig).await;
    assert!(alice_removed.error.is_some(), "{alice_removed:?}");
    assert_bob_unaffected(&mut rig, &bob_added).await;

    alice.stop();
    bob.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_consumer_that_fails_to_start_is_removed_without_having_been_added() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    rig.factory.set(|_| {
      let queue = gstreamer::ElementFactory::make("queue").build().unwrap();
      let sink = gstreamer::ElementFactory::make("filesink")
        .property("location", "/nonexistent-directory-for-gst-meet-tests/file")
        .build()
        .unwrap();
      Some(consumer_bin(&[queue, sink]))
    });
    let audio = source(1000, "alice", MediaType::Audio);
    let session = rig.start(vec![audio.clone()]).await;
    let sender = Sender::opus(&rig, 1000, 111);

    let removed = rig.expect(StreamEventKind::Removed).await;
    assert!(removed.error.is_some(), "{removed:?}");
    rig.expect_no_event(Duration::from_millis(800)).await;
    rig.assert_pipeline_running();
    assert_eq!(rig.factory.calls.lock().unwrap().len(), 1);
    assert!(rig.observed.failures.lock().unwrap().is_empty());

    // Announcing the source again is a new attempt.
    rig.factory.set(rig.factory.recording());
    session.sources_added(vec![audio]).await;
    let added = rig.expect(StreamEventKind::Added).await;
    assert_ne!(added.stream_id, removed.stream_id);

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }
}

mod drain {
  use super::*;

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn draining_ends_every_stream_with_an_eos() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig
      .start(vec![
        source(1000, "alice", MediaType::Audio),
        source(2000, "bob", MediaType::Audio),
      ])
      .await;
    let alice = Sender::opus(&rig, 1000, 111);
    let bob = Sender::opus(&rig, 2000, 111);
    let first = rig.expect(StreamEventKind::Added).await;
    let second = rig.expect(StreamEventKind::Added).await;
    let records = [
      rig.factory.record(first.stream_id),
      rig.factory.record(second.stream_id),
    ];
    wait_until("data", DATA, || records.iter().all(|record| record.buffers() > 3)).await;

    session.drain(DRAIN).await.unwrap();
    assert!(records.iter().all(|record| record.has_eos()));
    let mut removed = [
      rig.expect(StreamEventKind::Removed).await.stream_id,
      rig.expect(StreamEventKind::Removed).await.stream_id,
    ];
    removed.sort();
    assert_eq!(
      removed,
      [
        first.stream_id.min(second.stream_id),
        first.stream_id.max(second.stream_id)
      ]
    );

    // Nothing is activated afterwards.
    session
      .sources_added(vec![source(3000, "carol", MediaType::Audio)])
      .await;
    let carol = Sender::opus(&rig, 3000, 111);
    rig.expect_no_event(Duration::from_millis(800)).await;
    assert_eq!(rig.factory.calls.lock().unwrap().len(), 2);

    alice.stop();
    bob.stop();
    carol.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn draining_reaches_a_stream_that_is_still_activating() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let sender = Sender::opus(&rig, 1000, 111);
    let session = rig.start(vec![]).await;
    wait_until("rtpbin's pad", DATA, || session_has_pad(&session, "stream_1000_111")).await;
    let records = Arc::new(Mutex::new(Vec::new()));
    rig.factory.set({
      let records = records.clone();
      move |_| {
        std::thread::sleep(Duration::from_millis(600));
        let (consumer, record) = recording_consumer();
        records.lock().unwrap().push(record);
        Some(consumer)
      }
    });
    session
      .sources_added(vec![source(1000, "alice", MediaType::Audio)])
      .await;
    wait_until("the activation to start", DATA, || {
      !rig.factory.calls.lock().unwrap().is_empty()
    })
    .await;

    session.drain(Duration::from_secs(5)).await.unwrap();
    let record = records.lock().unwrap()[0].clone();
    assert!(record.has_eos(), "{:?}", record.events());
    rig.expect(StreamEventKind::Added).await;
    rig.expect(StreamEventKind::Removed).await;

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn draining_waits_for_a_stream_that_is_being_retired() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 3).await;

    session.sources_removed(vec![1000]).await;
    session.drain(DRAIN).await.unwrap();
    assert!(record.has_eos());
    rig.expect(StreamEventKind::Removed).await;

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn draining_fails_when_a_consumer_cannot_keep_up() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    // Every buffer takes two seconds, so the EOS behind the queued ones does not get there in time.
    rig.factory.set(|_| {
      let queue = gstreamer::ElementFactory::make("queue").build().unwrap();
      let slow = gstreamer::ElementFactory::make("identity")
        .property("sleep-time", 2_000_000u32)
        .build()
        .unwrap();
      let sink = gstreamer::ElementFactory::make("fakesink")
        .property("async", false)
        .build()
        .unwrap();
      Some(consumer_bin(&[queue, slow, sink]))
    });
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    rig.expect(StreamEventKind::Added).await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let result = session.drain(Duration::from_millis(500)).await;
    assert!(result.is_err(), "{result:?}");
    // The stream is torn down regardless.
    rig.expect(StreamEventKind::Removed).await;

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  /// A hook that waits for the drain it asks for, as an application's callback might through a blocking call.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn draining_from_the_stream_event_hook_does_not_deadlock() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = Arc::new(rig.start(vec![source(1000, "alice", MediaType::Audio)]).await);
    let drained = Arc::new(Mutex::new(None));
    rig.set_event_hook({
      let (session, drained) = (session.clone(), drained.clone());
      let runtime = tokio::runtime::Handle::current();
      move |event| {
        if event.kind == StreamEventKind::Added {
          let result = runtime.block_on(session.drain(DRAIN));
          *drained.lock().unwrap() = Some(result.map_err(|e| e.to_string()));
        }
      }
    });
    let sender = Sender::opus(&rig, 1000, 111);

    rig.expect(StreamEventKind::Added).await;
    wait_until("the drain to return", DATA, || drained.lock().unwrap().is_some()).await;
    assert_eq!(*drained.lock().unwrap(), Some(Ok(())));
    rig.expect(StreamEventKind::Removed).await;

    sender.stop();
    rig.set_event_hook(|_| {});
    let session = Arc::into_inner(session).expect("the hook should not hold on to the session");
    rig.media.stop_rtp_session(session, DRAIN).await;
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_codec_switch_keeps_the_stream_and_its_consumer_and_delivers_the_new_caps() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let session = rig.start(vec![source(2000, "bob", MediaType::Video)]).await;
  let vp8 = Sender::vp8(&rig, 2000, 96);
  let added = rig.expect(StreamEventKind::Added).await;
  let record = rig.factory.record(added.stream_id);
  let consumer = rig.factory.element(added.stream_id);
  wait_until("VP8 at the consumer", DATA, || record.buffers() > 3).await;
  assert!(record
    .events()
    .iter()
    .any(|event| event.starts_with("caps video/x-vp8")));

  let vp9 = Sender::vp9_after(&rig, &vp8, 2000, 98);
  vp8.stop();
  wait_until("VP9 caps at the consumer", DATA, || {
    record
      .events()
      .iter()
      .any(|event| event.starts_with("caps video/x-vp9"))
  })
  .await;
  let before = record.buffers();
  wait_until("VP9 data at the consumer", DATA, || record.buffers() > before + 3).await;

  assert!(!record.has_eos(), "{:?}", record.events());
  rig.expect_no_event(Duration::from_millis(500)).await;
  assert_eq!(rig.factory.calls.lock().unwrap().len(), 1);
  assert_eq!(rig.factory.element(added.stream_id), consumer);
  rig.assert_pipeline_running();

  vp9.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
  assert_eq!(rig.expect(StreamEventKind::Removed).await.stream_id, added.stream_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_streams_of_a_new_participant_wait_until_the_application_is_done_with_it() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let (open, gate) = tokio::sync::watch::channel(false);
  *rig.observed.gate.lock().unwrap() = Some(gate);
  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  wait_until("rtpbin's pad", DATA, || session_has_pad(&session, "stream_1000_111")).await;

  rig.expect_no_event(Duration::from_millis(500)).await;
  assert!(rig.factory.calls.lock().unwrap().is_empty(), "no consumer is asked for before the application is ready");
  assert_eq!(*rig.observed.participants.lock().unwrap(), ["alice"]);

  open.send(true).unwrap();
  let added = rig.expect(StreamEventKind::Added).await;
  let record = rig.factory.record(added.stream_id);
  wait_until("data at the consumer", DATA, || record.buffers() > 5).await;
  // The participant is asked about once, however many sources it announces.
  session.sources_added(vec![source(1001, "alice", MediaType::Video)]).await;
  rig.expect_no_event(Duration::from_millis(300)).await;
  assert_eq!(*rig.observed.participants.lock().unwrap(), ["alice"]);

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_does_not_wait_for_the_application_to_be_done_with_a_participant() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let (open, gate) = tokio::sync::watch::channel(false);
  *rig.observed.gate.lock().unwrap() = Some(gate);
  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  wait_until("rtpbin's pad", DATA, || session_has_pad(&session, "stream_1000_111")).await;

  // A callback that is not done with the participant calls `drain_streams`: it must get its answer.
  let started = std::time::Instant::now();
  session.drain(DRAIN).await.expect("nothing to drain");
  assert!(started.elapsed() < Duration::from_secs(1), "{:?}", started.elapsed());

  // And the participant's streams stay down afterwards.
  open.send(true).unwrap();
  rig.expect_no_event(Duration::from_millis(500)).await;
  assert!(rig.factory.calls.lock().unwrap().is_empty());

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_that_found_no_consumer_is_activated_when_one_is_registered_later() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  rig.factory.set(|_| None);
  rig.has_stream_handler.store(false, Ordering::SeqCst);
  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  // The factory is asked when the pad appears and, depending on who is quicker, once more when its first data is
  // reported; how often it declined before it is registered later is not what the test is about.
  wait_until("the factory to be asked", DATA, || {
    !rig.factory.calls.lock().unwrap().is_empty()
  })
  .await;
  let mut declined = rig.factory.calls.lock().unwrap().len();
  loop {
    tokio::time::sleep(Duration::from_millis(300)).await;
    let now = rig.factory.calls.lock().unwrap().len();
    if now == declined {
      break;
    }
    declined = now;
  }
  rig.expect_no_event(Duration::from_millis(300)).await;

  rig.factory.set(rig.factory.recording());
  rig.media.reevaluate_streams();
  let added = rig.expect(StreamEventKind::Added).await;
  let record = rig.factory.record(added.stream_id);
  wait_until("data at the consumer", DATA, || record.buffers() > 5).await;
  assert_eq!(rig.factory.calls.lock().unwrap().len(), declined + 1);

  // Looking again changes nothing for a stream that is there.
  rig.media.reevaluate_streams();
  rig.expect_no_event(Duration::from_millis(300)).await;
  assert_eq!(rig.factory.calls.lock().unwrap().len(), declined + 1);

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}
