use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use gstreamer::prelude::*;

use super::support::*;
use crate::{
  conference::ReceiveMode,
  pipeline::{
    owner::{Owner, StreamPart},
    SessionFailure, StreamEventKind,
  },
  source::MediaType,
};

const DATA: Duration = Duration::from_secs(10);
const DRAIN: Duration = Duration::from_secs(3);

fn chain_bins(pipeline: &gstreamer::Pipeline) -> Vec<String> {
  pipeline
    .children()
    .iter()
    .map(|child| child.name().to_string())
    .filter(|name| name.starts_with("chain_"))
    .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_consumer_is_reached_through_a_ghost_pad_and_left_running() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let pipeline = rig.media.pipeline();
  // Like an audiomixer in a bin the application added.
  let (mixer_bin, shared_sink_record) = {
    let funnel = gstreamer::ElementFactory::make("funnel").name("mixer").build().unwrap();
    let (consumer, record) = recording_consumer();
    let bin = gstreamer::Bin::new();
    bin.add_many([&funnel, &consumer]).unwrap();
    funnel.link(&consumer).unwrap();
    (bin, record)
  };
  pipeline.add(&mixer_bin).unwrap();
  mixer_bin.sync_state_with_parent().unwrap();
  let mixer = mixer_bin.by_name("mixer").unwrap();
  rig.factory.set({
    let mixer = mixer.clone();
    move |_| Some(mixer.clone())
  });

  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  let added = rig.expect(StreamEventKind::Added).await;
  wait_until("data through the mixer", DATA, || shared_sink_record.buffers() > 5).await;
  assert_eq!(mixer.sink_pads().len(), 1);
  assert!(mixer_bin.static_pad(&format!("stream_{}", added.stream_id)).is_some());

  session.sources_removed(vec![1000]).await;
  let removed = rig.expect(StreamEventKind::Removed).await;
  assert_eq!(removed.error, None);
  // The stream is gone from the mixer, which goes on: it never saw an EOS.
  assert!(mixer.sink_pads().is_empty());
  assert!(mixer_bin.static_pad(&format!("stream_{}", added.stream_id)).is_none());
  assert!(mixer_bin.parent().is_some());
  assert!(!shared_sink_record.has_eos());
  rig.assert_pipeline_running();

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_participant_bin_is_used_when_the_factory_has_nothing() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let pipeline = rig.media.pipeline();
  let (consumer, record) = recording_consumer();
  let participant_bin = gstreamer::Bin::with_name("participant_alice");
  participant_bin.add(&consumer).unwrap();
  participant_bin
    .add_pad(
      &gstreamer::GhostPad::builder_with_target(&consumer.static_pad("sink").unwrap())
        .unwrap()
        .name("audio")
        .build(),
    )
    .unwrap();
  pipeline.add(&participant_bin).unwrap();
  participant_bin.sync_state_with_parent().unwrap();
  rig.factory.set(|_| None);

  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  rig.expect(StreamEventKind::Added).await;
  wait_until("data in the participant bin", DATA, || record.buffers() > 5).await;

  session.sources_removed(vec![1000]).await;
  rig.expect(StreamEventKind::Removed).await;
  assert!(participant_bin.static_pad("audio").unwrap().peer().is_none());
  assert!(participant_bin.parent().is_some());
  assert!(!record.has_eos());

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_handler_alone_gets_the_events_and_no_chain() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  rig.factory.set(|_| None);
  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  let added = rig.expect(StreamEventKind::Added).await;
  assert_eq!(added.source_name.as_deref(), Some("alice-a0"));
  assert_eq!(chain_bins(&rig.media.pipeline()), Vec::<String>::new());

  session.sources_removed(vec![1000]).await;
  assert_eq!(rig.expect(StreamEventKind::Removed).await.stream_id, added.stream_id);

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_is_activated_when_there_is_nothing_to_deliver_to() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  rig.factory.set(|_| None);
  rig.has_stream_handler.store(false, Ordering::SeqCst);
  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);

  rig.expect_no_event(Duration::from_millis(1000)).await;
  // Asked when the pad appears and, depending on who is quicker, once more when its first data is reported. Not
  // again for every packet.
  let asked = rig.factory.calls.lock().unwrap().len();
  assert!((1..=2).contains(&asked), "asked {asked} times");
  tokio::time::sleep(Duration::from_millis(500)).await;
  assert_eq!(
    rig.factory.calls.lock().unwrap().len(),
    asked,
    "asked again for a packet"
  );
  assert_eq!(chain_bins(&rig.media.pipeline()), Vec::<String>::new());
  rig.assert_pipeline_running();

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decoded_streams_deliver_raw_audio() {
  let mut rig = Rig::new(ReceiveMode::Decoded);
  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  let added = rig.expect(StreamEventKind::Added).await;
  let record = rig.factory.record(added.stream_id);
  wait_until("raw audio", DATA, || record.buffers() > 5).await;
  assert!(
    record
      .events()
      .iter()
      .any(|event| event.starts_with("caps audio/x-raw")),
    "{:?}",
    record.events()
  );

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retransmissions_are_requested_for_video_sources_only() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let session = rig
    .start(vec![
      source(1000, "alice", MediaType::Audio),
      source(2000, "alice", MediaType::Video),
    ])
    .await;
  let audio = Sender::opus(&rig, 1000, 111);
  let video = Sender::vp8(&rig, 2000, 96);
  rig.expect(StreamEventKind::Added).await;
  rig.expect(StreamEventKind::Added).await;

  let rtpbin = session.bin().by_name("rtpbin").unwrap();
  let mut retransmitting = Vec::new();
  let mut children = rtpbin.downcast_ref::<gstreamer::Bin>().unwrap().iterate_recurse();
  while let Ok(Some(child)) = children.next() {
    if child
      .factory()
      .is_some_and(|factory| factory.name() == "rtpjitterbuffer")
    {
      retransmitting.push(child.property::<bool>("do-retransmission"));
    }
  }
  retransmitting.sort();
  assert_eq!(retransmitting, [false, true]);

  audio.stop();
  video.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

/// The application's encoders feed the payloaders for the whole conference; sessions come and go behind a barrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_send_path_survives_the_session() {
  let rig = Rig::new(ReceiveMode::Encoded);
  assert!(
    rig.media.audio_sink_element().is_none(),
    "there is no payloader before the first session"
  );

  let first = rig.start(vec![]).await;
  let payloader = rig
    .media
    .audio_sink_element()
    .expect("the first session creates the send path");
  assert_eq!(payloader.property::<u32>("ssrc"), rig.media.local_ssrcs().audio);
  assert_eq!(payloader.property::<u32>("pt"), 111);
  assert!(rig.media.video_sink_element().is_some());

  let pipeline = rig.media.pipeline();
  let source = gstreamer::ElementFactory::make("audiotestsrc")
    .property("is-live", true)
    .build()
    .unwrap();
  let convert = gstreamer::ElementFactory::make("audioconvert").build().unwrap();
  let opus = gstreamer::ElementFactory::make("opusenc").build().unwrap();
  pipeline.add_many([&source, &convert, &opus]).unwrap();
  gstreamer::Element::link_many([&source, &convert, &opus, &payloader]).unwrap();
  for element in [&source, &convert, &opus] {
    element.sync_state_with_parent().unwrap();
  }

  let sent = Arc::new(AtomicUsize::new(0));
  let count_sent = |session: &crate::pipeline::RtpSession| {
    let sent = sent.clone();
    let sink = session
      .bin()
      .by_name("transport_rtp_out")
      .unwrap()
      .static_pad("sink")
      .unwrap();
    sink.add_probe(gstreamer::PadProbeType::BUFFER, move |_, _| {
      sent.fetch_add(1, Ordering::SeqCst);
      gstreamer::PadProbeReturn::Ok
    });
  };
  count_sent(&first);
  wait_until("RTP out of the first session", DATA, || sent.load(Ordering::SeqCst) > 5).await;
  let barrier_src = pipeline.by_name("send_barrier").unwrap().static_pad("src").unwrap();
  assert!(barrier_src.peer().is_some());

  rig.media.stop_rtp_session(first, DRAIN).await;
  assert!(
    barrier_src.peer().is_none(),
    "the send path is detached from the ended session"
  );
  tokio::time::sleep(Duration::from_millis(300)).await;
  rig.assert_pipeline_running();
  assert_eq!(pads_with_failed_flow(&pipeline), Vec::<String>::new());

  let second = rig.start(vec![]).await;
  assert_eq!(
    rig.media.audio_sink_element().unwrap(),
    payloader,
    "the payloader stays"
  );
  let before = sent.load(Ordering::SeqCst);
  count_sent(&second);
  wait_until("RTP out of the second session", DATA, || {
    sent.load(Ordering::SeqCst) > before + 5
  })
  .await;
  rig.media.stop_rtp_session(second, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_of_the_session_is_reported_as_its_failure_and_nothing_else_stops() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  let added = rig.expect(StreamEventKind::Added).await;

  let rtpbin = session.bin().by_name("rtpbin").unwrap();
  let error = gstreamer::message::Error::builder(gstreamer::CoreError::Failed, "the transport broke")
    .src(&rtpbin)
    .build();
  rtpbin.post_message(error).unwrap();
  wait_until("the failure", DATA, || {
    !rig.observed.failures.lock().unwrap().is_empty()
  })
  .await;
  let failures = rig.observed.failures.lock().unwrap().clone();
  assert!(
    matches!(failures.as_slice(), [(1, SessionFailure::Transport(message))] if message.contains("the transport broke"))
  );

  rig.expect_no_event(Duration::from_millis(300)).await;
  rig.assert_pipeline_running();
  let record = rig.factory.record(added.stream_id);
  let before = record.buffers();
  wait_until("the stream to go on", DATA, || record.buffers() > before + 5).await;

  sender.stop();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn errors_of_sessions_and_streams_that_are_gone_are_ignored() {
  let rig = Rig::new(ReceiveMode::Encoded);
  let session = rig.start(vec![]).await;
  let pipeline = rig.media.pipeline();
  let owned_by = |owner| {
    let element = gstreamer::ElementFactory::make("identity").build().unwrap();
    crate::pipeline::owner::mark(&element, owner);
    pipeline.add(&element).unwrap();
    element
  };
  let session_element = owned_by(Owner::Session { session: 99 });
  let stream_element = owned_by(Owner::Stream {
    session: 99,
    stream_id: 5,
    part: StreamPart::Consumer,
  });
  for element in [&session_element, &stream_element] {
    let error = gstreamer::message::Error::builder(gstreamer::CoreError::Failed, "too late")
      .src(element)
      .build();
    element.post_message(error).unwrap();
  }
  tokio::time::sleep(Duration::from_millis(500)).await;
  assert!(rig.observed.failures.lock().unwrap().is_empty());
  rig.assert_pipeline_running();
  rig.media.stop_rtp_session(session, DRAIN).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_nobody_owns_stops_the_pipeline_and_is_fatal() {
  let rig = Rig::new(ReceiveMode::Encoded);
  let session = rig.start(vec![]).await;
  let pipeline = rig.media.pipeline();
  let application_element = gstreamer::ElementFactory::make("identity").build().unwrap();
  pipeline.add(&application_element).unwrap();
  application_element.sync_state_with_parent().unwrap();

  let error = gstreamer::message::Error::builder(gstreamer::CoreError::Failed, "the application's element broke")
    .src(&application_element)
    .build();
  application_element.post_message(error).unwrap();
  wait_until("the fatal error", DATA, || {
    !rig.observed.fatal.lock().unwrap().is_empty()
  })
  .await;
  assert!(rig.observed.fatal.lock().unwrap()[0].contains("the application's element broke"));
  assert_eq!(pipeline.current_state(), gstreamer::State::Null);
  assert!(rig.observed.failures.lock().unwrap().is_empty());
  let _ = session;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutting_down_stops_the_pipeline_without_waiting_for_a_message_and_only_once() {
  let mut rig = Rig::new(ReceiveMode::Encoded);
  let _session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
  let sender = Sender::opus(&rig, 1000, 111);
  let added = rig.expect(StreamEventKind::Added).await;
  let record = rig.factory.record(added.stream_id);
  wait_until("data", DATA, || record.buffers() > 3).await;

  let started = tokio::time::Instant::now();
  rig.media.shutdown().await;
  assert!(started.elapsed() < Duration::from_secs(4), "{:?}", started.elapsed());
  assert_eq!(rig.media.pipeline().current_state(), gstreamer::State::Null);
  assert!(record.has_eos(), "the stream of a session that is left is drained");
  assert_eq!(rig.expect(StreamEventKind::Removed).await.stream_id, added.stream_id);

  rig.media.shutdown().await;
  sender.stop();
}
