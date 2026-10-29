use std::{
  collections::HashMap,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
  },
  time::Duration,
};

use gstreamer::prelude::*;

use super::support::*;
use crate::{
  conference::ReceiveMode,
  pipeline::{
    codec::Codec,
    owner::{self, Owner, StreamPart},
    streams::PadKey,
    CodecName, RtpSession, StreamEvent, StreamEventKind, StreamId,
  },
  source::MediaType,
};

const DATA: Duration = Duration::from_secs(10);
const DRAIN: Duration = Duration::from_secs(3);

fn session_has_pad(session: &RtpSession, prefix: &str) -> bool {
  session.bin().pads().iter().any(|pad| pad.name().starts_with(prefix))
}

fn chain_generations(pipeline: &gstreamer::Pipeline, stream_id: StreamId) -> Vec<u64> {
  let mut generations: Vec<u64> = pipeline
    .children()
    .iter()
    .filter_map(|child| match owner::of(child) {
      Some(Owner::Stream {
        stream_id: id,
        part: StreamPart::Chain(generation),
        ..
      }) if id == stream_id => Some(generation),
      _ => None,
    })
    .collect();
  generations.sort();
  generations
}

/// Holds back whatever reaches the consumer, so that the chain in front of it cannot finish.
struct Stall {
  pad: gstreamer::Pad,
  probe: Option<gstreamer::PadProbeId>,
}

impl Stall {
  /// Returns once the chain in front of the consumer has run into it, with the data behind it queued up.
  async fn new(consumer: &gstreamer::Element) -> Self {
    let pad = consumer.static_pad("sink").unwrap();
    let hit = Arc::new(AtomicBool::new(false));
    let probe = pad.add_probe(gstreamer::PadProbeType::BLOCK_DOWNSTREAM, {
      let hit = hit.clone();
      move |_, _| {
        hit.store(true, Ordering::SeqCst);
        gstreamer::PadProbeReturn::Ok
      }
    });
    wait_until("the stall to be hit", DATA, || hit.load(Ordering::SeqCst)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    Self { pad, probe }
  }

  fn release(&mut self) {
    if let Some(probe) = self.probe.take() {
      self.pad.remove_probe(probe);
    }
  }
}

async fn next_event_within(rig: &mut Rig, wait: Duration) -> Option<StreamEvent> {
  tokio::time::timeout(wait, rig.events.recv()).await.ok().flatten()
}

mod notifications_that_cannot_be_acted_on_are_not_lost {
  use super::*;

  /// A sink factory that takes `delay` and records what its consumers get.
  fn slow_factory(rig: &Rig, delay: Duration) -> Arc<Mutex<Vec<Record>>> {
    let records = Arc::new(Mutex::new(Vec::new()));
    rig.factory.set({
      let records = records.clone();
      move |_| {
        std::thread::sleep(delay);
        let (consumer, record) = recording_consumer();
        records.lock().unwrap().push(record);
        Some(consumer)
      }
    });
    records
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_payload_type_change_while_the_stream_is_activating() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let sender = Sender::opus(&rig, 1000, 111);
    let session = rig.start(vec![]).await;
    wait_until("rtpbin's pad", DATA, || session_has_pad(&session, "stream_1000_111")).await;
    let records = slow_factory(&rig, Duration::from_millis(800));

    session
      .sources_added(vec![source(1000, "alice", MediaType::Audio)])
      .await;
    wait_until("the activation to start", DATA, || {
      !rig.factory.calls.lock().unwrap().is_empty()
    })
    .await;
    // The sender moves on while the consumer is being made. The pad of the new payload type reports its data once.
    sender.set_pt(112);
    wait_until("the pad of the new payload type", DATA, || {
      session_has_pad(&session, "stream_1000_112")
    })
    .await;

    rig.expect(StreamEventKind::Added).await;
    let record = records.lock().unwrap()[0].clone();
    // Nothing arrives on the pad the stream was activated on any more: it has to have moved to get any data.
    wait_until("data from the new pad", DATA, || record.buffers() > 5).await;
    assert!(!record.has_eos(), "{:?}", record.events());
    rig.expect_no_event(Duration::from_millis(300)).await;
    assert_eq!(rig.factory.calls.lock().unwrap().len(), 1);

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_payload_type_change_back_while_the_stream_is_switching() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 5).await;

    // The old chain cannot finish, so the switch to the new payload type takes as long as the stall lasts.
    let mut stall = Stall::new(&rig.factory.element(added.stream_id)).await;
    sender.set_pt(112);
    wait_until("the switch to start", DATA, || {
      chain_generations(&rig.media.pipeline(), added.stream_id).len() == 2
    })
    .await;
    sender.set_pt(111);
    // Its pad reports its data, once, to a stream that is busy.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = record.buffers();
    stall.release();

    // It lands on 112, where nothing arrives any more, and has to move back to be fed.
    wait_until("data after the move back", DATA, || record.buffers() > before + 10).await;
    assert!(!record.has_eos(), "{:?}", record.events());
    rig.expect_no_event(Duration::from_millis(300)).await;
    assert_eq!(rig.factory.calls.lock().unwrap().len(), 1);

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_payload_type_that_cannot_be_set_up_is_not_tried_again_and_again() {
    init();
    // The AV1 chain is the one that cannot be built; with the elements installed it can.
    if ["rtpav1depay", "av1parse"]
      .iter()
      .all(|factory| gstreamer::ElementFactory::find(factory).is_some())
    {
      return;
    }
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let mut offer = offer(vec![source(2000, "bob", MediaType::Video)]);
    offer.codecs.push(Codec {
      name: CodecName::Av1,
      pt: 100,
      rtx_pt: None,
      rtcp_fbs: vec![],
    });
    let generation = rig.media.next_generation();
    let session = rig
      .media
      .start_rtp_session(generation, &offer, fake_transport(rig.port))
      .await
      .unwrap();
    let sender = Sender::vp8(&rig, 2000, 96);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 3).await;

    // The sender claims a payload type of a codec that cannot be received.
    sender.set_pt(100);
    wait_until("the pad of the new payload type", DATA, || {
      session_has_pad(&session, "stream_2000_100")
    })
    .await;
    wait_until("an attempt", DATA, || session.streams().chains_started() >= 2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let attempts = session.streams().chains_started();
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(session.streams().chains_started(), attempts, "tried again");
    rig.expect_no_event(Duration::from_millis(300)).await;
    rig.assert_pipeline_running();

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  /// The pad of the other payload type delivered while the first activation failed.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_failed_activation_goes_on_with_the_other_pad() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let sender = Sender::opus(&rig, 1000, 111);
    let session = rig.start(vec![]).await;
    wait_until("rtpbin's pad", DATA, || session_has_pad(&session, "stream_1000_111")).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let records = Arc::new(Mutex::new(Vec::new()));
    rig.factory.set({
      let (calls, records) = (calls.clone(), records.clone());
      move |_| {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
          std::thread::sleep(Duration::from_millis(800));
          let queue = gstreamer::ElementFactory::make("queue").build().unwrap();
          let sink = gstreamer::ElementFactory::make("filesink")
            .property("location", "/nonexistent-directory-for-gst-meet-tests/file")
            .build()
            .unwrap();
          return Some(consumer_bin(&[queue, sink]));
        }
        let (consumer, record) = recording_consumer();
        records.lock().unwrap().push(record);
        Some(consumer)
      }
    });

    session
      .sources_added(vec![source(1000, "alice", MediaType::Audio)])
      .await;
    wait_until("the activation to start", DATA, || calls.load(Ordering::SeqCst) == 1).await;
    sender.set_pt(112);
    wait_until("the pad of the new payload type", DATA, || {
      session_has_pad(&session, "stream_1000_112")
    })
    .await;

    let removed = rig.expect(StreamEventKind::Removed).await;
    assert!(removed.error.is_some(), "{removed:?}");
    let added = rig.expect(StreamEventKind::Added).await;
    assert_ne!(added.stream_id, removed.stream_id);
    wait_until("the second consumer", DATA, || !records.lock().unwrap().is_empty()).await;
    let record = records.lock().unwrap()[0].clone();
    wait_until("data", DATA, || record.buffers() > 5).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn flipping_payload_types_while_streams_come_and_go() {
    struct Rng(u64);
    impl Rng {
      fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
      }
    }

    for (seed, max_sleep) in [(1u64, 400u64), (2, 120), (3, 40)] {
      let mut rig = Rig::new(ReceiveMode::Encoded);
      let audio = source(1000, "alice", MediaType::Audio);
      let session = rig.start(vec![audio.clone()]).await;
      let sender = Sender::opus(&rig, 1000, 111);
      rig.expect(StreamEventKind::Added).await;
      let mut rng = Rng(seed * 7919 + 1);
      let mut pt = 111;
      for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(rng.below(max_sleep))).await;
        if rng.below(10) == 0 {
          session.sources_removed(vec![1000]).await;
          tokio::time::sleep(Duration::from_millis(rng.below(max_sleep))).await;
          session.sources_added(vec![audio.clone()]).await;
        }
        else {
          pt = if pt == 111 { 112 } else { 111 };
          sender.set_pt(pt);
        }
      }
      tokio::time::sleep(Duration::from_millis(1500)).await;

      let mut by_id: HashMap<StreamId, Vec<StreamEventKind>> = HashMap::new();
      while let Some(event) = next_event_within(&mut rig, Duration::from_millis(200)).await {
        by_id.entry(event.stream_id).or_default().push(event.kind);
      }
      let live: Vec<StreamId> = by_id
        .iter()
        .filter(|(_, kinds)| kinds.last() == Some(&StreamEventKind::Added))
        .map(|(id, _)| *id)
        .collect();
      // One stream is live, at the latest, and it gets data.
      assert!(live.len() <= 1, "seed {seed}: {live:?}");
      let live = live
        .first()
        .copied()
        .unwrap_or_else(|| panic!("seed {seed}: no live stream, {by_id:?}"));
      let record = rig.factory.record(live);
      let before = record.buffers();
      tokio::time::sleep(Duration::from_millis(600)).await;
      assert!(
        record.buffers() > before,
        "seed {seed}: the live stream {live} gets no data"
      );

      sender.stop();
      rig.media.stop_rtp_session(session, DRAIN).await;
    }
  }
}

mod failures_are_reported_and_attributed_correctly {
  use super::*;

  /// The wiring of the session bin: the ghost pad is what is linked, and the barrier behind it must notice.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_failure_after_a_relink_without_data_in_between_is_reported() {
    let rig = Rig::new(ReceiveMode::Encoded);
    let session = rig.start(vec![]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    wait_until("rtpbin's pad", DATA, || session_has_pad(&session, "stream_1000_111")).await;
    let barrier = session
      .bin()
      .children()
      .into_iter()
      .find(|child| child.name().starts_with("barrier_1000_111"))
      .and_then(|child| child.downcast::<crate::pipeline::elements::StreamBarrier>().ok())
      .expect("the barrier of the pad");
    let ghost = session
      .bin()
      .pads()
      .into_iter()
      .find(|pad| pad.name().starts_with("stream_1000_111"))
      .expect("the ghost pad");

    let failures = Arc::new(AtomicUsize::new(0));
    barrier.set_callbacks(|| {}, {
      let failures = failures.clone();
      move |_| {
        failures.fetch_add(1, Ordering::SeqCst);
      }
    });
    let refusing_sink = gstreamer::Pad::builder(gstreamer::PadDirection::Sink)
      .name("refusing_sink")
      .chain_function(|_, _, _| Err(gstreamer::FlowError::NotNegotiated))
      .event_function(|_, _, _| true)
      .build();
    refusing_sink.set_active(true).unwrap();

    ghost.link(&refusing_sink).unwrap();
    wait_until("the first failure", DATA, || failures.load(Ordering::SeqCst) == 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(failures.load(Ordering::SeqCst), 1);

    // The failed chain is replaced while the sender is quiet, by one that fails just as well.
    sender.stop();
    tokio::time::sleep(Duration::from_millis(300)).await;
    ghost.unlink(&refusing_sink).unwrap();
    ghost.link(&refusing_sink).unwrap();
    let sender = Sender::opus(&rig, 1000, 111);
    wait_until("the second failure", DATA, || failures.load(Ordering::SeqCst) == 2).await;

    sender.stop();
    ghost.unlink(&refusing_sink).unwrap();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn the_failure_of_the_incoming_pad_during_a_switch_is_the_streams() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 5).await;

    let mut stall = Stall::new(&rig.factory.element(added.stream_id)).await;
    sender.set_pt(112);
    wait_until("the switch to start", DATA, || {
      chain_generations(&rig.media.pipeline(), added.stream_id).len() == 2
    })
    .await;
    // What is linked to the new pad fails, and the old chain that is still on its way out is told about as well.
    session
      .streams()
      .pad_failed(PadKey { ssrc: 1000, pt: 111 }, gstreamer::FlowError::NotLinked);
    session
      .streams()
      .pad_failed(PadKey { ssrc: 1000, pt: 112 }, gstreamer::FlowError::NotNegotiated);
    stall.release();

    let removed = next_event_within(&mut rig, Duration::from_secs(8))
      .await
      .expect("the stream whose new chain failed was not retired");
    assert_eq!(removed.kind, StreamEventKind::Removed);
    assert_eq!(removed.stream_id, added.stream_id);
    assert!(
      removed
        .error
        .as_deref()
        .is_some_and(|error| error.contains("NotNegotiated")),
      "{removed:?}"
    );

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  /// A failure of the pad the stream is leaving belongs to the chain that is being thrown away.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn the_failure_of_the_outgoing_pad_during_a_switch_does_not_stop_the_new_chain() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 5).await;

    let mut stall = Stall::new(&rig.factory.element(added.stream_id)).await;
    sender.set_pt(112);
    wait_until("the switch to start", DATA, || {
      chain_generations(&rig.media.pipeline(), added.stream_id).len() == 2
    })
    .await;
    session
      .streams()
      .pad_failed(PadKey { ssrc: 1000, pt: 111 }, gstreamer::FlowError::NotLinked);
    stall.release();

    wait_until("the new chain's stream start", DATA, || {
      record.count("stream-start") == 2
    })
    .await;
    let before = record.buffers();
    wait_until("data from the new chain", DATA, || record.buffers() > before + 5).await;
    rig.expect_no_event(Duration::from_millis(300)).await;

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  fn post_error_of_chain(pipeline: &gstreamer::Pipeline, session: u64, stream_id: StreamId, generation: u64) {
    let element = gstreamer::ElementFactory::make("identity").build().unwrap();
    owner::mark(
      &element,
      Owner::Stream {
        session,
        stream_id,
        part: StreamPart::Chain(generation),
      },
    );
    pipeline.add(&element).unwrap();
    let error = gstreamer::message::Error::builder(gstreamer::StreamError::Failed, "not linked")
      .src(&element)
      .build();
    element.post_message(error).unwrap();
    element.set_state(gstreamer::State::Null).unwrap();
    pipeline.remove(&element).unwrap();
  }

  /// A chain that was replaced posts its last error, e.g. a queue whose consumer was taken away.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn the_errors_of_a_replaced_chain_are_not_the_streams() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let pipeline = rig.media.pipeline();
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 5).await;
    let generations = chain_generations(&pipeline, added.stream_id);
    assert_eq!(generations.len(), 1, "{generations:?}");
    let first_generation = generations[0];

    sender.set_pt(112);
    wait_until("the switch", DATA, || {
      let generations = chain_generations(&pipeline, added.stream_id);
      generations.len() == 1 && generations[0] != first_generation && record.count("stream-start") == 2
    })
    .await;
    let current_generation = chain_generations(&pipeline, added.stream_id)[0];

    post_error_of_chain(&pipeline, session.generation(), added.stream_id, first_generation);
    rig.expect_no_event(Duration::from_millis(500)).await;
    let before = record.buffers();
    wait_until("data from the new chain", DATA, || record.buffers() > before + 5).await;

    // The errors of the chain the stream is on are its own.
    post_error_of_chain(&pipeline, session.generation(), added.stream_id, current_generation);
    let removed = next_event_within(&mut rig, Duration::from_secs(8))
      .await
      .expect("the error of the stream's own chain was ignored");
    assert_eq!(removed.kind, StreamEventKind::Removed);
    assert!(removed.error.is_some(), "{removed:?}");

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  /// The consumer stalls, the old chain cannot finish, and the swap is forced after two seconds.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_forced_swap_does_not_cost_the_stream_its_new_chain() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 5).await;

    let mut stall = Stall::new(&rig.factory.element(added.stream_id)).await;
    sender.set_pt(112);
    wait_until("the switch to start", DATA, || {
      chain_generations(&rig.media.pipeline(), added.stream_id).len() == 2
    })
    .await;
    // The old chain cannot be removed while the consumer holds it, so what happens in between is not visible;
    // the stall ends after the forced swap.
    tokio::time::sleep(Duration::from_millis(2600)).await;
    stall.release();
    wait_until("the new chain's stream start", DATA, || {
      record.count("stream-start") == 2
    })
    .await;

    let before = record.buffers();
    wait_until("data from the new chain", DATA, || record.buffers() > before + 5).await;
    assert_eq!(chain_generations(&rig.media.pipeline(), added.stream_id).len(), 1);
    rig.expect_no_event(Duration::from_millis(500)).await;
    rig.assert_pipeline_running();

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }
}

mod ending_a_stream {
  use super::*;

  /// Some other part of the application has taken the consumer's pad, so the new chain of a switch cannot be
  /// linked to it, and the old chain has been cut off by then.
  fn take_the_pad(consumer: &gstreamer::Element) -> gstreamer::Pad {
    let sink = consumer.static_pad("sink").unwrap();
    if let Some(peer) = sink.peer() {
      peer.unlink(&sink).unwrap();
    }
    let thief = gstreamer::Pad::builder(gstreamer::PadDirection::Src)
      .name("thief")
      .build();
    thief.link(&sink).unwrap();
    thief
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_broken_switch_still_ends_the_consumer() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    let consumer = rig.factory.element(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 5).await;

    let mut stall = Stall::new(&consumer).await;
    sender.set_pt(112);
    wait_until("the switch to start", DATA, || {
      chain_generations(&rig.media.pipeline(), added.stream_id).len() == 2
    })
    .await;
    let _thief = take_the_pad(&consumer);
    stall.release();

    let removed = next_event_within(&mut rig, Duration::from_secs(10))
      .await
      .expect("the stream was not ended");
    assert_eq!(
      (removed.kind, removed.stream_id),
      (StreamEventKind::Removed, added.stream_id)
    );
    assert!(removed.error.is_some(), "{removed:?}");
    assert!(
      record.has_eos(),
      "the consumer never saw the end of its stream: {:?}",
      record.events()
    );
    rig.assert_pipeline_running();

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_drain_does_not_report_a_consumer_that_missed_its_eos_as_drained() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    let consumer = rig.factory.element(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 5).await;

    consumer
      .static_pad("sink")
      .unwrap()
      .add_probe(gstreamer::PadProbeType::EVENT_DOWNSTREAM, |_, info| {
        match info.event().map(|event| event.type_()) {
          Some(gstreamer::EventType::Eos) => gstreamer::PadProbeReturn::Drop,
          _ => gstreamer::PadProbeReturn::Ok,
        }
      });
    let mut stall = Stall::new(&consumer).await;
    sender.set_pt(112);
    wait_until("the switch to start", DATA, || {
      chain_generations(&rig.media.pipeline(), added.stream_id).len() == 2
    })
    .await;
    let _thief = take_the_pad(&consumer);
    // The drain comes while the stream is switching; the switch ends when the stall does.
    let release = tokio::spawn(async move {
      tokio::time::sleep(Duration::from_millis(300)).await;
      stall.release();
    });
    let result = session.drain(Duration::from_secs(10)).await;
    release.await.unwrap();
    assert!(result.is_err(), "{result:?}");
    assert!(!record.has_eos());

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }
}

mod freeing_what_rtpbin_keeps {
  use super::*;

  /// The source is removed while the stream is still being made, and making it fails.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn clear_ssrc_is_honored_when_the_activation_fails() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let sender = Sender::opus(&rig, 1000, 111);
    let session = rig.start(vec![]).await;
    wait_until("rtpbin's pad", DATA, || session_has_pad(&session, "stream_1000_111")).await;
    rig.factory.set(|_| {
      std::thread::sleep(Duration::from_millis(600));
      let queue = gstreamer::ElementFactory::make("queue").build().unwrap();
      let sink = gstreamer::ElementFactory::make("filesink")
        .property("location", "/nonexistent-directory-for-gst-meet-tests/file")
        .build()
        .unwrap();
      Some(consumer_bin(&[queue, sink]))
    });

    session
      .sources_added(vec![source(1000, "alice", MediaType::Audio)])
      .await;
    wait_until("the activation to start", DATA, || {
      !rig.factory.calls.lock().unwrap().is_empty()
    })
    .await;
    session.sources_removed(vec![1000]).await;

    let removed = rig.expect(StreamEventKind::Removed).await;
    assert!(removed.error.is_some(), "{removed:?}");
    let rtpbin = session.bin().by_name("rtpbin").unwrap();
    wait_until("rtpbin to free the pad", DATA, || {
      !rtpbin.pads().iter().any(|pad| pad.name() == "recv_rtp_src_0_1000_111")
        && !session_has_pad(&session, "stream_1000_111")
        && session
          .bin()
          .children()
          .iter()
          .all(|child| !child.name().starts_with("barrier_1000"))
    })
    .await;

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }
}

mod the_application_hears_everything_before_the_end {
  use super::*;

  fn slow_removed_hook(rig: &Rig) -> Arc<AtomicBool> {
    let handled = Arc::new(AtomicBool::new(false));
    rig.set_event_hook({
      let handled = handled.clone();
      move |event| {
        if event.kind == StreamEventKind::Removed {
          std::thread::sleep(Duration::from_millis(500));
          handled.store(true, Ordering::SeqCst);
        }
      }
    });
    handled
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn stopping_a_session_returns_after_the_removed_events_were_handled() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let handled = slow_removed_hook(&rig);
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 5).await;

    rig.media.stop_rtp_session(session, DRAIN).await;
    assert!(
      handled.load(Ordering::SeqCst),
      "the end of the session overtook the Removed event"
    );
    sender.stop();
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_drain_returns_after_the_removed_events_were_handled() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let handled = slow_removed_hook(&rig);
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 5).await;

    session.drain(DRAIN).await.unwrap();
    assert!(handled.load(Ordering::SeqCst), "the drain overtook the Removed event");

    sender.stop();
    rig.set_event_hook(|_| {});
    rig.media.stop_rtp_session(session, DRAIN).await;
  }

  /// The hook waits for the controller to be done, as an application's callback might through a blocking call.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn stopping_from_the_hook_does_not_wait_for_the_hook() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = Arc::new(rig.start(vec![source(1000, "alice", MediaType::Audio)]).await);
    let stopped = Arc::new(AtomicBool::new(false));
    rig.set_event_hook({
      let (session, stopped) = (session.clone(), stopped.clone());
      let runtime = tokio::runtime::Handle::current();
      move |event| {
        if event.kind == StreamEventKind::Added {
          runtime.block_on(session.retire_streams(DRAIN));
          stopped.store(true, Ordering::SeqCst);
        }
      }
    });
    let sender = Sender::opus(&rig, 1000, 111);

    rig.expect(StreamEventKind::Added).await;
    wait_until("the controller to stop", DATA, || stopped.load(Ordering::SeqCst)).await;
    rig.expect(StreamEventKind::Removed).await;

    sender.stop();
    rig.set_event_hook(|_| {});
    let session = Arc::into_inner(session).expect("the hook should not hold on to the session");
    rig.media.stop_rtp_session(session, DRAIN).await;
  }
}

mod a_stalled_consumer_does_not_hold_up_the_end_of_its_stream {
  use super::*;

  /// The chain's streaming thread is stuck in a push into the consumer, which never returns by itself.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_source_removed_while_its_consumer_stalls() {
    let mut rig = Rig::new(ReceiveMode::Encoded);
    let session = rig.start(vec![source(1000, "alice", MediaType::Audio)]).await;
    let sender = Sender::opus(&rig, 1000, 111);
    let added = rig.expect(StreamEventKind::Added).await;
    let record = rig.factory.record(added.stream_id);
    wait_until("data", DATA, || record.buffers() > 5).await;
    let _stall = Stall::new(&rig.factory.element(added.stream_id)).await;

    session.sources_removed(vec![1000]).await;
    let removed = next_event_within(&mut rig, DRAIN + Duration::from_secs(5))
      .await
      .expect("the stream should end although its consumer never takes its data");
    assert_eq!(removed.kind, StreamEventKind::Removed);
    assert_eq!(removed.stream_id, added.stream_id);
    assert!(chain_generations(&rig.media.pipeline(), added.stream_id).is_empty());

    sender.stop();
    rig.media.stop_rtp_session(session, DRAIN).await;
  }
}
