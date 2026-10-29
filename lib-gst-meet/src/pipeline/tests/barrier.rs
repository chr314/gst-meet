use std::sync::{
  atomic::{AtomicUsize, Ordering},
  Arc, Mutex,
};

use gstreamer::prelude::*;

use super::support::init;
use crate::pipeline::elements::StreamBarrier;

/// A raw pad pushing into a barrier whose output goes to another raw pad that the test controls.
struct Rig {
  src: gstreamer::Pad,
  barrier: StreamBarrier,
  sink: gstreamer::Pad,
  sink_flow: Arc<Mutex<Result<gstreamer::FlowSuccess, gstreamer::FlowError>>>,
  delivered: Arc<AtomicUsize>,
  events: Arc<Mutex<Vec<String>>>,
  upstream_events: Arc<Mutex<Vec<String>>>,
  idle: Arc<AtomicUsize>,
  failures: Arc<Mutex<Vec<gstreamer::FlowError>>>,
}

impl Rig {
  fn new() -> Self {
    init();
    let upstream_events = Arc::new(Mutex::new(Vec::new()));
    let src = {
      let upstream_events = upstream_events.clone();
      gstreamer::Pad::builder(gstreamer::PadDirection::Src)
        .name("test_src")
        .event_function(move |_, _, event| {
          upstream_events.lock().unwrap().push(format!("{:?}", event.type_()));
          true
        })
        .build()
    };

    let sink_flow = Arc::new(Mutex::new(Ok(gstreamer::FlowSuccess::Ok)));
    let delivered = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = {
      let (sink_flow, delivered, events) = (sink_flow.clone(), delivered.clone(), events.clone());
      gstreamer::Pad::builder(gstreamer::PadDirection::Sink)
        .name("test_sink")
        .chain_function(move |_, _, _| {
          delivered.fetch_add(1, Ordering::SeqCst);
          *sink_flow.lock().unwrap()
        })
        .event_function(move |_, _, event| {
          events.lock().unwrap().push(format!("{:?}", event.type_()));
          true
        })
        .query_function(|_, _, query| {
          // Refuses everything it is asked about.
          if let gstreamer::QueryViewMut::AcceptCaps(accept) = query.view_mut() {
            accept.set_result(false);
          }
          false
        })
        .build()
    };

    let idle = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(Mutex::new(Vec::new()));
    let barrier = StreamBarrier::new("barrier");
    {
      let (idle, failures) = (idle.clone(), failures.clone());
      barrier.set_callbacks(
        move || {
          idle.fetch_add(1, Ordering::SeqCst);
        },
        move |flow| failures.lock().unwrap().push(flow),
      );
    }
    src.link(&barrier.static_pad("sink").unwrap()).unwrap();
    barrier.set_state(gstreamer::State::Playing).unwrap();
    src.set_active(true).unwrap();
    sink.set_active(true).unwrap();

    let rig = Self {
      src,
      barrier,
      sink,
      sink_flow,
      delivered,
      events,
      upstream_events,
      idle,
      failures,
    };
    assert!(rig.src.push_event(gstreamer::event::StreamStart::new("test")));
    assert!(rig.src.push_event(gstreamer::event::Caps::new(
      &gstreamer::Caps::builder("application/x-test").build()
    )));
    assert!(rig
      .src
      .push_event(gstreamer::event::Segment::new(&gstreamer::FormattedSegment::<
        gstreamer::ClockTime,
      >::new())));
    rig
  }

  fn barrier_src(&self) -> gstreamer::Pad {
    self.barrier.static_pad("src").unwrap()
  }

  fn link(&self) {
    self.barrier_src().link(&self.sink).unwrap();
  }

  fn unlink(&self) {
    self.barrier_src().unlink(&self.sink).unwrap();
  }

  fn push(&self) -> Result<gstreamer::FlowSuccess, gstreamer::FlowError> {
    self.src.push(gstreamer::Buffer::new())
  }

  fn failures(&self) -> Vec<gstreamer::FlowError> {
    self.failures.lock().unwrap().clone()
  }
}

impl Drop for Rig {
  fn drop(&mut self) {
    let _ = self.barrier.set_state(gstreamer::State::Null);
  }
}

#[test]
fn data_without_a_downstream_is_dropped_and_reported_once_per_period() {
  let rig = Rig::new();
  for _ in 0..3 {
    assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  }
  assert_eq!(rig.idle.load(Ordering::SeqCst), 1);
  assert!(rig.barrier.last_data().is_some());

  rig.link();
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  assert_eq!(rig.delivered.load(Ordering::SeqCst), 1);
  assert_eq!(rig.idle.load(Ordering::SeqCst), 1);

  rig.unlink();
  for _ in 0..2 {
    assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  }
  assert_eq!(rig.idle.load(Ordering::SeqCst), 2);
  assert_eq!(rig.delivered.load(Ordering::SeqCst), 1);
  assert!(rig.failures().is_empty());
}

#[test]
fn downstream_failures_are_not_given_back_and_reported_once_until_relinked() {
  let rig = Rig::new();
  rig.link();
  *rig.sink_flow.lock().unwrap() = Err(gstreamer::FlowError::NotNegotiated);
  for _ in 0..3 {
    assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  }
  assert_eq!(rig.delivered.load(Ordering::SeqCst), 3);
  assert_eq!(rig.failures(), [gstreamer::FlowError::NotNegotiated]);

  // Every kind of failure is absorbed, not only the one that was reported.
  for flow in [
    gstreamer::FlowError::NotLinked,
    gstreamer::FlowError::Error,
    gstreamer::FlowError::Eos,
    gstreamer::FlowError::Flushing,
  ] {
    *rig.sink_flow.lock().unwrap() = Err(flow);
    assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok), "{flow:?}");
  }
  assert_eq!(rig.failures().len(), 1);

  // A relink starts a new period.
  rig.unlink();
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  rig.link();
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  assert_eq!(
    rig.failures(),
    [gstreamer::FlowError::NotNegotiated, gstreamer::FlowError::Flushing]
  );
}

#[test]
fn a_flushing_barrier_says_so() {
  let rig = Rig::new();
  rig.link();
  rig.barrier_src().set_active(false).unwrap();
  assert_eq!(rig.push(), Err(gstreamer::FlowError::Flushing));
  assert!(rig.failures().is_empty());
}

#[test]
fn any_caps_are_accepted_and_the_filter_is_the_answer_to_a_caps_query() {
  let rig = Rig::new();
  rig.link();
  let barrier_sink = rig.barrier.static_pad("sink").unwrap();
  let refused = gstreamer::Caps::builder("application/x-rtp").build();
  // The consumer refuses them, but the barrier accepts.
  assert!(barrier_sink.query_accept_caps(&refused));
  let filter = gstreamer::Caps::builder("application/x-rtp")
    .field("media", "audio")
    .build();
  assert_eq!(barrier_sink.query_caps(Some(&filter)), filter);
  assert!(barrier_sink.query_caps(None).is_any());
}

#[test]
fn sticky_events_reach_a_consumer_linked_later() {
  let rig = Rig::new();
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  rig.link();
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  let events = rig.events.lock().unwrap().clone();
  assert_eq!(events, ["StreamStart", "Caps", "Segment"]);
  assert_eq!(rig.delivered.load(Ordering::SeqCst), 1);
}

#[test]
fn upstream_events_pass_through() {
  let rig = Rig::new();
  rig.link();
  let force_key_unit = crate::pipeline::chain::force_key_unit_event();
  assert!(rig.sink.push_event(force_key_unit));
  assert!(rig
    .upstream_events
    .lock()
    .unwrap()
    .contains(&"CustomUpstream".to_owned()));
}

/// The way the receive side uses it: the barrier's output leaves a bin through a ghost pad whose own peer is what
/// counts.
#[test]
fn a_ghost_pad_that_is_not_linked_counts_as_unlinked() {
  init();
  let barrier = StreamBarrier::new("ghosted");
  let idle = Arc::new(AtomicUsize::new(0));
  {
    let idle = idle.clone();
    barrier.set_callbacks(
      move || {
        idle.fetch_add(1, Ordering::SeqCst);
      },
      |_| {},
    );
  }
  let bin = gstreamer::Bin::new();
  bin.add(&barrier).unwrap();
  let ghost = gstreamer::GhostPad::builder_with_target(&barrier.static_pad("src").unwrap())
    .unwrap()
    .name("out")
    .build();
  ghost.set_active(true).unwrap();
  bin.add_pad(&ghost).unwrap();

  let src = gstreamer::Pad::builder(gstreamer::PadDirection::Src)
    .name("test_src")
    .build();
  src.link(&barrier.static_pad("sink").unwrap()).unwrap();
  let delivered = Arc::new(AtomicUsize::new(0));
  let sink = {
    let delivered = delivered.clone();
    gstreamer::Pad::builder(gstreamer::PadDirection::Sink)
      .name("test_sink")
      .chain_function(move |_, _, _| {
        delivered.fetch_add(1, Ordering::SeqCst);
        Ok(gstreamer::FlowSuccess::Ok)
      })
      .event_function(|_, _, _| true)
      .build()
  };
  bin.set_state(gstreamer::State::Playing).unwrap();
  src.set_active(true).unwrap();
  sink.set_active(true).unwrap();
  assert!(src.push_event(gstreamer::event::StreamStart::new("test")));
  assert!(src.push_event(gstreamer::event::Caps::new(
    &gstreamer::Caps::builder("application/x-test").build()
  )));
  assert!(
    src.push_event(gstreamer::event::Segment::new(&gstreamer::FormattedSegment::<
      gstreamer::ClockTime,
    >::new()))
  );

  assert_eq!(src.push(gstreamer::Buffer::new()), Ok(gstreamer::FlowSuccess::Ok));
  assert_eq!(idle.load(Ordering::SeqCst), 1);
  assert_eq!(delivered.load(Ordering::SeqCst), 0);

  ghost.link(&sink).unwrap();
  assert_eq!(src.push(gstreamer::Buffer::new()), Ok(gstreamer::FlowSuccess::Ok));
  assert_eq!(delivered.load(Ordering::SeqCst), 1);
  bin.set_state(gstreamer::State::Null).unwrap();
}

#[test]
fn the_barrier_is_registered_as_an_element() {
  init();
  let element = gstreamer::ElementFactory::make("gstmeetbarrier").build().unwrap();
  assert!(element.static_pad("sink").is_some() && element.static_pad("src").is_some());
}

#[test]
fn a_relink_without_data_in_between_starts_a_new_period_for_failures() {
  let rig = Rig::new();
  rig.link();
  *rig.sink_flow.lock().unwrap() = Err(gstreamer::FlowError::NotNegotiated);
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  assert_eq!(rig.failures(), [gstreamer::FlowError::NotNegotiated]);

  // The failed chain is replaced while the sender is quiet, and the new one fails as well.
  rig.unlink();
  rig.link();
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  assert_eq!(
    rig.failures(),
    [gstreamer::FlowError::NotNegotiated, gstreamer::FlowError::NotNegotiated]
  );
  // Still once per period.
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  assert_eq!(rig.failures().len(), 2);
}

#[test]
fn a_relink_without_data_in_between_starts_a_new_period_for_idle_data() {
  let rig = Rig::new();
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  assert_eq!(rig.idle.load(Ordering::SeqCst), 1);

  // A stream is activated and retired again before a single buffer went through.
  rig.link();
  rig.unlink();
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  assert_eq!(rig.idle.load(Ordering::SeqCst), 2);
  assert_eq!(rig.push(), Ok(gstreamer::FlowSuccess::Ok));
  assert_eq!(rig.idle.load(Ordering::SeqCst), 2);
}

/// What the receive side has: the barrier's output leaves its bin through a ghost pad, and it is that pad which
/// gets linked and unlinked.
struct GhostRig {
  src: gstreamer::Pad,
  barrier: StreamBarrier,
  bin: gstreamer::Bin,
  ghost: gstreamer::GhostPad,
  sink: gstreamer::Pad,
  sink_flow: Arc<Mutex<Result<gstreamer::FlowSuccess, gstreamer::FlowError>>>,
  idle: Arc<AtomicUsize>,
  failures: Arc<AtomicUsize>,
}

impl GhostRig {
  fn new(track: bool) -> Self {
    init();
    let barrier = StreamBarrier::new("ghosted_periods");
    let idle = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    {
      let (idle, failures) = (idle.clone(), failures.clone());
      barrier.set_callbacks(
        move || {
          idle.fetch_add(1, Ordering::SeqCst);
        },
        move |_| {
          failures.fetch_add(1, Ordering::SeqCst);
        },
      );
    }
    let bin = gstreamer::Bin::new();
    bin.add(&barrier).unwrap();
    let ghost = gstreamer::GhostPad::builder_with_target(&barrier.static_pad("src").unwrap())
      .unwrap()
      .name("out")
      .build();
    ghost.set_active(true).unwrap();
    bin.add_pad(&ghost).unwrap();
    if track {
      barrier.track_downstream(&ghost);
    }

    let src = gstreamer::Pad::builder(gstreamer::PadDirection::Src)
      .name("test_src")
      .build();
    src.link(&barrier.static_pad("sink").unwrap()).unwrap();
    let sink_flow = Arc::new(Mutex::new(Ok(gstreamer::FlowSuccess::Ok)));
    let sink = {
      let sink_flow = sink_flow.clone();
      gstreamer::Pad::builder(gstreamer::PadDirection::Sink)
        .name("test_sink")
        .chain_function(move |_, _, _| *sink_flow.lock().unwrap())
        .event_function(|_, _, _| true)
        .build()
    };
    bin.set_state(gstreamer::State::Playing).unwrap();
    src.set_active(true).unwrap();
    sink.set_active(true).unwrap();
    assert!(src.push_event(gstreamer::event::StreamStart::new("test")));
    assert!(src.push_event(gstreamer::event::Caps::new(
      &gstreamer::Caps::builder("application/x-test").build()
    )));
    assert!(
      src.push_event(gstreamer::event::Segment::new(&gstreamer::FormattedSegment::<
        gstreamer::ClockTime,
      >::new()))
    );
    Self {
      src,
      barrier,
      bin,
      ghost,
      sink,
      sink_flow,
      idle,
      failures,
    }
  }

  fn push(&self) {
    assert_eq!(self.src.push(gstreamer::Buffer::new()), Ok(gstreamer::FlowSuccess::Ok));
  }
}

impl Drop for GhostRig {
  fn drop(&mut self) {
    let _ = self.bin.set_state(gstreamer::State::Null);
  }
}

#[test]
fn relinking_the_ghost_pad_starts_a_new_period() {
  let rig = GhostRig::new(true);
  rig.push();
  assert_eq!(rig.idle.load(Ordering::SeqCst), 1);

  rig.ghost.link(&rig.sink).unwrap();
  *rig.sink_flow.lock().unwrap() = Err(gstreamer::FlowError::NotNegotiated);
  rig.push();
  rig.push();
  assert_eq!(rig.failures.load(Ordering::SeqCst), 1);

  // The next chain is linked in the same quiet moment the old one is dropped.
  rig.ghost.unlink(&rig.sink).unwrap();
  rig.ghost.link(&rig.sink).unwrap();
  rig.push();
  assert_eq!(rig.failures.load(Ordering::SeqCst), 2);

  // Unlinked and linked again without data: the idle notification is due again.
  rig.ghost.unlink(&rig.sink).unwrap();
  rig.push();
  assert_eq!(rig.idle.load(Ordering::SeqCst), 2);
  rig.ghost.link(&rig.sink).unwrap();
  rig.ghost.unlink(&rig.sink).unwrap();
  rig.push();
  assert_eq!(rig.idle.load(Ordering::SeqCst), 3);
  let _ = &rig.barrier;
}
