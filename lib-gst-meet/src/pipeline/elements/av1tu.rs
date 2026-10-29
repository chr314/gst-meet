//! AV1 temporal unit aggregator.
//!
//! `rtpav1depay` outputs `alignment=obu` buffers, flagging the one that ends a temporal unit with MARKER. `av1parse`
//! only recognizes the end of a temporal unit when the next temporal delimiter shows up, which delays every frame
//! by one. This element collects the OBU buffers of a temporal unit and pushes them as one `alignment=tu` buffer
//! as soon as it is complete.

use std::sync::{Mutex, MutexGuard};

use anyhow::{Context, Result};
use glib::prelude::*;
use gstreamer as gst;

pub(super) const ELEMENT_NAME: &str = "gstmeetav1tu";

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
  mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// OBU buffers of the temporal unit being assembled. The first buffer provides the metadata.
struct Pending {
  buffer: gst::Buffer,
  pts: Option<gst::ClockTime>,
  all_delta: bool,
}

#[derive(Default)]
struct State {
  pending: Option<Pending>,
}

impl State {
  fn append(&mut self, buffer: gst::Buffer) {
    let delta = buffer.flags().contains(gst::BufferFlags::DELTA_UNIT);
    match &mut self.pending {
      Some(pending) => {
        pending.all_delta &= delta;
        // Joins the memories without copying.
        pending.buffer.append(buffer);
      },
      None => {
        self.pending = Some(Pending {
          pts: buffer.pts(),
          all_delta: delta,
          buffer,
        });
      },
    }
  }

  /// Takes the temporal unit. `complete` is whether a MARKER ended it, as opposed to it being cut short.
  fn finish(&mut self, complete: bool) -> Option<gst::Buffer> {
    let Pending {
      mut buffer, all_delta, ..
    } = self.pending.take()?;
    let flags = buffer.make_mut();
    if all_delta {
      flags.set_flags(gst::BufferFlags::DELTA_UNIT);
    } else {
      flags.unset_flags(gst::BufferFlags::DELTA_UNIT);
    }
    if complete {
      flags.set_flags(gst::BufferFlags::MARKER);
    }
    Some(buffer)
  }
}

fn with_alignment(caps: &gst::Caps, alignment: Option<&str>) -> gst::Caps {
  if caps.is_any() || caps.is_empty() {
    return caps.clone();
  }
  let mut caps = caps.clone();
  for structure in caps.make_mut().iter_mut() {
    match alignment {
      Some(alignment) => structure.set("alignment", alignment),
      None => structure.remove_field("alignment"),
    }
  }
  caps
}

/// What the element pushes for the sink caps, which may have any alignment.
fn to_src_caps(caps: &gst::Caps) -> gst::Caps {
  with_alignment(caps, Some("tu"))
}

fn to_sink_caps(caps: &gst::Caps) -> gst::Caps {
  with_alignment(caps, None)
}

glib::wrapper! {
  pub struct Av1TuAggregator(ObjectSubclass<imp::Av1TuAggregator>) @extends gst::Element, gst::Object;
}

pub(crate) fn make_av1_tu_aggregator() -> Result<gst::Element> {
  super::register()?;
  gst::ElementFactory::make(ELEMENT_NAME)
    .build()
    .with_context(|| format!("failed to create {}", ELEMENT_NAME))
}

pub(super) fn register() -> Result<(), glib::BoolError> {
  gst::Element::register(
    None,
    ELEMENT_NAME,
    gst::Rank::NONE,
    <Av1TuAggregator as StaticType>::static_type(),
  )
}

mod imp {
  use std::sync::{LazyLock, Mutex};

  use gstreamer as gst;
  use gstreamer::{prelude::*, subclass::prelude::*};

  use super::{lock, to_sink_caps, to_src_caps, State, ELEMENT_NAME};

  static CAT: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
    gst::DebugCategory::new(
      ELEMENT_NAME,
      gst::DebugColorFlags::empty(),
      Some("AV1 temporal unit aggregator"),
    )
  });

  pub struct Av1TuAggregator {
    sinkpad: gst::Pad,
    srcpad: gst::Pad,
    state: Mutex<State>,
  }

  #[glib::object_subclass]
  impl ObjectSubclass for Av1TuAggregator {
    const NAME: &'static str = "GstMeetAv1TuAggregator";
    type Type = super::Av1TuAggregator;
    type ParentType = gst::Element;

    fn with_class(klass: &Self::Class) -> Self {
      let sink_template = klass.pad_template("sink").expect("sink pad template is declared");
      let sinkpad = gst::Pad::builder_from_template(&sink_template)
        .chain_function(|pad, parent, buffer| {
          Av1TuAggregator::catch_panic_pad_function(
            parent,
            || Err(gst::FlowError::Error),
            |this| this.sink_chain(pad, buffer),
          )
        })
        .event_function(|pad, parent, event| {
          Av1TuAggregator::catch_panic_pad_function(parent, || false, |this| this.sink_event(pad, event))
        })
        .query_function(|pad, parent, query| {
          Av1TuAggregator::catch_panic_pad_function(parent, || false, |this| this.sink_query(pad, query))
        })
        .build();
      let src_template = klass.pad_template("src").expect("src pad template is declared");
      let srcpad = gst::Pad::builder_from_template(&src_template)
        .query_function(|pad, parent, query| {
          Av1TuAggregator::catch_panic_pad_function(parent, || false, |this| this.src_query(pad, query))
        })
        .build();
      Self {
        sinkpad,
        srcpad,
        state: Mutex::new(State::default()),
      }
    }
  }

  impl ObjectImpl for Av1TuAggregator {
    fn constructed(&self) {
      self.parent_constructed();
      let obj = self.obj();
      obj.add_pad(&self.sinkpad).expect("adding the sink pad");
      obj.add_pad(&self.srcpad).expect("adding the src pad");
    }
  }

  impl GstObjectImpl for Av1TuAggregator {}

  impl ElementImpl for Av1TuAggregator {
    fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
      static METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
        gst::subclass::ElementMetadata::new(
          "AV1 temporal unit aggregator",
          "Codec/Parser/Video",
          "Joins AV1 OBU buffers into temporal units, using the RTP marker flag to find their end",
          "gst-meet",
        )
      });
      Some(&*METADATA)
    }

    fn pad_templates() -> &'static [gst::PadTemplate] {
      static PAD_TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
        let sink_caps = gst::Caps::builder("video/x-av1")
          .field("stream-format", "obu-stream")
          .build();
        let src_caps = gst::Caps::builder("video/x-av1")
          .field("stream-format", "obu-stream")
          .field("alignment", "tu")
          .build();
        vec![
          gst::PadTemplate::new("sink", gst::PadDirection::Sink, gst::PadPresence::Always, &sink_caps)
            .expect("sink pad template"),
          gst::PadTemplate::new("src", gst::PadDirection::Src, gst::PadPresence::Always, &src_caps)
            .expect("src pad template"),
        ]
      });
      PAD_TEMPLATES.as_ref()
    }

    fn change_state(&self, transition: gst::StateChange) -> Result<gst::StateChangeSuccess, gst::StateChangeError> {
      if transition == gst::StateChange::ReadyToPaused {
        lock(&self.state).pending = None;
      }
      let success = self.parent_change_state(transition)?;
      if transition == gst::StateChange::PausedToReady {
        lock(&self.state).pending = None;
      }
      Ok(success)
    }
  }

  impl Av1TuAggregator {
    /// Pushes the temporal unit being assembled, complete or not.
    fn drain(&self) -> Result<gst::FlowSuccess, gst::FlowError> {
      let unit = lock(&self.state).finish(false);
      match unit {
        Some(unit) => self.srcpad.push(unit),
        None => Ok(gst::FlowSuccess::Ok),
      }
    }

    fn sink_chain(&self, _pad: &gst::Pad, buffer: gst::Buffer) -> Result<gst::FlowSuccess, gst::FlowError> {
      let flags = buffer.flags();
      let marker = flags.contains(gst::BufferFlags::MARKER);
      let discont = flags.contains(gst::BufferFlags::DISCONT);
      let pts = buffer.pts();

      let (cut_short, complete) = {
        let mut state = lock(&self.state);
        // A new timestamp or a discontinuity means the pending OBUs are all there will be of their temporal unit.
        let cut_short = if state
          .pending
          .as_ref()
          .is_some_and(|pending| discont || pending.pts != pts)
        {
          state.finish(false)
        } else {
          None
        };
        state.append(buffer);
        (cut_short, if marker { state.finish(true) } else { None })
      };

      if let Some(unit) = cut_short {
        gst::debug!(CAT, imp = self, "pushing a temporal unit that was cut short");
        self.srcpad.push(unit)?;
      }
      match complete {
        Some(unit) => self.srcpad.push(unit),
        None => Ok(gst::FlowSuccess::Ok),
      }
    }

    fn sink_event(&self, pad: &gst::Pad, event: gst::Event) -> bool {
      use gst::EventView;

      match event.view() {
        EventView::Caps(caps_event) => {
          // A temporal unit never spans a caps change.
          let _ = self.drain();
          let caps = to_src_caps(&caps_event.caps_owned());
          gst::debug!(CAT, imp = self, "src caps {}", caps);
          self
            .srcpad
            .push_event(gst::event::Caps::builder(&caps).seqnum(event.seqnum()).build())
        },
        EventView::FlushStop(_) => {
          lock(&self.state).pending = None;
          gst::Pad::event_default(pad, Some(&*self.obj()), event)
        },
        // Eos, Segment, Gap and the like must come after the data they follow.
        _ if event.is_serialized() => {
          let _ = self.drain();
          gst::Pad::event_default(pad, Some(&*self.obj()), event)
        },
        _ => gst::Pad::event_default(pad, Some(&*self.obj()), event),
      }
    }

    fn sink_query(&self, pad: &gst::Pad, query: &mut gst::QueryRef) -> bool {
      match query.view_mut() {
        gst::QueryViewMut::Caps(q) => {
          let filter = q.filter_owned();
          let peer_caps = self.srcpad.peer_query_caps(filter.as_ref().map(to_src_caps).as_ref());
          let mut caps = to_sink_caps(&peer_caps).intersect(&pad.pad_template_caps());
          if let Some(filter) = filter {
            caps = filter.intersect_with_mode(&caps, gst::CapsIntersectMode::First);
          }
          q.set_result(&caps);
          true
        },
        gst::QueryViewMut::AcceptCaps(q) => {
          let caps = q.caps_owned();
          q.set_result(
            caps.can_intersect(&pad.pad_template_caps()) && self.srcpad.peer_query_accept_caps(&to_src_caps(&caps)),
          );
          true
        },
        gst::QueryViewMut::Drain(_) => {
          let _ = self.drain();
          gst::Pad::query_default(pad, Some(&*self.obj()), query)
        },
        // The default only forwards it on pads flagged as proxying allocations.
        gst::QueryViewMut::Allocation(_) => self.srcpad.peer_query(query),
        _ => gst::Pad::query_default(pad, Some(&*self.obj()), query),
      }
    }

    fn src_query(&self, pad: &gst::Pad, query: &mut gst::QueryRef) -> bool {
      match query.view_mut() {
        gst::QueryViewMut::Caps(q) => {
          let filter = q.filter_owned();
          let peer_caps = self.sinkpad.peer_query_caps(filter.as_ref().map(to_sink_caps).as_ref());
          let mut caps = to_src_caps(&peer_caps).intersect(&pad.pad_template_caps());
          if let Some(filter) = filter {
            caps = filter.intersect_with_mode(&caps, gst::CapsIntersectMode::First);
          }
          q.set_result(&caps);
          true
        },
        gst::QueryViewMut::AcceptCaps(q) => {
          let caps = q.caps_owned();
          q.set_result(
            caps.can_intersect(&pad.pad_template_caps()) && self.sinkpad.peer_query_accept_caps(&to_sink_caps(&caps)),
          );
          true
        },
        _ => gst::Pad::query_default(pad, Some(&*self.obj()), query),
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use gstreamer::prelude::*;

  use super::*;
  use crate::pipeline::elements::test_util::{bytes, Harness};

  fn obu_caps() -> gst::Caps {
    gst::init().unwrap();
    gst::Caps::builder("video/x-av1")
      .field("stream-format", "obu-stream")
      .field("alignment", "obu")
      .field("parsed", true)
      .build()
  }

  fn started() -> Harness {
    let harness = Harness::new(vec![make_av1_tu_aggregator().unwrap()], None);
    harness.start(&obu_caps());
    harness
  }

  fn obus(data: &[u8], pts: Option<u64>, flags: gst::BufferFlags) -> gst::Buffer {
    let mut buffer = gst::Buffer::from_slice(data.to_vec());
    let buffer_ref = buffer.get_mut().unwrap();
    buffer_ref.set_pts(pts.map(gst::ClockTime::from_nseconds));
    buffer_ref.set_dts(pts.map(|pts| gst::ClockTime::from_nseconds(pts + 1000)));
    buffer_ref.set_flags(flags);
    buffer
  }

  const MARKER: gst::BufferFlags = gst::BufferFlags::MARKER;
  const DISCONT: gst::BufferFlags = gst::BufferFlags::DISCONT;
  const DELTA: gst::BufferFlags = gst::BufferFlags::DELTA_UNIT;

  #[test]
  fn pushes_a_temporal_unit_when_the_marker_arrives() {
    let harness = started();

    harness.push(obus(b"td", Some(10), DISCONT));
    harness.push(obus(b"seq", Some(10), gst::BufferFlags::empty()));
    assert!(
      harness.buffers().is_empty(),
      "nothing is pushed before the unit is complete"
    );
    harness.push(obus(b"frame", Some(10), MARKER));

    let buffers = harness.buffers();
    assert_eq!(buffers.len(), 1);
    assert_eq!(bytes(&buffers[0]), b"tdseqframe");
    assert_eq!(buffers[0].pts(), Some(gst::ClockTime::from_nseconds(10)));
    assert_eq!(buffers[0].dts(), Some(gst::ClockTime::from_nseconds(1010)));
    assert!(buffers[0].flags().contains(DISCONT));
    assert!(buffers[0].flags().contains(MARKER));
  }

  #[test]
  fn consecutive_units_are_pushed_separately() {
    let harness = started();

    harness.push(obus(b"a1", Some(1), gst::BufferFlags::empty()));
    harness.push(obus(b"a2", Some(1), MARKER));
    harness.push(obus(b"b", Some(2), MARKER));
    harness.push(obus(b"c1", Some(3), gst::BufferFlags::empty()));
    harness.push(obus(b"c2", Some(3), MARKER));

    let buffers = harness.buffers();
    assert_eq!(
      buffers.iter().map(bytes).collect::<Vec<_>>(),
      [b"a1a2".to_vec(), b"b".to_vec(), b"c1c2".to_vec()]
    );
    assert!(buffers.iter().all(|b| !b.flags().contains(DISCONT)));
    assert_eq!(
      buffers.iter().map(|b| b.pts().unwrap().nseconds()).collect::<Vec<_>>(),
      [1, 2, 3]
    );
  }

  #[test]
  fn a_new_timestamp_cuts_the_pending_unit_short() {
    let harness = started();

    harness.push(obus(b"lost-marker", Some(1), gst::BufferFlags::empty()));
    harness.push(obus(b"next", Some(2), MARKER));

    let buffers = harness.buffers();
    assert_eq!(
      buffers.iter().map(bytes).collect::<Vec<_>>(),
      [b"lost-marker".to_vec(), b"next".to_vec()]
    );
    assert!(
      !buffers[0].flags().contains(MARKER),
      "an incomplete unit does not claim to be complete"
    );
    assert!(buffers[1].flags().contains(MARKER));
  }

  #[test]
  fn a_discontinuity_cuts_the_pending_unit_short_and_stays_on_the_next() {
    let harness = started();

    harness.push(obus(b"head", Some(1), gst::BufferFlags::empty()));
    harness.push(obus(b"tail", Some(1), DISCONT | MARKER));

    let buffers = harness.buffers();
    assert_eq!(
      buffers.iter().map(bytes).collect::<Vec<_>>(),
      [b"head".to_vec(), b"tail".to_vec()]
    );
    assert!(!buffers[0].flags().contains(DISCONT));
    assert!(buffers[1].flags().contains(DISCONT));
  }

  #[test]
  fn eos_pushes_the_pending_unit_first() {
    let harness = started();

    harness.push(obus(b"unfinished", Some(1), gst::BufferFlags::empty()));
    harness.eos();

    let items = harness.items();
    let eos = items.iter().position(|item| item == "Eos").expect("EOS is forwarded");
    let buffer = items
      .iter()
      .position(|item| item.starts_with("buffer"))
      .expect("the unit is pushed");
    assert!(buffer < eos, "{items:?}");
    assert_eq!(bytes(&harness.buffers()[0]), b"unfinished");
  }

  #[test]
  fn a_new_segment_pushes_the_pending_unit_first() {
    let harness = started();

    harness.push(obus(b"old", Some(1), gst::BufferFlags::empty()));
    assert!(harness
      .pad()
      .push_event(gst::event::Segment::new(&gst::FormattedSegment::<gst::ClockTime>::new())));

    let items = harness.items();
    let segments: Vec<_> = items
      .iter()
      .enumerate()
      .filter(|(_, item)| *item == "Segment")
      .collect();
    let buffer = items.iter().position(|item| item.starts_with("buffer")).unwrap();
    assert_eq!(segments.len(), 2, "{items:?}");
    assert!(segments[0].0 < buffer && buffer < segments[1].0, "{items:?}");
  }

  #[test]
  fn flushing_drops_the_pending_unit() {
    let harness = started();

    harness.push(obus(b"stale", Some(1), gst::BufferFlags::empty()));
    assert!(harness.pad().push_event(gst::event::FlushStart::new()));
    assert!(harness.pad().push_event(gst::event::FlushStop::new(true)));
    assert!(harness
      .pad()
      .push_event(gst::event::Segment::new(&gst::FormattedSegment::<gst::ClockTime>::new())));
    harness.push(obus(b"fresh", Some(1), MARKER));

    let buffers = harness.buffers();
    assert_eq!(buffers.iter().map(bytes).collect::<Vec<_>>(), [b"fresh".to_vec()]);
  }

  #[test]
  fn delta_unit_is_only_kept_when_every_part_has_it() {
    let harness = started();

    harness.push(obus(b"a", Some(1), DELTA));
    harness.push(obus(b"b", Some(1), DELTA | MARKER));
    harness.push(obus(b"c", Some(2), DELTA));
    harness.push(obus(b"d", Some(2), MARKER));
    harness.push(obus(b"e", Some(3), gst::BufferFlags::empty()));
    harness.push(obus(b"f", Some(3), DELTA | MARKER));
    harness.push(obus(b"g", Some(4), DELTA | MARKER));

    let delta: Vec<_> = harness.buffers().iter().map(|b| b.flags().contains(DELTA)).collect();
    assert_eq!(delta, [true, false, false, true]);
  }

  #[test]
  fn unit_without_timestamps_is_split_by_markers_only() {
    let harness = started();

    harness.push(obus(b"a", None, gst::BufferFlags::empty()));
    harness.push(obus(b"b", None, MARKER));
    harness.push(obus(b"c", None, MARKER));

    let buffers = harness.buffers();
    assert_eq!(
      buffers.iter().map(bytes).collect::<Vec<_>>(),
      [b"ab".to_vec(), b"c".to_vec()]
    );
    assert_eq!(buffers[0].pts(), None);
  }

  #[test]
  fn caps_are_rewritten_to_temporal_unit_alignment() {
    let harness = started();

    let caps = harness.events(gst::EventType::Caps);
    assert_eq!(caps.len(), 1);
    let gst::EventView::Caps(event) = caps[0].view() else {
      unreachable!()
    };
    let structure = event.caps().structure(0).unwrap();
    assert_eq!(structure.name(), "video/x-av1");
    assert_eq!(structure.get::<&str>("alignment").unwrap(), "tu");
    assert_eq!(structure.get::<&str>("stream-format").unwrap(), "obu-stream");
    assert!(structure.get::<bool>("parsed").unwrap(), "other fields are kept");
  }

  #[test]
  fn a_caps_change_pushes_the_pending_unit_first() {
    let harness = started();

    harness.push(obus(b"old", Some(1), gst::BufferFlags::empty()));
    let new_caps = gst::Caps::builder("video/x-av1")
      .field("stream-format", "obu-stream")
      .field("alignment", "obu")
      .field("profile", "main")
      .build();
    assert!(harness.pad().push_event(gst::event::Caps::new(&new_caps)));

    let items = harness.items();
    let caps: Vec<_> = items.iter().enumerate().filter(|(_, item)| *item == "Caps").collect();
    let buffer = items.iter().position(|item| item.starts_with("buffer")).unwrap();
    assert_eq!(caps.len(), 2, "{items:?}");
    assert!(caps[0].0 < buffer && buffer < caps[1].0, "{items:?}");
  }

  #[test]
  fn caps_queries_translate_the_alignment() {
    gst::init().unwrap();
    let downstream = gst::Caps::builder("video/x-av1")
      .field("stream-format", "obu-stream")
      .field("alignment", "tu")
      .build();
    let harness = Harness::new(vec![make_av1_tu_aggregator().unwrap()], Some(downstream));

    // Upstream asks what the element accepts: any alignment, as long as downstream takes the temporal unit one.
    let result = harness.pad().peer_query_caps(None);
    assert!(result.can_intersect(&obu_caps()), "{result}");
    assert!(result.can_intersect(
      &gst::Caps::builder("video/x-av1")
        .field("stream-format", "obu-stream")
        .field("alignment", "tu")
        .build()
    ));
    assert!(
      !result.can_intersect(
        &gst::Caps::builder("video/x-av1")
          .field("stream-format", "annexb")
          .build()
      ),
      "{result}"
    );
    assert!(harness.pad().peer_query_accept_caps(&obu_caps()));
    assert!(!harness
      .pad()
      .peer_query_accept_caps(&gst::Caps::builder("video/x-vp9").build()));
  }

  #[test]
  fn src_pad_caps_query_asks_upstream() {
    let element = make_av1_tu_aggregator().unwrap();
    let harness = Harness::new(vec![element.clone()], None);
    let src = element.static_pad("src").unwrap();

    // Nothing is linked to the sink pad's peer that restricts the caps beyond the harness pad (which accepts any).
    let caps = src.query_caps(None);
    assert_eq!(caps.structure(0).unwrap().get::<&str>("alignment").unwrap(), "tu");
    drop(harness);
  }

  #[test]
  fn upstream_events_and_allocation_queries_pass_through() {
    let harness = started();
    let sink_pad = harness.pad().peer().unwrap();
    let src_pad_of_element = sink_pad.parent_element().unwrap().static_pad("src").unwrap();
    let downstream_sink = src_pad_of_element.peer().unwrap();

    let fku = gst::Structure::builder("GstForceKeyUnit").build();
    assert!(downstream_sink.push_event(gst::event::CustomUpstream::new(fku)));
    let events = harness.upstream_events();
    assert!(events
      .iter()
      .any(|e| e.structure().is_some_and(|s| s.name() == "GstForceKeyUnit")));

    let mut query = gst::query::Allocation::new(Some(&obu_caps()), false);
    assert!(harness.pad().peer_query(&mut query));
    assert!(harness.queries().contains(&gst::QueryType::Allocation));
  }

  #[test]
  fn registers_once_and_creates_elements_through_the_factory() {
    crate::pipeline::elements::register().unwrap();
    crate::pipeline::elements::register().unwrap();
    let element = make_av1_tu_aggregator().unwrap();
    assert_eq!(element.factory().unwrap().name(), ELEMENT_NAME);
    assert!(element.static_pad("sink").is_some() && element.static_pad("src").is_some());
  }
}
