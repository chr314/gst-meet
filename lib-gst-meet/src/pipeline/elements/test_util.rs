use std::sync::{Arc, Mutex};

use gstreamer as gst;
use gstreamer::prelude::*;

pub(super) enum Item {
  Buffer(gst::Buffer),
  Event(gst::Event),
}

/// Pushes into the sink pad of the first element and collects what comes out of the src pad of the last one.
pub(super) struct Harness {
  elements: Vec<gst::Element>,
  src: gst::Pad,
  /// Kept alive: a dropped pad unlinks itself.
  _sink: gst::Pad,
  items: Arc<Mutex<Vec<Item>>>,
  upstream_events: Arc<Mutex<Vec<gst::Event>>>,
  queries: Arc<Mutex<Vec<gst::QueryType>>>,
}

impl Harness {
  /// `downstream_caps` is what the collecting pad answers to caps queries, anything if `None`.
  pub(super) fn new(elements: Vec<gst::Element>, downstream_caps: Option<gst::Caps>) -> Self {
    gst::init().unwrap();
    super::register().unwrap();

    let items = Arc::new(Mutex::new(Vec::new()));
    let upstream_events = Arc::new(Mutex::new(Vec::new()));
    let queries = Arc::new(Mutex::new(Vec::new()));

    let src = {
      let upstream_events = upstream_events.clone();
      gst::Pad::builder(gst::PadDirection::Src)
        .name("test_src")
        .event_function(move |_pad, _parent, event| {
          upstream_events.lock().unwrap().push(event);
          true
        })
        .build()
    };
    let sink = {
      let (chain_items, event_items, queries) = (items.clone(), items.clone(), queries.clone());
      gst::Pad::builder(gst::PadDirection::Sink)
        .name("test_sink")
        .chain_function(move |_pad, _parent, buffer| {
          chain_items.lock().unwrap().push(Item::Buffer(buffer));
          Ok(gst::FlowSuccess::Ok)
        })
        .event_function(move |_pad, _parent, event| {
          event_items.lock().unwrap().push(Item::Event(event));
          true
        })
        .query_function(move |_pad, _parent, query| {
          queries.lock().unwrap().push(query.type_());
          match query.view_mut() {
            gst::QueryViewMut::Caps(q) => {
              let caps = match (&downstream_caps, q.filter()) {
                (Some(caps), Some(filter)) => filter.intersect(caps),
                (Some(caps), None) => caps.clone(),
                (None, Some(filter)) => filter.to_owned(),
                (None, None) => gst::Caps::new_any(),
              };
              q.set_result(&caps);
              true
            },
            gst::QueryViewMut::AcceptCaps(q) => {
              q.set_result(downstream_caps.as_ref().is_none_or(|caps| q.caps().can_intersect(caps)));
              true
            },
            gst::QueryViewMut::Allocation(_) => true,
            _ => false,
          }
        })
        .build()
    };

    for pair in elements.windows(2) {
      pair[0]
        .static_pad("src")
        .unwrap()
        .link(&pair[1].static_pad("sink").unwrap())
        .unwrap();
    }
    src.link(&elements[0].static_pad("sink").unwrap()).unwrap();
    elements.last().unwrap().static_pad("src").unwrap().link(&sink).unwrap();
    src.set_active(true).unwrap();
    sink.set_active(true).unwrap();
    for element in &elements {
      element.set_state(gst::State::Playing).unwrap();
    }

    Harness {
      elements,
      src,
      _sink: sink,
      items,
      upstream_events,
      queries,
    }
  }

  pub(super) fn start(&self, caps: &gst::Caps) {
    assert!(self.src.push_event(gst::event::StreamStart::new("test")));
    assert!(self.src.push_event(gst::event::Caps::new(caps)));
    assert!(self
      .src
      .push_event(gst::event::Segment::new(&gst::FormattedSegment::<gst::ClockTime>::new())));
  }

  pub(super) fn pad(&self) -> &gst::Pad {
    &self.src
  }

  pub(super) fn push(&self, buffer: gst::Buffer) {
    self.src.push(buffer).unwrap();
  }

  pub(super) fn eos(&self) {
    assert!(self.src.push_event(gst::event::Eos::new()));
  }

  pub(super) fn buffers(&self) -> Vec<gst::Buffer> {
    self
      .items
      .lock()
      .unwrap()
      .iter()
      .filter_map(|item| match item {
        Item::Buffer(buffer) => Some(buffer.clone()),
        _ => None,
      })
      .collect()
  }

  pub(super) fn items(&self) -> Vec<String> {
    self
      .items
      .lock()
      .unwrap()
      .iter()
      .map(|item| match item {
        Item::Buffer(buffer) => format!("buffer pts={:?} size={}", buffer.pts(), buffer.size()),
        Item::Event(event) => format!("{:?}", event.type_()),
      })
      .collect()
  }

  pub(super) fn events(&self, type_: gst::EventType) -> Vec<gst::Event> {
    self
      .items
      .lock()
      .unwrap()
      .iter()
      .filter_map(|item| match item {
        Item::Event(event) if event.type_() == type_ => Some(event.clone()),
        _ => None,
      })
      .collect()
  }

  pub(super) fn upstream_events(&self) -> Vec<gst::Event> {
    self.upstream_events.lock().unwrap().clone()
  }

  pub(super) fn queries(&self) -> Vec<gst::QueryType> {
    self.queries.lock().unwrap().clone()
  }
}

impl Drop for Harness {
  fn drop(&mut self) {
    for element in &self.elements {
      let _ = element.set_state(gst::State::Null);
    }
  }
}

pub(super) fn bytes(buffer: &gst::Buffer) -> Vec<u8> {
  buffer.map_readable().unwrap().as_slice().to_vec()
}
