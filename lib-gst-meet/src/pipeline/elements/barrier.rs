//! `gstmeetbarrier`: a pass-through element that never gives a downstream failure back to its upstream.
//!
//! rtpbin and its jitterbuffers share one streaming thread per SSRC with the element that is linked to them, and
//! they report any non-OK flow as an error of the shared pipeline. A consumer that cannot keep up, refuses its
//! caps or is being torn down must therefore never be visible to them. The barrier sits right behind every rtpbin
//! source pad (and behind the send path) and turns such failures into a callback.

use std::{
  sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, RwLock,
  },
  time::Instant,
};

use anyhow::Result;
use glib::prelude::*;
use gstreamer::{
  prelude::*, subclass::prelude::*, Buffer, Caps, Event, EventView, FlowError, FlowSuccess, Pad, PadDirection,
  PadFlags, PadPresence, PadTemplate, QueryViewMut,
};

type IdleDataCallback = Arc<dyn Fn() + Send + Sync>;
type FailureCallback = Arc<dyn Fn(FlowError) + Send + Sync>;

glib::wrapper! {
  pub(crate) struct StreamBarrier(ObjectSubclass<imp::StreamBarrier>) @extends gstreamer::Element, gstreamer::Object;
}

impl StreamBarrier {
  pub(crate) fn new(name: &str) -> Self {
    glib::Object::builder().property("name", name).build()
  }

  /// `on_idle_data` fires once per period in which data arrives while nothing is linked downstream, and
  /// `on_failure` once per period in which linked downstream elements fail. A new period starts whenever the
  /// downstream is linked or unlinked (see [`Self::track_downstream`]) and when data finds it unlinked. The
  /// callbacks run on the streaming thread and must not block.
  pub(crate) fn set_callbacks(
    &self,
    on_idle_data: impl Fn() + Send + Sync + 'static,
    on_failure: impl Fn(FlowError) + Send + Sync + 'static,
  ) {
    *self.imp().callbacks.write().unwrap_or_else(|e| e.into_inner()) = Callbacks {
      on_idle_data: Some(Arc::new(on_idle_data)),
      on_failure: Some(Arc::new(on_failure)),
    };
  }

  /// The barrier's own src pad is watched for being linked and unlinked. When it leads out of a bin through a
  /// ghost pad it is linked to the ghost pad's internal pad for good, and only the ghost pad itself tells when
  /// the downstream changes: pass it here.
  pub(crate) fn track_downstream(&self, pad: &impl IsA<Pad>) {
    imp::watch_links(&self.imp().periods, pad.upcast_ref());
  }

  pub(crate) fn last_data(&self) -> Option<Instant> {
    *self.imp().last_data.lock().unwrap_or_else(|e| e.into_inner())
  }
}

pub(super) fn register() -> Result<()> {
  gstreamer::Element::register(
    None,
    "gstmeetbarrier",
    gstreamer::Rank::NONE,
    <StreamBarrier as StaticType>::static_type(),
  )?;
  Ok(())
}

/// The downstream's link epoch and what has been reported in it, in one word: a failure that was found by a push
/// started before a relink can then never use up the report of the period that follows it.
///
/// Bits 0 and 1 are the flags of the current period, the rest counts periods.
#[derive(Default)]
struct Periods(AtomicU64);

const IDLE_NOTIFIED: u64 = 1;
const FAILURE_REPORTED: u64 = 2;
const EPOCH_SHIFT: u32 = 2;

impl Periods {
  fn epoch(&self) -> u64 {
    self.0.load(Ordering::SeqCst) >> EPOCH_SHIFT
  }

  /// Replaces the word by `change(word)` unless that is `None`. True if it did.
  fn update(&self, change: impl Fn(u64) -> Option<u64>) -> bool {
    let mut word = self.0.load(Ordering::SeqCst);
    loop {
      let Some(changed) = change(word) else {
        return false;
      };
      match self
        .0
        .compare_exchange_weak(word, changed, Ordering::SeqCst, Ordering::SeqCst)
      {
        Ok(_) => return true,
        Err(current) => word = current,
      }
    }
  }

  /// The downstream was linked or unlinked: nothing has been reported in the period that starts.
  fn renew(&self) {
    self.update(|word| Some(((word >> EPOCH_SHIFT) + 1) << EPOCH_SHIFT));
  }

  /// Data found nothing linked. True the first time in a period.
  fn first_idle_data(&self) -> bool {
    self.0.fetch_or(IDLE_NOTIFIED, Ordering::SeqCst) & IDLE_NOTIFIED == 0
  }

  /// Data found nothing linked, which ends the period of a failure: whatever failed is gone.
  fn failure_gone(&self) {
    self.0.fetch_and(!FAILURE_REPORTED, Ordering::SeqCst);
  }

  /// Data went through, which ends the period in which it did not.
  fn data_flowing(&self) {
    self.0.fetch_and(!IDLE_NOTIFIED, Ordering::SeqCst);
  }

  /// A push that began in `epoch` failed. True if the failure is the first of its period; false if it was
  /// reported already or the downstream has changed since the push began and the failure is its predecessor's.
  fn first_failure(&self, epoch: u64) -> bool {
    self
      .update(|word| (word >> EPOCH_SHIFT == epoch && word & FAILURE_REPORTED == 0).then_some(word | FAILURE_REPORTED))
  }
}

#[derive(Default)]
struct Callbacks {
  on_idle_data: Option<IdleDataCallback>,
  on_failure: Option<FailureCallback>,
}

/// Whether a push on `src` reaches anything. The peer of a pad that leads out of a bin is the ghost pad's
/// internal pad, which is always linked; what counts is whether the ghost pad is.
fn downstream_connected(src: &Pad) -> bool {
  let Some(peer) = src.peer() else {
    return false;
  };
  if peer.is::<gstreamer::GhostPad>() {
    return true;
  }
  match peer
    .downcast_ref::<gstreamer::ProxyPad>()
    .and_then(|proxy| proxy.internal())
  {
    Some(ghost) => ghost.is_linked(),
    None => true,
  }
}

mod imp {
  use super::*;

  pub(crate) struct StreamBarrier {
    sinkpad: Pad,
    srcpad: Pad,
    pub(super) callbacks: RwLock<Callbacks>,
    pub(super) last_data: Mutex<Option<Instant>>,
    pub(super) periods: Arc<Periods>,
    flushing: AtomicBool,
  }

  pub(super) fn watch_links(periods: &Arc<Periods>, pad: &Pad) {
    let (linked, unlinked) = (periods.clone(), periods.clone());
    pad.connect_linked(move |_, _| linked.renew());
    pad.connect_unlinked(move |_, _| unlinked.renew());
  }

  #[glib::object_subclass]
  impl ObjectSubclass for StreamBarrier {
    const NAME: &'static str = "GstMeetBarrier";
    type Type = super::StreamBarrier;
    type ParentType = gstreamer::Element;

    fn with_class(klass: &Self::Class) -> Self {
      let sink_template = klass.pad_template("sink").expect("sink template is registered");
      let src_template = klass.pad_template("src").expect("src template is registered");

      let sinkpad = Pad::builder_from_template(&sink_template)
        .chain_function(|pad, parent, buffer| {
          StreamBarrier::catch_panic_pad_function(parent, || Err(FlowError::Error), |this| this.sink_chain(pad, buffer))
        })
        .event_function(|pad, parent, event| {
          StreamBarrier::catch_panic_pad_function(parent, || false, |this| this.sink_event(pad, event))
        })
        .query_function(|pad, parent, query| {
          StreamBarrier::catch_panic_pad_function(parent, || false, |this| this.sink_query(pad, query))
        })
        .build();
      let srcpad = Pad::builder_from_template(&src_template)
        .event_function(|pad, parent, event| {
          StreamBarrier::catch_panic_pad_function(parent, || false, |this| this.src_event(pad, event))
        })
        .query_function(|pad, parent, query| {
          StreamBarrier::catch_panic_pad_function(parent, || false, |this| this.src_query(pad, query))
        })
        .build();

      Self {
        sinkpad,
        srcpad,
        callbacks: RwLock::default(),
        last_data: Mutex::new(None),
        periods: Arc::default(),
        flushing: AtomicBool::new(false),
      }
    }
  }

  impl ObjectImpl for StreamBarrier {
    fn constructed(&self) {
      self.parent_constructed();
      let obj = self.obj();
      obj.add_pad(&self.sinkpad).expect("a new element accepts its pad");
      obj.add_pad(&self.srcpad).expect("a new element accepts its pad");
      watch_links(&self.periods, &self.srcpad);
    }
  }

  impl GstObjectImpl for StreamBarrier {}

  impl ElementImpl for StreamBarrier {
    fn metadata() -> Option<&'static gstreamer::subclass::ElementMetadata> {
      static METADATA: std::sync::LazyLock<gstreamer::subclass::ElementMetadata> = std::sync::LazyLock::new(|| {
        gstreamer::subclass::ElementMetadata::new(
          "Stream barrier",
          "Generic",
          "Keeps downstream failures away from its upstream",
          "gst-meet",
        )
      });
      Some(&*METADATA)
    }

    fn pad_templates() -> &'static [PadTemplate] {
      static TEMPLATES: std::sync::LazyLock<Vec<PadTemplate>> = std::sync::LazyLock::new(|| {
        let caps = Caps::new_any();
        vec![
          PadTemplate::new("sink", PadDirection::Sink, PadPresence::Always, &caps).expect("valid sink template"),
          PadTemplate::new("src", PadDirection::Src, PadPresence::Always, &caps).expect("valid src template"),
        ]
      });
      TEMPLATES.as_ref()
    }
  }

  impl StreamBarrier {
    fn callbacks(&self) -> (Option<IdleDataCallback>, Option<FailureCallback>) {
      let callbacks = self.callbacks.read().unwrap_or_else(|e| e.into_inner());
      (callbacks.on_idle_data.clone(), callbacks.on_failure.clone())
    }

    fn is_flushing(&self) -> bool {
      self.flushing.load(Ordering::SeqCst)
        || self.sinkpad.pad_flags().contains(PadFlags::FLUSHING)
        || self.srcpad.pad_flags().contains(PadFlags::FLUSHING)
    }

    fn sink_chain(&self, _pad: &Pad, buffer: Buffer) -> Result<FlowSuccess, FlowError> {
      *self.last_data.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());

      if !downstream_connected(&self.srcpad) {
        self.periods.failure_gone();
        if self.periods.first_idle_data() {
          if let (Some(on_idle_data), _) = self.callbacks() {
            on_idle_data();
          }
        }
        return Ok(FlowSuccess::Ok);
      }
      self.periods.data_flowing();

      let epoch = self.periods.epoch();
      match self.srcpad.push(buffer) {
        Ok(success) => Ok(success),
        Err(flow) => {
          if self.is_flushing() {
            return Err(FlowError::Flushing);
          }
          if self.periods.first_failure(epoch) {
            if let (_, Some(on_failure)) = self.callbacks() {
              on_failure(flow);
            }
          }
          Ok(FlowSuccess::Ok)
        },
      }
    }

    fn sink_event(&self, _pad: &Pad, event: Event) -> bool {
      match event.view() {
        EventView::FlushStart(_) => self.flushing.store(true, Ordering::SeqCst),
        EventView::FlushStop(_) => self.flushing.store(false, Ordering::SeqCst),
        _ => {},
      }
      // Whether the consumer took it is the consumer's business.
      self.srcpad.push_event(event);
      true
    }

    /// The barrier accepts any caps: a consumer that refuses them must not stop the upstream's caps event, which
    /// asks this query first. The refusal comes back as a failure of the next buffer instead.
    fn sink_query(&self, _pad: &Pad, query: &mut gstreamer::QueryRef) -> bool {
      match query.view_mut() {
        QueryViewMut::AcceptCaps(accept) => {
          accept.set_result(true);
          true
        },
        QueryViewMut::Caps(caps) => {
          let result = caps.filter_owned().unwrap_or_else(Caps::new_any);
          caps.set_result(&result);
          true
        },
        _ => downstream_connected(&self.srcpad) && self.srcpad.peer_query(query),
      }
    }

    fn src_event(&self, _pad: &Pad, event: Event) -> bool {
      self.sinkpad.push_event(event)
    }

    fn src_query(&self, pad: &Pad, query: &mut gstreamer::QueryRef) -> bool {
      Pad::query_default(pad, Some(&*self.obj()), query)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_failure_found_across_a_relink_belongs_to_the_old_downstream() {
    let periods = Periods::default();
    let begun = periods.epoch();
    // The push began, then the downstream was replaced, then the push failed.
    periods.renew();
    assert!(!periods.first_failure(begun));
    let current = periods.epoch();
    assert!(periods.first_failure(current));
    assert!(!periods.first_failure(current));
    periods.renew();
    assert!(periods.first_failure(periods.epoch()));
  }

  #[test]
  fn data_that_finds_nothing_linked_ends_the_period_of_a_failure() {
    let periods = Periods::default();
    assert!(periods.first_failure(periods.epoch()));
    periods.failure_gone();
    assert!(periods.first_failure(periods.epoch()));
  }

  #[test]
  fn idle_data_is_reported_once_until_data_flows_or_the_downstream_changes() {
    let periods = Periods::default();
    assert!(periods.first_idle_data());
    assert!(!periods.first_idle_data());
    periods.renew();
    assert!(periods.first_idle_data());
    periods.data_flowing();
    assert!(periods.first_idle_data());
  }
}
