//! VP9 superframe builder.
//!
//! `rtpvp9depay` concatenates the payloads of all packets of a picture, so a picture with several spatial layers
//! comes out as several VP9 frames glued together without the superframe index (VP9 bitstream specification,
//! Annex B) that parsers, muxers and ffmpeg need to tell them apart. The depayloader does not say where the frames
//! start, but the RTP payload descriptor does: [`Vp9FrameSizes`] watches the packets entering the depayloader,
//! mirrors its reassembly and remembers the size of every frame per picture. [`Vp9SuperframeBuilder`] looks the
//! sizes up for each depayloaded buffer and appends the index.

use std::{
  collections::VecDeque,
  fmt,
  sync::{Arc, Mutex, MutexGuard},
  time::{Duration, Instant},
};

use anyhow::{Context, Result};
use glib::prelude::*;
use gstreamer as gst;
use gstreamer::prelude::PadExtManual as _;
use gstreamer_rtp::RTPBuffer;

use crate::pipeline::vp9_descriptor;

pub(super) const ELEMENT_NAME: &str = "gstmeetvp9sf";

/// How many depayloaded pictures may be remembered. The depayloader outputs a picture right after its last packet,
/// so only a handful are ever waiting.
const MAX_PICTURES: usize = 32;
/// Entries of pictures the depayloader never output (it drops incomplete ones) are left behind.
const MAX_PICTURE_AGE: Duration = Duration::from_secs(2);
/// Bound on the frames of one picture still being assembled (a superframe holds at most 8).
const MAX_FRAMES_PER_PICTURE: usize = 64;
const MAX_FRAMES_PER_SUPERFRAME: usize = 8;
/// rtpvp9depay discards completed pictures smaller than this.
const DEPAYLOADER_MIN_PICTURE_SIZE: usize = 10;
/// rtpbasedepayload drops packets that are this many or fewer sequence numbers old instead of marking a DISCONT.
const DEPAYLOADER_MAX_REORDER: i16 = 100;
const KEYFRAME_REQUEST_MIN_INTERVAL: Duration = Duration::from_secs(1);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
  mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// The frame sizes of one depayloaded picture, in bitstream order.
struct Picture {
  pts: Option<gst::ClockTime>,
  frames: Vec<u32>,
  added: Instant,
}

struct Packet<'a> {
  payload: &'a [u8],
  seqnum: u16,
  ssrc: u32,
  marker: bool,
  discont: bool,
  pts: Option<gst::ClockTime>,
}

/// Mirrors the reassembly of `rtpvp9depay` (and the sequence checks of `rtpbasedepayload`) to know exactly which
/// bytes end up in each output buffer and where the frames in it start.
///
/// Whether the depayloader outputs a completed picture at all is not mirrored: it holds inter pictures back while it
/// waits for a keyframe, at the start and, if `wait-for-keyframe` is set, after loss. Those pictures stay here until
/// [`Self::take`] is asked for a later one, so the tracker follows the depayloader with either setting.
#[derive(Default)]
struct Tracker {
  next_seqnum: Option<u16>,
  last_ssrc: u32,
  /// A start of picture was seen and the picture is being assembled.
  started: bool,
  /// Completed frames of the picture being assembled.
  frames: Vec<u32>,
  /// The frame whose last packet (E) has not been seen.
  open_frame: Option<u32>,
  size: usize,
  pictures: VecDeque<Picture>,
}

impl Tracker {
  fn reset_picture(&mut self) {
    self.started = false;
    self.frames.clear();
    self.open_frame = None;
    self.size = 0;
  }

  fn reset(&mut self) {
    self.reset_picture();
    self.next_seqnum = None;
    self.pictures.clear();
  }

  fn push(&mut self, packet: Packet<'_>, now: Instant) {
    let mut discont = packet.discont;
    if let Some(next) = self.next_seqnum {
      if packet.ssrc != self.last_ssrc {
        discont = true;
      } else {
        // rtpbasedepayload: gap = compare_seqnum(seqnum, next), negative when packets are missing.
        let gap = next.wrapping_sub(packet.seqnum) as i16;
        if gap < 0 {
          discont = true;
        } else if gap > 0 {
          if gap <= DEPAYLOADER_MAX_REORDER {
            return;
          }
          discont = true;
        }
      }
    }
    self.next_seqnum = Some(packet.seqnum.wrapping_add(1));
    self.last_ssrc = packet.ssrc;
    if discont {
      self.reset_picture();
    }

    // Anything the depayloader finds too small or invalid makes it drop the picture being assembled.
    let Ok(descriptor) = vp9_descriptor::parse(packet.payload) else {
      self.reset_picture();
      return;
    };
    if descriptor.header_len >= packet.payload.len() {
      self.reset_picture();
      return;
    }

    if descriptor.starts_picture() && self.started {
      self.reset_picture();
    }
    if !self.started {
      if !descriptor.begin {
        return;
      }
      self.started = true;
    }

    let len = (packet.payload.len() - descriptor.header_len) as u32;
    self.size += len as usize;
    if descriptor.begin {
      if let Some(unfinished) = self.open_frame.take() {
        self.frames.push(unfinished);
      }
      self.open_frame = Some(len);
    } else {
      *self.open_frame.get_or_insert(0) += len;
    }
    if descriptor.end {
      if let Some(frame) = self.open_frame.take() {
        self.frames.push(frame);
      }
    }
    if self.frames.len() >= MAX_FRAMES_PER_PICTURE {
      self.reset_picture();
      return;
    }

    if packet.marker {
      if self.size < DEPAYLOADER_MIN_PICTURE_SIZE {
        self.reset_picture();
        return;
      }
      let mut frames = std::mem::take(&mut self.frames);
      if let Some(frame) = self.open_frame.take() {
        frames.push(frame);
      }
      self.reset_picture();
      self.pictures.push_back(Picture {
        pts: packet.pts,
        frames,
        added: now,
      });
      while self.pictures.len() > MAX_PICTURES
        || self
          .pictures
          .front()
          .is_some_and(|picture| now.duration_since(picture.added) > MAX_PICTURE_AGE)
      {
        self.pictures.pop_front();
      }
    }
  }

  /// Takes the sizes of the picture the depayloader pushed with this PTS, together with every older picture, which
  /// the depayloader must have dropped.
  fn take(&mut self, pts: Option<gst::ClockTime>) -> Option<Vec<u32>> {
    let index = self.pictures.iter().position(|picture| picture.pts == pts)?;
    self.pictures.drain(..index);
    self.pictures.pop_front().map(|picture| picture.frames)
  }
}

/// Frame sizes of the pictures an `rtpvp9depay` is about to output, filled by a probe on its sink pad.
///
/// The depayloader assigns its output buffer the PTS of the packet carrying the marker bit, and all packets of a
/// picture share one RTP timestamp and hence one PTS from the jitterbuffer, so the PTS identifies the picture.
#[derive(Clone, Default)]
pub(crate) struct Vp9FrameSizes {
  tracker: Arc<Mutex<Tracker>>,
}

impl fmt::Debug for Vp9FrameSizes {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Vp9FrameSizes").finish_non_exhaustive()
  }
}

impl Vp9FrameSizes {
  pub(crate) fn install_probe(&self, depay_sink: &gst::Pad) {
    let sizes = self.clone();
    depay_sink.add_probe(
      gst::PadProbeType::BUFFER | gst::PadProbeType::BUFFER_LIST | gst::PadProbeType::EVENT_FLUSH,
      move |_pad, info| {
        match &info.data {
          Some(gst::PadProbeData::Buffer(buffer)) => sizes.track(buffer),
          Some(gst::PadProbeData::BufferList(list)) => list.iter().for_each(|buffer| sizes.track(buffer)),
          Some(gst::PadProbeData::Event(event)) if event.type_() == gst::EventType::FlushStop => {
            lock(&sizes.tracker).reset()
          },
          _ => {},
        }
        gst::PadProbeReturn::Ok
      },
    );
  }

  fn track(&self, buffer: &gst::BufferRef) {
    let Ok(rtp) = RTPBuffer::from_buffer_readable(buffer) else {
      return;
    };
    let Ok(payload) = rtp.payload() else {
      return;
    };
    let packet = Packet {
      payload,
      seqnum: rtp.seq(),
      ssrc: rtp.ssrc(),
      marker: rtp.is_marker(),
      discont: buffer.flags().contains(gst::BufferFlags::DISCONT),
      pts: buffer.pts(),
    };
    lock(&self.tracker).push(packet, Instant::now());
  }

  /// The frame sizes of the depayloaded picture with this PTS, if it was tracked. Older pictures are forgotten.
  fn take(&self, pts: Option<gst::ClockTime>) -> Option<Vec<u32>> {
    lock(&self.tracker).take(pts)
  }
}

/// The superframe index for frames of the given sizes (VP9 bitstream specification, Annex B).
///
/// ```text
/// marker      1 1 0 | bytes per size - 1 (2 bits) | frames - 1 (3 bits)
/// sizes       little endian, `bytes per size` each
/// marker      the same byte again
/// ```
fn superframe_index(sizes: &[u32]) -> Option<Vec<u8>> {
  if !(2..=MAX_FRAMES_PER_SUPERFRAME).contains(&sizes.len()) || sizes.contains(&0) {
    return None;
  }
  let largest = sizes.iter().copied().max()?;
  let size_bytes = match largest {
    0..=0xFF => 1,
    0x100..=0xFFFF => 2,
    0x1_0000..=0xFF_FFFF => 3,
    _ => 4,
  };
  let marker = 0xC0 | ((size_bytes - 1) as u8) << 3 | (sizes.len() - 1) as u8;
  let mut index = Vec::with_capacity(2 + size_bytes * sizes.len());
  index.push(marker);
  for size in sizes {
    index.extend_from_slice(&size.to_le_bytes()[..size_bytes]);
  }
  index.push(marker);
  Some(index)
}

#[derive(Debug, PartialEq, Eq)]
enum Plan {
  Passthrough,
  Append(Vec<u8>),
  Mismatch,
}

fn plan(frames: Option<&[u32]>, buffer_size: usize) -> Plan {
  let Some(frames) = frames else {
    return Plan::Mismatch;
  };
  let total: u64 = frames.iter().map(|&size| u64::from(size)).sum();
  if total != buffer_size as u64 {
    return Plan::Mismatch;
  }
  match frames {
    [] => Plan::Mismatch,
    [_] => Plan::Passthrough,
    _ => superframe_index(frames).map_or(Plan::Mismatch, Plan::Append),
  }
}

/// Makes rtpbin send a PLI/FIR.
fn force_key_unit_event() -> gst::Event {
  let fku = gst::Structure::builder("GstForceKeyUnit")
    .field("running-time", gst::ClockTime::NONE)
    .field("all-headers", true)
    .field("count", 0u32)
    .build();
  gst::event::CustomUpstream::builder(fku).build()
}

glib::wrapper! {
  pub struct Vp9SuperframeBuilder(ObjectSubclass<imp::Vp9SuperframeBuilder>) @extends gst::Element, gst::Object;
}

impl Vp9SuperframeBuilder {
  fn set_frame_sizes(&self, sizes: Vp9FrameSizes) {
    use gst::subclass::prelude::ObjectSubclassIsExt as _;
    lock(&self.imp().state).sizes = Some(sizes);
  }
}

/// A `gstmeetvp9sf` that builds superframes from the sizes tracked by `sizes`, which must have its probe installed
/// on the sink pad of the `rtpvp9depay` feeding it.
pub(crate) fn make_vp9_superframe_builder(sizes: Vp9FrameSizes) -> Result<gst::Element> {
  super::register()?;
  let element = gst::ElementFactory::make(ELEMENT_NAME)
    .build()
    .with_context(|| format!("failed to create {}", ELEMENT_NAME))?;
  element
    .downcast_ref::<Vp9SuperframeBuilder>()
    .context("unexpected type for gstmeetvp9sf")?
    .set_frame_sizes(sizes);
  Ok(element)
}

pub(super) fn register() -> Result<(), glib::BoolError> {
  gst::Element::register(
    None,
    ELEMENT_NAME,
    gst::Rank::NONE,
    <Vp9SuperframeBuilder as StaticType>::static_type(),
  )
}

mod imp {
  use std::{
    sync::{LazyLock, Mutex},
    time::Instant,
  };

  use gstreamer as gst;
  use gstreamer::{prelude::*, subclass::prelude::*};

  use super::{force_key_unit_event, lock, plan, Plan, Vp9FrameSizes, ELEMENT_NAME, KEYFRAME_REQUEST_MIN_INTERVAL};

  static CAT: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
    gst::DebugCategory::new(
      ELEMENT_NAME,
      gst::DebugColorFlags::empty(),
      Some("VP9 superframe builder"),
    )
  });

  #[derive(Default)]
  pub(super) struct State {
    /// Without sizes the element passes everything through.
    pub(super) sizes: Option<Vp9FrameSizes>,
    /// The previous buffer was dropped, so the next one is marked DISCONT.
    discont_pending: bool,
    last_key_request: Option<Instant>,
  }

  pub struct Vp9SuperframeBuilder {
    sinkpad: gst::Pad,
    srcpad: gst::Pad,
    pub(super) state: Mutex<State>,
  }

  #[glib::object_subclass]
  impl ObjectSubclass for Vp9SuperframeBuilder {
    const NAME: &'static str = "GstMeetVp9SuperframeBuilder";
    type Type = super::Vp9SuperframeBuilder;
    type ParentType = gst::Element;

    fn with_class(klass: &Self::Class) -> Self {
      let sink_template = klass.pad_template("sink").expect("sink pad template is declared");
      let sinkpad = gst::Pad::builder_from_template(&sink_template)
        .chain_function(|pad, parent, buffer| {
          Vp9SuperframeBuilder::catch_panic_pad_function(
            parent,
            || Err(gst::FlowError::Error),
            |this| this.sink_chain(pad, buffer),
          )
        })
        .query_function(|pad, parent, query| {
          Vp9SuperframeBuilder::catch_panic_pad_function(parent, || false, |this| this.query(pad, query))
        })
        // The default event handler only forwards caps events from pads that proxy caps.
        .flags(gst::PadFlags::PROXY_CAPS)
        .build();
      let src_template = klass.pad_template("src").expect("src pad template is declared");
      let srcpad = gst::Pad::builder_from_template(&src_template)
        .query_function(|pad, parent, query| {
          Vp9SuperframeBuilder::catch_panic_pad_function(parent, || false, |this| this.query(pad, query))
        })
        .build();
      Self {
        sinkpad,
        srcpad,
        state: Mutex::new(State::default()),
      }
    }
  }

  impl ObjectImpl for Vp9SuperframeBuilder {
    fn constructed(&self) {
      self.parent_constructed();
      let obj = self.obj();
      obj.add_pad(&self.sinkpad).expect("adding the sink pad");
      obj.add_pad(&self.srcpad).expect("adding the src pad");
    }
  }

  impl GstObjectImpl for Vp9SuperframeBuilder {}

  impl ElementImpl for Vp9SuperframeBuilder {
    fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
      static METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
        gst::subclass::ElementMetadata::new(
          "VP9 superframe builder",
          "Codec/Parser/Video",
          "Appends a superframe index to depayloaded VP9 pictures made of several frames",
          "gst-meet",
        )
      });
      Some(&*METADATA)
    }

    fn pad_templates() -> &'static [gst::PadTemplate] {
      static PAD_TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
        let caps = gst::Caps::builder("video/x-vp9").build();
        vec![
          gst::PadTemplate::new("sink", gst::PadDirection::Sink, gst::PadPresence::Always, &caps)
            .expect("sink pad template"),
          gst::PadTemplate::new("src", gst::PadDirection::Src, gst::PadPresence::Always, &caps)
            .expect("src pad template"),
        ]
      });
      PAD_TEMPLATES.as_ref()
    }

    fn change_state(&self, transition: gst::StateChange) -> Result<gst::StateChangeSuccess, gst::StateChangeError> {
      if transition == gst::StateChange::ReadyToPaused {
        self.reset();
      }
      let success = self.parent_change_state(transition)?;
      if transition == gst::StateChange::PausedToReady {
        self.reset();
      }
      Ok(success)
    }
  }

  impl Vp9SuperframeBuilder {
    fn reset(&self) {
      let mut state = lock(&self.state);
      state.discont_pending = false;
      state.last_key_request = None;
    }

    /// The caps are not touched, so queries are answered by the peer on the other side.
    fn query(&self, pad: &gst::Pad, query: &mut gst::QueryRef) -> bool {
      match query.type_() {
        gst::QueryType::Caps => pad.proxy_query_caps(query),
        gst::QueryType::AcceptCaps => {
          // The verdict is in the query; returning false would mean "not handled", which callers read as accepted.
          pad.proxy_query_accept_caps(query);
          true
        },
        // The default only forwards it on pads flagged as proxying allocations.
        gst::QueryType::Allocation if pad == &self.sinkpad => self.srcpad.peer_query(query),
        _ => gst::Pad::query_default(pad, Some(&*self.obj()), query),
      }
    }

    fn sink_chain(&self, _pad: &gst::Pad, mut buffer: gst::Buffer) -> Result<gst::FlowSuccess, gst::FlowError> {
      let (outcome, request_key_unit) = {
        let mut state = lock(&self.state);
        let Some(sizes) = state.sizes.clone() else {
          drop(state);
          return self.srcpad.push(buffer);
        };
        let frames = sizes.take(buffer.pts());
        match plan(frames.as_deref(), buffer.size()) {
          Plan::Mismatch => {
            state.discont_pending = true;
            let due = state
              .last_key_request
              .is_none_or(|at| at.elapsed() >= KEYFRAME_REQUEST_MIN_INTERVAL);
            if due {
              state.last_key_request = Some(Instant::now());
            }
            gst::warning!(
              CAT,
              imp = self,
              "dropping a {} byte buffer, the RTP frame sizes {:?} do not describe it",
              buffer.size(),
              frames
            );
            (None, due)
          },
          plan => {
            if std::mem::take(&mut state.discont_pending) {
              buffer.make_mut().set_flags(gst::BufferFlags::DISCONT);
            }
            (Some(plan), false)
          },
        }
      };

      if request_key_unit {
        self.sinkpad.push_event(force_key_unit_event());
      }
      match outcome {
        None => Ok(gst::FlowSuccess::Ok),
        Some(Plan::Append(index)) => {
          buffer.make_mut().append_memory(gst::Memory::from_slice(index));
          self.srcpad.push(buffer)
        },
        Some(_) => self.srcpad.push(buffer),
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn index_of(sizes: &[u32]) -> Vec<u8> {
    superframe_index(sizes).unwrap()
  }

  #[test]
  fn index_bytes_follow_annex_b() {
    // 0b110 | 00 (one byte per size) | 001 (two frames)
    assert_eq!(index_of(&[100, 200]), [0xC1, 100, 200, 0xC1]);
    // Two bytes: 0b110 | 01 | 010 (three frames), little endian.
    assert_eq!(
      index_of(&[0x0102, 0x0A, 0x1_00]),
      [0xCA, 0x02, 0x01, 0x0A, 0x00, 0x00, 0x01, 0xCA]
    );
    // Three bytes: 0b110 | 10 | 001.
    assert_eq!(
      index_of(&[0x01_0000, 1]),
      [0xD1, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0xD1]
    );
    // Four bytes: 0b110 | 11 | 001.
    assert_eq!(
      index_of(&[0x0100_0000, 7]),
      [0xD9, 0x00, 0x00, 0x00, 0x01, 0x07, 0x00, 0x00, 0x00, 0xD9]
    );
    // Eight frames: 0b110 | 00 | 111.
    let eight = index_of(&[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(eight.len(), 2 + 8);
    assert_eq!((eight[0], eight[9]), (0xC7, 0xC7));
  }

  #[test]
  fn size_width_boundaries() {
    assert_eq!(index_of(&[0xFF, 1])[0], 0xC1);
    assert_eq!(index_of(&[0x100, 1])[0], 0xC9);
    assert_eq!(index_of(&[0xFFFF, 1])[0], 0xC9);
    assert_eq!(index_of(&[0x1_0000, 1])[0], 0xD1);
    assert_eq!(index_of(&[0xFF_FFFF, 1])[0], 0xD1);
    assert_eq!(index_of(&[0x100_0000, 1])[0], 0xD9);
    assert_eq!(index_of(&[u32::MAX, 1])[0], 0xD9);
  }

  #[test]
  fn index_needs_two_to_eight_nonempty_frames() {
    assert_eq!(superframe_index(&[]), None);
    assert_eq!(superframe_index(&[5]), None);
    assert_eq!(superframe_index(&[1; 9]), None);
    assert_eq!(superframe_index(&[5, 0]), None);
    assert!(superframe_index(&[1; 8]).is_some());
  }

  #[test]
  fn plans() {
    assert_eq!(plan(Some(&[10]), 10), Plan::Passthrough);
    assert_eq!(plan(Some(&[10]), 11), Plan::Mismatch);
    assert_eq!(plan(Some(&[4, 6]), 10), Plan::Append(vec![0xC1, 4, 6, 0xC1]));
    assert_eq!(plan(Some(&[4, 6]), 9), Plan::Mismatch);
    assert_eq!(plan(Some(&[4, 6]), 11), Plan::Mismatch);
    assert_eq!(plan(Some(&[]), 0), Plan::Mismatch);
    assert_eq!(plan(None, 10), Plan::Mismatch);
    assert_eq!(plan(Some(&[1; 9]), 9), Plan::Mismatch);
    // The sum must not wrap.
    assert_eq!(plan(Some(&[u32::MAX, u32::MAX]), 0), Plan::Mismatch);
    assert_eq!(plan(Some(&[u32::MAX, 2]), 1), Plan::Mismatch);
  }

  /// Bit flags of the first descriptor byte.
  const B: u8 = 0x08;
  const E: u8 = 0x04;
  const L: u8 = 0x20;

  struct Source {
    seqnum: u16,
    pts: u64,
    now: Instant,
    tracker: Tracker,
  }

  impl Source {
    fn new() -> Self {
      Source {
        seqnum: 100,
        pts: 1,
        now: Instant::now(),
        tracker: Tracker::default(),
      }
    }

    fn send(&mut self, descriptor: &[u8], len: usize, marker: bool) {
      self.send_with(descriptor, len, marker, false);
    }

    fn send_with(&mut self, descriptor: &[u8], len: usize, marker: bool, discont: bool) {
      let mut payload = descriptor.to_vec();
      payload.extend(std::iter::repeat_n(0x55, len));
      let seqnum = self.seqnum;
      self.seqnum = self.seqnum.wrapping_add(1);
      self.tracker.push(
        Packet {
          payload: &payload,
          seqnum,
          ssrc: 7,
          marker,
          discont,
          pts: Some(gst::ClockTime::from_nseconds(self.pts)),
        },
        self.now,
      );
    }

    fn skip_seqnum(&mut self) {
      self.seqnum = self.seqnum.wrapping_add(1);
    }

    fn next_picture(&mut self) {
      self.pts += 1;
    }

    fn take(&mut self, pts: u64) -> Option<Vec<u32>> {
      self.tracker.take(Some(gst::ClockTime::from_nseconds(pts)))
    }
  }

  // Descriptors with the layer indices present (L, non-flexible: layer byte and TL0PICIDX), D = `d`, SID = `sid`.
  fn layered(flags: u8, sid: u8, d: bool) -> [u8; 3] {
    [flags | L, sid << 1 | u8::from(d), 0]
  }

  #[test]
  fn single_frame_in_several_packets() {
    let mut s = Source::new();
    s.send(&[B], 1000, false);
    s.send(&[0x00], 1000, false);
    s.send(&[E], 500, true);
    assert_eq!(s.take(1), Some(vec![2500]));
    assert_eq!(s.take(1), None);
  }

  #[test]
  fn spatial_layers_become_separate_frames() {
    let mut s = Source::new();
    // Layer 0: two packets. Layer 1 depends on it (D): three packets, the last one carries the marker.
    s.send(&layered(B, 0, false), 800, false);
    s.send(&layered(E, 0, false), 200, false);
    s.send(&layered(B, 1, true), 700, false);
    s.send(&layered(0, 1, true), 700, false);
    s.send(&layered(E, 1, true), 100, true);
    assert_eq!(s.take(1), Some(vec![1000, 1500]));
  }

  #[test]
  fn frame_without_end_marker_is_closed_by_the_picture_end() {
    let mut s = Source::new();
    s.send(&layered(B | E, 0, false), 50, false);
    s.send(&layered(B, 1, true), 60, true);
    assert_eq!(s.take(1), Some(vec![50, 60]));
  }

  #[test]
  fn consecutive_pictures_keep_their_own_sizes() {
    let mut s = Source::new();
    s.send(&[B | E], 100, true);
    s.next_picture();
    s.send(&layered(B | E, 0, false), 20, false);
    s.send(&layered(B | E, 1, true), 30, true);
    s.next_picture();
    s.send(&[B | E], 40, true);
    assert_eq!(s.take(1), Some(vec![100]));
    assert_eq!(s.take(2), Some(vec![20, 30]));
    assert_eq!(s.take(3), Some(vec![40]));
  }

  #[test]
  fn pictures_the_depayloader_dropped_are_discarded_on_lookup() {
    let mut s = Source::new();
    s.send(&[B | E], 100, true);
    s.next_picture();
    s.send(&[B | E], 200, true);
    s.next_picture();
    s.send(&[B | E], 300, true);
    // The depayloader never output the first two (waiting for a keyframe, say).
    assert_eq!(s.take(3), Some(vec![300]));
    assert_eq!(s.take(1), None);
    assert_eq!(s.take(2), None);
  }

  #[test]
  fn missing_packets_drop_the_picture_like_the_depayloader() {
    let mut s = Source::new();
    s.send(&[B], 100, false);
    s.skip_seqnum();
    s.send(&[E], 100, true);
    // The depayloader discards the partial picture on the gap and ignores the E packet without a start.
    assert_eq!(s.take(1), None);

    s.next_picture();
    s.send(&[B | E], 100, true);
    assert_eq!(s.take(2), Some(vec![100]));
  }

  #[test]
  fn a_discont_flag_drops_the_partial_picture() {
    let mut s = Source::new();
    s.send(&[B], 100, false);
    s.send_with(&[E], 100, true, true);
    assert_eq!(s.take(1), None);
  }

  #[test]
  fn a_discont_packet_that_starts_a_picture_is_kept() {
    let mut s = Source::new();
    s.send(&[B], 100, false);
    s.send_with(&[B | E], 80, true, true);
    assert_eq!(s.take(1), Some(vec![80]));
  }

  #[test]
  fn a_new_start_of_picture_flushes_the_unfinished_one() {
    let mut s = Source::new();
    s.send(&[B], 100, false);
    // No marker was seen for it, and the next picture begins.
    s.next_picture();
    s.send(&[B | E], 70, true);
    assert_eq!(s.take(1), None);
    assert_eq!(s.take(2), Some(vec![70]));
  }

  #[test]
  fn upper_layer_without_inter_layer_dependency_restarts_the_picture_like_the_depayloader() {
    // K-SVC delta pictures: the upper layer has D = 0, which rtpvp9depay takes for a new picture, dropping what it
    // had assembled. The output only holds the upper layer frame, and so do the tracked sizes.
    let mut s = Source::new();
    s.send(&layered(B | E, 0, false), 400, false);
    s.send(&layered(B | E, 1, false), 900, true);
    assert_eq!(s.take(1), Some(vec![900]));
  }

  #[test]
  fn tiny_pictures_are_dropped_like_the_depayloader_does() {
    let mut s = Source::new();
    s.send(&[B | E], 3, true);
    assert_eq!(s.take(1), None);
  }

  #[test]
  fn invalid_descriptors_drop_the_picture() {
    let mut s = Source::new();
    s.send(&[B], 100, false);
    // An I bit without the picture id byte.
    s.tracker.push(
      Packet {
        payload: &[0x80],
        seqnum: s.seqnum,
        ssrc: 7,
        marker: true,
        discont: false,
        pts: None,
      },
      s.now,
    );
    assert_eq!(s.take(1), None);
    // A descriptor without any payload.
    s.seqnum += 1;
    s.send(&[B | E], 0, true);
    assert_eq!(s.take(1), None);
  }

  #[test]
  fn old_packets_are_ignored_and_ssrc_changes_start_over() {
    let mut s = Source::new();
    s.send(&[B], 100, false);
    // A retransmitted packet from before is dropped by rtpbasedepayload.
    s.seqnum -= 1;
    s.send(&[B], 5, false);
    s.send(&[E], 100, true);
    assert_eq!(s.take(1), Some(vec![200]));

    // New SSRC: a discontinuity.
    s.next_picture();
    s.send(&[B], 100, false);
    s.tracker.push(
      Packet {
        payload: &[E, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
        seqnum: s.seqnum,
        ssrc: 8,
        marker: true,
        discont: false,
        pts: Some(gst::ClockTime::from_nseconds(s.pts)),
      },
      s.now,
    );
    assert_eq!(s.take(2), None);
  }

  #[test]
  fn sequence_numbers_wrap() {
    let mut s = Source::new();
    s.seqnum = 0xFFFF;
    s.send(&[B], 100, false);
    s.send(&[E], 100, true);
    assert_eq!(s.take(1), Some(vec![200]));
  }

  #[test]
  fn flush_clears_everything() {
    let mut s = Source::new();
    s.send(&[B | E], 100, true);
    s.tracker.reset();
    assert_eq!(s.take(1), None);
  }

  #[test]
  fn memory_is_bounded() {
    let mut s = Source::new();
    for _ in 0..(MAX_PICTURES * 3) {
      s.next_picture();
      s.send(&[B | E], 100, true);
      assert!(s.tracker.pictures.len() <= MAX_PICTURES);
    }
    assert_eq!(s.tracker.pictures.len(), MAX_PICTURES);
    assert_eq!(s.take(1), None);
    assert_eq!(s.take(s.pts), Some(vec![100]));

    // Entries older than the maximum age are dropped when the next picture arrives.
    s.next_picture();
    s.send(&[B | E], 100, true);
    s.now += MAX_PICTURE_AGE + Duration::from_millis(1);
    s.next_picture();
    s.send(&[B | E], 100, true);
    assert_eq!(s.tracker.pictures.len(), 1);

    // A picture never ending does not accumulate frames forever.
    for _ in 0..(MAX_FRAMES_PER_PICTURE * 2) {
      s.send(&layered(B | E, 1, true), 100, false);
    }
    assert!(s.tracker.frames.len() < MAX_FRAMES_PER_PICTURE);
  }

  #[test]
  fn pictures_without_pts_are_matched_in_order() {
    let mut tracker = Tracker::default();
    let now = Instant::now();
    for (seqnum, len) in [(1u16, 100usize), (2, 200)] {
      let mut payload = vec![B | E];
      payload.extend(std::iter::repeat_n(0, len));
      tracker.push(
        Packet {
          payload: &payload,
          seqnum,
          ssrc: 1,
          marker: true,
          discont: false,
          pts: None,
        },
        now,
      );
    }
    assert_eq!(tracker.take(None), Some(vec![100]));
    assert_eq!(tracker.take(None), Some(vec![200]));
    assert_eq!(tracker.take(None), None);
  }

  use gstreamer::prelude::*;

  use crate::pipeline::elements::test_util::{bytes, Harness};

  fn rtp_caps() -> gst::Caps {
    gst::init().unwrap();
    gst::Caps::builder("application/x-rtp")
      .field("media", "video")
      .field("clock-rate", 90000i32)
      .field("encoding-name", "VP9")
      .field("payload", 96i32)
      .build()
  }

  /// A depayloader followed by the superframe builder, which reads the sizes the probe collects.
  fn depay_chain() -> Option<(Harness, Vp9FrameSizes)> {
    gst::init().unwrap();
    let depay = gst::ElementFactory::make("rtpvp9depay").build().ok()?;
    let sizes = Vp9FrameSizes::default();
    sizes.install_probe(&depay.static_pad("sink").unwrap());
    let builder = make_vp9_superframe_builder(sizes.clone()).unwrap();
    let harness = Harness::new(vec![depay, builder], None);
    harness.start(&rtp_caps());
    Some((harness, sizes))
  }

  struct Stream {
    seqnum: u16,
    timestamp: u32,
  }

  /// One layer frame of a picture. `depends` is the D bit of its descriptor.
  struct LayerFrame {
    data: Vec<u8>,
    depends: bool,
  }

  fn frame(tag: u8, len: usize) -> Vec<u8> {
    (0..len).map(|i| tag.wrapping_add(i as u8)).collect()
  }

  impl Stream {
    fn new() -> Self {
      Stream {
        seqnum: 1000,
        timestamp: 90000,
      }
    }

    /// RTP packets for a picture, `packet_size` payload bytes at most each, the marker on the last one.
    fn picture(&mut self, frames: &[LayerFrame], packet_size: usize) -> Vec<gst::Buffer> {
      let mut packets = Vec::new();
      for (spatial_id, layer_frame) in frames.iter().enumerate() {
        let chunks: Vec<_> = layer_frame.data.chunks(packet_size).collect();
        for (i, chunk) in chunks.iter().enumerate() {
          let mut flags = L;
          if i == 0 {
            flags |= B;
          }
          if i == chunks.len() - 1 {
            flags |= E;
          }
          let marker = spatial_id == frames.len() - 1 && i == chunks.len() - 1;
          let mut payload = layered(flags, spatial_id as u8, layer_frame.depends).to_vec();
          payload.extend_from_slice(chunk);
          packets.push(self.packet(&payload, marker));
        }
      }
      self.timestamp += 3000;
      packets
    }

    fn packet(&mut self, payload: &[u8], marker: bool) -> gst::Buffer {
      let mut data = vec![0x80, if marker { 0x80 | 96 } else { 96 }];
      data.extend_from_slice(&self.seqnum.to_be_bytes());
      data.extend_from_slice(&self.timestamp.to_be_bytes());
      data.extend_from_slice(&[0, 0, 0, 1]);
      data.extend_from_slice(payload);
      self.seqnum = self.seqnum.wrapping_add(1);
      let mut buffer = gst::Buffer::from_mut_slice(data);
      buffer.get_mut().unwrap().set_pts(gst::ClockTime::from_nseconds(
        u64::from(self.timestamp) * 1_000_000_000 / 90000,
      ));
      buffer
    }
  }

  fn layer(data: Vec<u8>, depends: bool) -> LayerFrame {
    LayerFrame { data, depends }
  }

  /// The decoder side of Annex B, as libvpx implements it.
  fn split_superframe(data: &[u8]) -> Option<Vec<&[u8]>> {
    let marker = *data.last()?;
    if marker & 0xE0 != 0xC0 {
      return None;
    }
    let frames = usize::from(marker & 0x07) + 1;
    let size_bytes = usize::from((marker >> 3) & 0x03) + 1;
    let index_len = 2 + size_bytes * frames;
    if data.len() < index_len || data[data.len() - index_len] != marker {
      return None;
    }
    let sizes = &data[data.len() - index_len + 1..data.len() - 1];
    let mut offset = 0;
    let mut out = Vec::new();
    for size in sizes.chunks(size_bytes) {
      let size = size.iter().rev().fold(0usize, |acc, &b| acc << 8 | usize::from(b));
      out.push(data.get(offset..offset + size)?);
      offset += size;
    }
    (offset == data.len() - index_len).then_some(out)
  }

  fn push_all(harness: &Harness, packets: Vec<gst::Buffer>) {
    for packet in packets {
      harness.push(packet);
    }
  }

  #[test]
  fn a_picture_of_two_layers_gets_a_superframe_index() {
    let Some((harness, _)) = depay_chain() else { return };
    let mut stream = Stream::new();
    let (base, upper) = (frame(0x10, 700), frame(0x80, 1900));

    push_all(
      &harness,
      stream.picture(&[layer(base.clone(), false), layer(upper.clone(), true)], 500),
    );

    let buffers = harness.buffers();
    assert_eq!(buffers.len(), 1);
    let out = bytes(&buffers[0]);
    assert_eq!(out.len(), 700 + 1900 + 2 + 2 * 2, "two byte sizes");
    assert_eq!(split_superframe(&out).unwrap(), [&base[..], &upper[..]]);
    assert!(!buffers[0].flags().contains(gst::BufferFlags::DISCONT));
  }

  #[test]
  fn small_frames_use_one_byte_sizes_and_large_ones_three() {
    let Some((harness, _)) = depay_chain() else { return };
    let mut stream = Stream::new();
    let small = [frame(1, 100), frame(2, 200), frame(3, 50)];
    let large = [frame(4, 70_000), frame(5, 20)];

    push_all(&harness, stream.picture(&small.clone().map(|f| layer(f, true)), 1200));
    push_all(&harness, stream.picture(&large.clone().map(|f| layer(f, true)), 1200));

    let buffers = harness.buffers();
    assert_eq!(buffers.len(), 2);
    let first = bytes(&buffers[0]);
    assert_eq!(first.len(), 350 + 2 + 3);
    assert_eq!(first[first.len() - 1], 0xC2, "three frames, one byte sizes");
    assert_eq!(
      split_superframe(&first).unwrap(),
      [&small[0][..], &small[1][..], &small[2][..]]
    );
    let second = bytes(&buffers[1]);
    assert_eq!(second[second.len() - 1], 0xD1, "two frames, three byte sizes");
    assert_eq!(split_superframe(&second).unwrap(), [&large[0][..], &large[1][..]]);
  }

  #[test]
  fn single_frame_pictures_pass_through_unchanged() {
    let Some((harness, _)) = depay_chain() else { return };
    let mut stream = Stream::new();
    let data = frame(0x33, 3000);

    push_all(&harness, stream.picture(&[layer(data.clone(), false)], 1000));
    push_all(&harness, stream.picture(&[layer(frame(0x44, 60), false)], 1000));

    let buffers = harness.buffers();
    assert_eq!(buffers.len(), 2);
    assert_eq!(bytes(&buffers[0]), data);
    assert_eq!(bytes(&buffers[1]), frame(0x44, 60));
  }

  #[test]
  fn timestamps_and_flags_of_the_depayloader_are_kept() {
    let Some((harness, _)) = depay_chain() else { return };
    let mut stream = Stream::new();

    push_all(
      &harness,
      stream.picture(&[layer(frame(1, 300), false), layer(frame(2, 300), true)], 1200),
    );

    let buffers = harness.buffers();
    assert_eq!(buffers[0].pts(), Some(gst::ClockTime::from_nseconds(1_000_000_000)));
    assert_eq!(
      harness.events(gst::EventType::Caps).len(),
      1,
      "the caps of the depayloader reach downstream"
    );
    // Non-flexible layer frames have P clear, so the depayloader says keyframe.
    assert!(!buffers[0].flags().contains(gst::BufferFlags::DELTA_UNIT));
  }

  #[test]
  fn lost_packets_make_the_depayloader_and_the_tracker_drop_the_same_picture() {
    let Some((harness, _)) = depay_chain() else { return };
    let mut stream = Stream::new();
    let two_layers = |tag| [layer(frame(tag, 900), false), layer(frame(tag + 1, 900), true)];

    push_all(&harness, stream.picture(&two_layers(1), 400));
    // The next picture loses a packet in the middle of its upper layer.
    let mut damaged = stream.picture(&two_layers(10), 400);
    damaged.remove(4);
    push_all(&harness, damaged);
    push_all(&harness, stream.picture(&two_layers(20), 400));

    let buffers = harness.buffers();
    let frames: Vec<_> = buffers
      .iter()
      .map(|b| {
        split_superframe(&bytes(b))
          .unwrap()
          .into_iter()
          .map(|f| f[0])
          .collect::<Vec<_>>()
      })
      .collect();
    assert_eq!(frames, [vec![1, 2], vec![20, 21]]);
    assert!(buffers[1].flags().contains(gst::BufferFlags::DISCONT));
    assert!(harness.upstream_events().is_empty(), "nothing was out of sync");
  }

  #[test]
  fn k_svc_pictures_where_the_depayloader_keeps_one_layer_stay_valid() {
    let Some((harness, _)) = depay_chain() else { return };
    let mut stream = Stream::new();

    // Upper layers without inter-layer dependency start a new picture for rtpvp9depay, which drops the base layer.
    push_all(
      &harness,
      stream.picture(&[layer(frame(1, 500), false), layer(frame(2, 800), false)], 400),
    );

    let buffers = harness.buffers();
    assert_eq!(buffers.len(), 1);
    assert_eq!(bytes(&buffers[0]), frame(2, 800));
  }

  #[test]
  fn untracked_pictures_are_dropped_and_a_keyframe_is_requested() {
    gst::init().unwrap();
    let Ok(depay) = gst::ElementFactory::make("rtpvp9depay").build() else {
      return;
    };
    let sizes = Vp9FrameSizes::default();
    let builder = make_vp9_superframe_builder(sizes.clone()).unwrap();
    let harness = Harness::new(vec![depay.clone(), builder], None);
    harness.start(&rtp_caps());
    let mut stream = Stream::new();
    let pair = |tag| [layer(frame(tag, 400), false), layer(frame(tag + 1, 400), true)];

    // The probe is not there yet: the builder cannot tell what is in the first picture.
    push_all(&harness, stream.picture(&pair(1), 1200));
    assert!(harness.buffers().is_empty());
    let requests = harness.upstream_events();
    assert_eq!(requests.len(), 1);
    let fku = requests[0].structure().unwrap();
    assert_eq!(fku.name(), "GstForceKeyUnit");
    assert!(fku.get::<bool>("all-headers").unwrap());

    sizes.install_probe(&depay.static_pad("sink").unwrap());
    push_all(&harness, stream.picture(&pair(10), 1200));

    let buffers = harness.buffers();
    assert_eq!(buffers.len(), 1);
    assert!(
      buffers[0].flags().contains(gst::BufferFlags::DISCONT),
      "the loss is announced downstream"
    );
    assert_eq!(split_superframe(&bytes(&buffers[0])).unwrap().len(), 2);
    assert_eq!(harness.upstream_events().len(), 1, "no second request");

    // Only the following buffer is clean again.
    push_all(&harness, stream.picture(&pair(20), 1200));
    assert!(!harness.buffers()[1].flags().contains(gst::BufferFlags::DISCONT));
  }

  #[test]
  fn keyframe_requests_are_rate_limited() {
    gst::init().unwrap();
    let Ok(depay) = gst::ElementFactory::make("rtpvp9depay").build() else {
      return;
    };
    let builder = make_vp9_superframe_builder(Vp9FrameSizes::default()).unwrap();
    let harness = Harness::new(vec![depay, builder], None);
    harness.start(&rtp_caps());
    let mut stream = Stream::new();

    for tag in 0..5 {
      push_all(&harness, stream.picture(&[layer(frame(tag, 100), false)], 1200));
    }

    assert!(harness.buffers().is_empty());
    assert_eq!(harness.upstream_events().len(), 1);
  }

  #[test]
  fn an_element_without_sizes_passes_everything_through() {
    gst::init().unwrap();
    crate::pipeline::elements::register().unwrap();
    let element = gst::ElementFactory::make(ELEMENT_NAME).build().unwrap();
    let harness = Harness::new(vec![element], None);
    harness.start(&gst::Caps::builder("video/x-vp9").build());

    harness.push(gst::Buffer::from_slice(vec![1, 2, 3]));

    assert_eq!(bytes(&harness.buffers()[0]), [1, 2, 3]);
  }

  #[test]
  fn caps_and_allocation_queries_are_proxied() {
    gst::init().unwrap();
    let downstream = gst::Caps::builder("video/x-vp9")
      .field("alignment", "super-frame")
      .build();
    let harness = Harness::new(
      vec![make_vp9_superframe_builder(Vp9FrameSizes::default()).unwrap()],
      Some(downstream),
    );
    harness.start(&gst::Caps::builder("video/x-vp9").build());

    let caps = harness.pad().peer_query_caps(None);
    assert_eq!(
      caps.structure(0).unwrap().get::<&str>("alignment").unwrap(),
      "super-frame"
    );
    assert!(!harness
      .pad()
      .peer_query_accept_caps(&gst::Caps::builder("video/x-vp9").field("alignment", "frame").build()));
    let mut query = gst::query::Allocation::new(Some(&gst::Caps::builder("video/x-vp9").build()), false);
    assert!(harness.pad().peer_query(&mut query));
  }

  /// A VP9 keyframe of the given size, encoded by vp9enc. `None` if the encoder is not installed.
  fn encode_keyframe(width: u32, height: u32) -> Option<Vec<u8>> {
    use std::sync::{Arc, Mutex};

    gst::init().unwrap();
    let pipeline = gst::parse::launch(&format!(
      "videotestsrc num-buffers=1 pattern=ball ! video/x-raw,format=I420,width={width},height={height},framerate=30/1 \
       ! vp9enc cpu-used=8 deadline=1 ! fakesink name=sink"
    ))
    .ok()?;
    let frames = Arc::new(Mutex::new(Vec::new()));
    {
      let frames = frames.clone();
      pipeline
        .downcast_ref::<gst::Bin>()
        .unwrap()
        .by_name("sink")
        .unwrap()
        .static_pad("sink")
        .unwrap()
        .add_probe(gst::PadProbeType::BUFFER, move |_, info| {
          if let Some(buffer) = info.buffer() {
            frames.lock().unwrap().push(bytes(buffer));
          }
          gst::PadProbeReturn::Ok
        });
    }
    pipeline.set_state(gst::State::Playing).unwrap();
    let message = pipeline.bus().unwrap().timed_pop_filtered(
      gst::ClockTime::from_seconds(20),
      &[gst::MessageType::Eos, gst::MessageType::Error],
    );
    pipeline.set_state(gst::State::Null).unwrap();
    assert!(
      matches!(message.as_ref().map(|m| m.view()), Some(gst::MessageView::Eos(_))),
      "{message:?}"
    );
    let frames = frames.lock().unwrap();
    assert_eq!(frames.len(), 1);
    Some(frames[0].clone())
  }

  fn make_parser_chain(downstream: &str, with_decoder: bool) -> Option<(Harness, Vp9FrameSizes)> {
    gst::init().unwrap();
    let depay = gst::ElementFactory::make("rtpvp9depay").build().ok()?;
    let parser = gst::ElementFactory::make("vp9parse").build().ok()?;
    let sizes = Vp9FrameSizes::default();
    sizes.install_probe(&depay.static_pad("sink").unwrap());
    let mut elements = vec![depay, make_vp9_superframe_builder(sizes.clone()).unwrap(), parser];
    if with_decoder {
      elements.push(gst::ElementFactory::make("vp9dec").build().ok()?);
    }
    let downstream = gst::Caps::from_str(downstream).unwrap();
    let harness = Harness::new(elements, Some(downstream));
    harness.start(&rtp_caps());
    Some((harness, sizes))
  }

  use std::str::FromStr;

  #[test]
  fn libvpx_frames_in_a_superframe_are_split_again_by_vp9parse() {
    let (Some(base), Some(upper)) = (encode_keyframe(64, 64), encode_keyframe(128, 96)) else {
      return;
    };
    let Some((harness, _)) = make_parser_chain("video/x-vp9,alignment=frame", false) else {
      return;
    };
    let mut stream = Stream::new();

    push_all(
      &harness,
      stream.picture(&[layer(base.clone(), false), layer(upper.clone(), true)], 1100),
    );
    harness.eos();

    let frames: Vec<_> = harness.buffers().iter().map(bytes).collect();
    assert_eq!(frames, [base, upper]);
  }

  #[test]
  fn libvpx_decodes_the_superframe_to_its_last_frame() {
    let (Some(base), Some(upper)) = (encode_keyframe(64, 64), encode_keyframe(128, 96)) else {
      return;
    };
    let Some((harness, _)) = make_parser_chain("video/x-raw", true) else {
      return;
    };
    let mut stream = Stream::new();

    push_all(
      &harness,
      stream.picture(&[layer(base, false), layer(upper, true)], 1100),
    );
    harness.eos();

    let buffers = harness.buffers();
    assert_eq!(buffers.len(), 1, "{:?}", harness.items());
    assert_eq!(buffers[0].size(), 128 * 96 * 3 / 2);
  }

  #[test]
  fn parsed_superframes_are_not_changed_by_the_parser() {
    // vp9parse keeps the superframe whole when downstream wants that alignment.
    let (Some(base), Some(upper)) = (encode_keyframe(64, 64), encode_keyframe(128, 96)) else {
      return;
    };
    let Some((harness, _)) = make_parser_chain("video/x-vp9,alignment=super-frame", false) else {
      return;
    };
    let mut stream = Stream::new();

    push_all(
      &harness,
      stream.picture(&[layer(base.clone(), false), layer(upper.clone(), true)], 1100),
    );
    harness.eos();

    let buffers = harness.buffers();
    assert_eq!(buffers.len(), 1);
    assert_eq!(split_superframe(&bytes(&buffers[0])).unwrap(), [&base[..], &upper[..]]);
  }
}
