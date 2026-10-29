use std::{
  sync::Mutex,
  time::{Duration, Instant},
};

use anyhow::{Context, Result};
use gstreamer::{prelude::*, Bin, ClockTime, GhostPad, PadProbeReturn, PadProbeType};
use gstreamer_rtp::RTPBuffer;
use tracing::debug;

use super::{
  codec::{Codec, CodecName},
  elements::{make_av1_tu_aggregator, make_vp9_superframe_builder, Vp9FrameSizes},
  owner::{self, Owner, StreamPart},
  rtp::connect_request_extension,
  types::{StreamId, SyncSample, SyncSampleSink},
};
use crate::{conference::ReceiveMode, source::MediaType};

const KEYFRAME_REQUEST_MIN_INTERVAL: Duration = Duration::from_secs(1);

const RTP_MAPPING_INTERVAL: Duration = Duration::from_secs(1);

/// Never drops: when the consumer stalls for longer, the stream blocks.
const ENCODED_QUEUE_MAX_TIME_NS: u64 = 2_000_000_000;

pub(super) struct ChainSpec<'a> {
  pub(super) codec: &'a Codec,
  pub(super) media_type: MediaType,
  pub(super) mode: ReceiveMode,
  pub(super) video_scale: (u16, u16),
  pub(super) session: u64,
  pub(super) stream_id: StreamId,
  /// Unique among the chains of the session: the errors of a chain are attributed to it, not to the stream.
  pub(super) generation: u64,
}

/// A bin with a ghost `sink` and `src` pad. It is marked with the stream's [`Owner`] from the start.
#[derive(Clone)]
pub(super) struct Chain {
  pub(super) generation: u64,
  pub(super) bin: Bin,
  pub(super) sink: gstreamer::Pad,
  pub(super) src: gstreamer::Pad,
  pub(super) depayloader_sink: gstreamer::Pad,
}

pub(super) fn build(spec: &ChainSpec, name: &str) -> Result<Chain> {
  let bin = Bin::with_name(name);
  owner::mark(
    &bin,
    Owner::Stream {
      session: spec.session,
      stream_id: spec.stream_id,
      part: StreamPart::Chain(spec.generation),
    },
  );

  let codec = spec.codec;
  let depayloader = gstreamer::ElementFactory::make(codec.depayloader_name())
    .build()
    .with_context(|| format!("failed to create {}", codec.depayloader_name()))?;
  depayloader.set_property("auto-header-extension", false);
  connect_request_extension(&depayloader, "depayloader");
  bin.add(&depayloader).context("failed to add depayloader to chain")?;

  let tail = match spec.mode {
    ReceiveMode::Decoded => build_decoded_chain(&bin, &depayloader, spec)?,
    ReceiveMode::Encoded => build_encoded_chain(&bin, &depayloader, codec)?,
  };

  let depayloader_sink = depayloader.static_pad("sink").context("depayloader has no sink pad")?;
  let sink = GhostPad::builder_with_target(&depayloader_sink)?.name("sink").build();
  bin.add_pad(&sink)?;
  let src = GhostPad::builder_with_target(&tail.static_pad("src").context("last element has no src pad")?)?
    .name("src")
    .build();
  bin.add_pad(&src)?;

  if spec.media_type.is_video() {
    configure_keyframe_requests(&depayloader, codec.name);
    if spec.mode == ReceiveMode::Encoded || !depayloader_requests_keyframes(codec.name) {
      install_packet_lost_keyframe_probe(&depayloader_sink);
    }
  }

  Ok(Chain {
    generation: spec.generation,
    bin,
    sink: sink.upcast(),
    src: src.upcast(),
    depayloader_sink,
  })
}

fn make(factory: &str) -> Result<gstreamer::Element> {
  gstreamer::ElementFactory::make(factory)
    .build()
    .with_context(|| format!("failed to create {factory}"))
}

/// Adds `elements` to `bin` and links them in order after `head`; returns the last one.
fn append(bin: &Bin, head: &gstreamer::Element, elements: &[&gstreamer::Element]) -> Result<gstreamer::Element> {
  bin
    .add_many(elements.iter().copied())
    .context("failed to add elements to chain")?;
  let mut previous = head.clone();
  for element in elements {
    previous
      .link(*element)
      .with_context(|| format!("failed to link {} to {}", previous.name(), element.name()))?;
    previous = (*element).clone();
  }
  Ok(previous)
}

fn build_decoded_chain(bin: &Bin, depayloader: &gstreamer::Element, spec: &ChainSpec) -> Result<gstreamer::Element> {
  let mut head = depayloader.clone();
  if spec.codec.name == CodecName::H264 {
    let h264parse = gstreamer::ElementFactory::make("h264parse")
      .property("config-interval", -1i32)
      .build()
      .context("failed to create h264parse")?;
    head = append(bin, &head, &[&h264parse])?;
  }

  let pre_decoder_queue = make("queue")?;
  let decoder = make(spec.codec.decoder_name())?;
  if spec.media_type.is_video() {
    decoder.set_property("automatic-request-sync-points", true);
    decoder.set_property_from_str("automatic-request-sync-point-flags", "corrupt-output");
    decoder.set_property("min-force-key-unit-interval", 2_000_000_000u64);
  }
  let post_decoder_queue = {
    let mut queue = gstreamer::ElementFactory::make("queue");
    if spec.media_type.is_video() {
      queue = queue
        .property("max-size-buffers", 10u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .property_from_str("leaky", "downstream");
    }
    queue.build().context("failed to create queue")?
  };
  let tail = append(bin, &head, &[&pre_decoder_queue, &decoder, &post_decoder_queue])?;

  if spec.media_type.is_audio() {
    return Ok(tail);
  }

  let (width, height) = spec.video_scale;
  let videoscale = make("videoscale")?;
  let capsfilter = gstreamer::ElementFactory::make("capsfilter")
    .property_from_str(
      "caps",
      &format!("video/x-raw, width=(int)[1,{width}], height=(int)[1,{height}], pixel-aspect-ratio=1/1"),
    )
    .build()
    .context("failed to create capsfilter")?;
  let videoconvert = make("videoconvert")?;
  let post_videoconvert_queue = gstreamer::ElementFactory::make("queue")
    .property("max-size-buffers", 10u32)
    .property("max-size-bytes", 0u32)
    .property("max-size-time", 0u64)
    .property_from_str("leaky", "downstream")
    .build()
    .context("failed to create queue")?;
  append(
    bin,
    &tail,
    &[&videoscale, &capsfilter, &videoconvert, &post_videoconvert_queue],
  )
}

/// Nothing is decoded and the queue does not drop. Parsers only where the muxer needs them: H264 as `avc`/`au`,
/// VP9 as super frames, AV1 as `obu-stream`/`tu`.
fn build_encoded_chain(bin: &Bin, depayloader: &gstreamer::Element, codec: &Codec) -> Result<gstreamer::Element> {
  let output_queue = gstreamer::ElementFactory::make("queue")
    .property("max-size-buffers", 0u32)
    .property("max-size-bytes", 0u32)
    .property("max-size-time", ENCODED_QUEUE_MAX_TIME_NS)
    .property_from_str("leaky", "no")
    .build()
    .context("failed to create queue")?;

  let capsfilter = |caps: gstreamer::Caps| -> Result<gstreamer::Element> {
    gstreamer::ElementFactory::make("capsfilter")
      .property("caps", &caps)
      .build()
      .context("failed to create capsfilter")
  };

  match codec.name {
    CodecName::Opus | CodecName::Vp8 => append(bin, depayloader, &[&output_queue]),
    CodecName::H264 => {
      let parse = gstreamer::ElementFactory::make("h264parse")
        .property("config-interval", -1i32)
        .build()
        .context("failed to create h264parse")?;
      let caps = capsfilter(
        gstreamer::Caps::builder("video/x-h264")
          .field("stream-format", "avc")
          .field("alignment", "au")
          .build(),
      )?;
      append(bin, depayloader, &[&parse, &caps, &output_queue])
    },
    CodecName::Vp9 => {
      let sizes = Vp9FrameSizes::default();
      sizes.install_probe(&depayloader.static_pad("sink").context("depayloader has no sink pad")?);
      let superframes = make_vp9_superframe_builder(sizes)?;
      let parse = make("vp9parse")?;
      let caps = capsfilter(
        gstreamer::Caps::builder("video/x-vp9")
          .field("alignment", "super-frame")
          .build(),
      )?;
      append(bin, depayloader, &[&superframes, &parse, &caps, &output_queue])
    },
    CodecName::Av1 => {
      let aggregator = make_av1_tu_aggregator()?;
      let parse = make("av1parse")?;
      let caps = capsfilter(
        gstreamer::Caps::builder("video/x-av1")
          .field("stream-format", "obu-stream")
          .field("alignment", "tu")
          .build(),
      )?;
      append(bin, depayloader, &[&aggregator, &parse, &caps, &output_queue])
    },
  }
}

/// Whether the depayloader of this codec is left to ask for keyframes after loss and to wait for one.
///
/// `rtpvp9depay` is not. It holds back inter pictures until the first keyframe on its own, but with
/// `wait-for-keyframe` it waits for a keyframe again whenever a packet that starts a layer frame with P set (B set,
/// and D clear) arrives while the previous layer frame is still being assembled. That is what each upper spatial
/// layer of a K-SVC delta picture looks like, so it would deliver only the keyframes, and with `request-keyframe`
/// ask for a new one with every picture. Loss reaches the sender through the packet-lost probe instead.
///
/// The others re-arm only on loss: `rtpvp8depay` and `rtph264depay` on a discontinuity or an incomplete frame,
/// `rtpav1depay` until the first start of a coded video sequence and after it was reset.
fn depayloader_requests_keyframes(codec: CodecName) -> bool {
  codec != CodecName::Vp9
}

/// A depayloader that can ask for keyframes after loss and wait for one does so itself.
fn configure_keyframe_requests(depayloader: &gstreamer::Element, codec: CodecName) {
  if !depayloader_requests_keyframes(codec) {
    return;
  }
  for name in ["wait-for-keyframe", "request-keyframe"] {
    if depayloader.has_property_with_type(name, glib::Type::BOOL) {
      depayloader.set_property(name, true);
    }
  }
}

/// Makes rtpbin send a PLI/FIR.
pub(super) fn force_key_unit_event() -> gstreamer::Event {
  let fku = gstreamer::Structure::builder("GstForceKeyUnit")
    .field("running-time", gstreamer::ClockTime::NONE)
    .field("all-headers", true)
    .field("count", 0u32)
    .build();
  gstreamer::event::CustomUpstream::builder(fku).build()
}

/// Without a decoder nothing requests keyframes after packet loss, and codecs such as AV1 lose whole frames
/// silently. rtpbin reports a gap as a `GstRTPPacketLost` event; this turns it into a keyframe request.
pub(super) fn install_packet_lost_keyframe_probe(depay_sink: &gstreamer::Pad) {
  let last_request: Mutex<Option<Instant>> = Mutex::new(None);
  depay_sink.add_probe(PadProbeType::EVENT_DOWNSTREAM, move |pad, info| {
    let is_packet_lost = info.event().is_some_and(|event| {
      event.type_() == gstreamer::EventType::CustomDownstream
        && event.structure().is_some_and(|s| s.name() == "GstRTPPacketLost")
    });
    if is_packet_lost {
      let due = {
        let mut last_request = last_request.lock().unwrap_or_else(|e| e.into_inner());
        let due = last_request.is_none_or(|at| at.elapsed() >= KEYFRAME_REQUEST_MIN_INTERVAL);
        if due {
          *last_request = Some(Instant::now());
        }
        due
      };
      if due {
        debug!("packet lost on {} — requesting a keyframe", pad.name());
        pad.push_event(force_key_unit_event());
      }
    }
    PadProbeReturn::Ok
  });
}

/// Reports how RTP timestamps relate to running time, once per second.
pub(super) fn install_rtp_mapping_probe(
  depayloader_sink: &gstreamer::Pad,
  stream_id: StreamId,
  participant_id: String,
  media_type: MediaType,
  ssrc: u32,
  sink: SyncSampleSink,
) {
  let last_sample: Mutex<Option<Instant>> = Mutex::new(None);
  depayloader_sink.add_probe(PadProbeType::BUFFER | PadProbeType::BUFFER_LIST, move |pad, info| {
    {
      let last_sample = last_sample.lock().unwrap_or_else(|e| e.into_inner());
      if last_sample.is_some_and(|at| at.elapsed() < RTP_MAPPING_INTERVAL) {
        return PadProbeReturn::Ok;
      }
    }

    let first_buffer = match (info.buffer(), info.buffer_list()) {
      (Some(buffer), _) => Some(buffer.as_ref()),
      (None, Some(list)) => list.get(0),
      (None, None) => None,
    };
    let sample = first_buffer.and_then(|buffer| {
      let pts = buffer.pts()?;
      let rtp_time = RTPBuffer::from_buffer_readable(buffer).ok()?.timestamp();
      // The depayloader's sink pad has rtpbin's TIME segment, the same running time base as downstream.
      let segment_event = pad.sticky_event::<gstreamer::event::Segment>(0)?;
      let running_time = segment_event
        .segment()
        .downcast_ref::<ClockTime>()?
        .to_running_time(pts)?;
      Some((rtp_time, running_time.nseconds()))
    });

    if let Some((rtp_time, pts_running_time_ns)) = sample {
      *last_sample.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
      sink.emit(|| SyncSample::RtpMapping {
        stream_id,
        participant_id: participant_id.clone(),
        media_type,
        ssrc,
        rtp_time,
        pts_running_time_ns,
      });
    }
    PadProbeReturn::Ok
  });
}
