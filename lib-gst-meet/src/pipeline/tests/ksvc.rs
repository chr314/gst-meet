//! A K-SVC stream through the real VP9 chains.
//!
//! In a K-SVC delta picture every spatial layer predicts from the same layer of the previous picture: the upper
//! layers start a layer frame with B set, D clear and P set. `rtpvp9depay` with `wait-for-keyframe` takes that for
//! a lost start of picture and waits for a keyframe again, and asks for one with `request-keyframe`.

use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
  },
  time::Duration,
};

use gstreamer::prelude::*;
use gstreamer_rtp::{prelude::*, RTPBuffer};

use super::support::*;
use crate::{
  conference::ReceiveMode,
  pipeline::{
    chain::{self, Chain, ChainSpec},
    codec::Codec,
    CodecName,
  },
  source::MediaType,
};

/// `(keyframe, bytes)` of a buffer.
type Output = (bool, Vec<u8>);

struct Encoded {
  data: Vec<u8>,
  key: bool,
}

/// `count` frames of libvpx, the first of them a keyframe. `None` if the encoder is not installed.
fn encode(width: u32, height: u32, count: u32) -> Option<Vec<Encoded>> {
  init();
  let pipeline = gstreamer::parse::launch(&format!(
    "videotestsrc num-buffers={count} pattern=ball ! \
     video/x-raw,format=I420,width={width},height={height},framerate=30/1 ! \
     vp9enc cpu-used=8 deadline=1 lag-in-frames=0 auto-alt-ref=false keyframe-max-dist=1000 ! fakesink name=sink"
  ))
  .ok()?;
  let frames = Arc::new(Mutex::new(Vec::new()));
  {
    let frames = frames.clone();
    pipeline
      .downcast_ref::<gstreamer::Bin>()
      .unwrap()
      .by_name("sink")
      .unwrap()
      .static_pad("sink")
      .unwrap()
      .add_probe(gstreamer::PadProbeType::BUFFER, move |_, info| {
        if let Some(buffer) = info.buffer() {
          frames.lock().unwrap().push(Encoded {
            data: buffer.map_readable().unwrap().as_slice().to_vec(),
            key: !buffer.flags().contains(gstreamer::BufferFlags::DELTA_UNIT),
          });
        }
        gstreamer::PadProbeReturn::Ok
      });
  }
  pipeline.set_state(gstreamer::State::Playing).unwrap();
  let message = pipeline.bus().unwrap().timed_pop_filtered(
    gstreamer::ClockTime::from_seconds(30),
    &[gstreamer::MessageType::Eos, gstreamer::MessageType::Error],
  );
  pipeline.set_state(gstreamer::State::Null).unwrap();
  assert!(
    matches!(message.as_ref().map(|m| m.view()), Some(gstreamer::MessageView::Eos(_))),
    "{message:?}"
  );
  let frames = std::mem::take(&mut *frames.lock().unwrap());
  assert_eq!(frames.len(), count as usize);
  assert!(frames[0].key && frames[1..].iter().all(|frame| !frame.key));
  Some(frames)
}

/// One layer frame of a picture: a layer of the keyframe or of a delta picture.
struct LayerFrame<'a> {
  data: &'a [u8],
  spatial_layer: u8,
  /// Predicted from an earlier picture (P).
  inter_picture: bool,
  /// Depends on the layer below in the same picture (D).
  inter_layer: bool,
}

/// RTP packets of the stream of a sender, one per layer frame.
struct Packetizer {
  seqnum: u16,
  picture: u16,
  timestamp: u32,
}

impl Packetizer {
  fn new() -> Self {
    Self {
      seqnum: 1000,
      picture: 0,
      timestamp: 90_000,
    }
  }

  /// A picture; the marker is on its last layer frame. Pictures are 33 ms apart.
  fn picture(&mut self, layers: &[LayerFrame]) -> Vec<gstreamer::Buffer> {
    let pts = 1_000_000_000 + u64::from(self.picture) * 33_000_000;
    let packets = layers
      .iter()
      .enumerate()
      .map(|(index, layer)| {
        // I, L and the layer frame in one packet (B and E); P as given, no flexible mode, no scalability structure.
        let first = 0x80 | 0x20 | 0x08 | 0x04 | if layer.inter_picture { 0x40 } else { 0 };
        let descriptor = [
          first,
          0x80 | (self.picture >> 8) as u8 & 0x7f,
          self.picture as u8,
          layer.spatial_layer << 1 | u8::from(layer.inter_layer),
          0,
        ];
        let mut buffer =
          gstreamer::Buffer::new_rtp_with_sizes((descriptor.len() + layer.data.len()) as u32, 0, 0).unwrap();
        {
          let buffer = buffer.get_mut().unwrap();
          let mut rtp = RTPBuffer::from_buffer_writable(buffer).unwrap();
          rtp.set_payload_type(98);
          rtp.set_seq(self.seqnum);
          rtp.set_timestamp(self.timestamp);
          rtp.set_ssrc(0x5eed);
          rtp.set_marker(index == layers.len() - 1);
          let payload = rtp.payload_mut().unwrap();
          payload[..descriptor.len()].copy_from_slice(&descriptor);
          payload[descriptor.len()..].copy_from_slice(layer.data);
        }
        buffer
          .get_mut()
          .unwrap()
          .set_pts(gstreamer::ClockTime::from_nseconds(pts));
        self.seqnum = self.seqnum.wrapping_add(1);
        buffer
      })
      .collect();
    self.picture = (self.picture + 1) & 0x7fff;
    self.timestamp = self.timestamp.wrapping_add(3000);
    packets
  }
}

/// A VP9 chain with a loose pad in front, counting the force-key-unit events that come back through it, and a loose
/// pad behind, collecting what comes out.
struct Rig {
  chain: Chain,
  src: gstreamer::Pad,
  _sink: gstreamer::Pad,
  force_key_units: Arc<AtomicUsize>,
  output: Arc<Mutex<Vec<Output>>>,
}

impl Rig {
  fn new(mode: ReceiveMode) -> Self {
    init();
    let chain = chain::build(
      &ChainSpec {
        codec: &Codec {
          name: CodecName::Vp9,
          pt: 98,
          rtx_pt: None,
          rtcp_fbs: vec![],
        },
        media_type: MediaType::Video,
        mode,
        video_scale: (1280, 720),
        session: 1,
        stream_id: 1,
        generation: 1,
      },
      "chain_ksvc",
    )
    .unwrap();

    let force_key_units = Arc::new(AtomicUsize::new(0));
    let src = {
      let force_key_units = force_key_units.clone();
      gstreamer::Pad::builder(gstreamer::PadDirection::Src)
        .name("test_src")
        .event_function(move |_, _, event| {
          if event.structure().is_some_and(|s| s.name() == "GstForceKeyUnit") {
            force_key_units.fetch_add(1, Ordering::SeqCst);
          }
          true
        })
        .build()
    };
    let output = Arc::new(Mutex::new(Vec::new()));
    let sink = {
      let output = output.clone();
      gstreamer::Pad::builder(gstreamer::PadDirection::Sink)
        .name("test_sink")
        .chain_function(move |_, _, buffer| {
          let key = !buffer.flags().contains(gstreamer::BufferFlags::DELTA_UNIT);
          output
            .lock()
            .unwrap()
            .push((key, buffer.map_readable().unwrap().as_slice().to_vec()));
          Ok(gstreamer::FlowSuccess::Ok)
        })
        .event_function(|_, _, _| true)
        .build()
    };
    src.link(&chain.sink).unwrap();
    chain.src.link(&sink).unwrap();
    sink.set_active(true).unwrap();
    chain.bin.set_state(gstreamer::State::Playing).unwrap();
    src.set_active(true).unwrap();
    assert!(src.push_event(gstreamer::event::StreamStart::new("ksvc")));
    assert!(src.push_event(gstreamer::event::Caps::new(
      &gstreamer::Caps::builder("application/x-rtp")
        .field("media", "video")
        .field("clock-rate", 90_000i32)
        .field("encoding-name", "VP9")
        .field("payload", 98i32)
        .build()
    )));
    assert!(
      src.push_event(gstreamer::event::Segment::new(&gstreamer::FormattedSegment::<
        gstreamer::ClockTime,
      >::new()))
    );
    Self {
      chain,
      src,
      _sink: sink,
      force_key_units,
      output,
    }
  }

  fn push(&self, packets: Vec<gstreamer::Buffer>) {
    for packet in packets {
      assert!(self.src.push(packet).is_ok());
    }
  }

  fn force_key_units(&self) -> usize {
    self.force_key_units.load(Ordering::SeqCst)
  }

  async fn delivered(&self, count: usize) -> Vec<Output> {
    wait_until("the pictures to leave the chain", Duration::from_secs(10), || {
      self.output.lock().unwrap().len() >= count
    })
    .await;
    // Nothing more than that is on its way.
    tokio::time::sleep(Duration::from_millis(150)).await;
    self.output.lock().unwrap().clone()
  }
}

impl Drop for Rig {
  fn drop(&mut self) {
    let _ = self.chain.bin.set_state(gstreamer::State::Null);
  }
}

/// The frames in a superframe (VP9 bitstream specification, Annex B), or the buffer itself if it has no index.
fn frames_of(buffer: &[u8]) -> Vec<&[u8]> {
  let marker = *buffer.last().unwrap();
  if marker & 0xe0 != 0xc0 {
    return vec![buffer];
  }
  let count = usize::from(marker & 7) + 1;
  let size_bytes = usize::from((marker >> 3) & 3) + 1;
  let index = 2 + size_bytes * count;
  assert_eq!(
    buffer[buffer.len() - index],
    marker,
    "the index starts and ends with its marker"
  );
  let mut frames = Vec::new();
  let mut offset = 0;
  for n in 0..count {
    let at = buffer.len() - index + 1 + n * size_bytes;
    let size = buffer[at..at + size_bytes]
      .iter()
      .rev()
      .fold(0usize, |size, byte| size << 8 | usize::from(*byte));
    frames.push(&buffer[offset..offset + size]);
    offset += size;
  }
  assert_eq!(offset, buffer.len() - index);
  frames
}

struct Stream {
  base_key: Vec<u8>,
  upper_key: Vec<u8>,
  base_deltas: Vec<Vec<u8>>,
  upper_deltas: Vec<Vec<u8>>,
}

impl Stream {
  fn encode(deltas: u32) -> Option<Self> {
    let base = encode(64, 64, 1 + deltas)?;
    let upper = encode(128, 96, 1 + deltas)?;
    let split = |frames: Vec<Encoded>| {
      let mut frames = frames.into_iter();
      let key = frames.next().unwrap().data;
      (key, frames.map(|frame| frame.data).collect::<Vec<_>>())
    };
    let (base_key, base_deltas) = split(base);
    let (upper_key, upper_deltas) = split(upper);
    Some(Self {
      base_key,
      upper_key,
      base_deltas,
      upper_deltas,
    })
  }

  /// The keyframe picture: the upper layer is predicted from the base layer.
  fn key_picture(&self, packetizer: &mut Packetizer) -> Vec<gstreamer::Buffer> {
    packetizer.picture(&[
      LayerFrame {
        data: &self.base_key,
        spatial_layer: 0,
        inter_picture: false,
        inter_layer: false,
      },
      LayerFrame {
        data: &self.upper_key,
        spatial_layer: 1,
        inter_picture: false,
        inter_layer: true,
      },
    ])
  }

  /// A delta picture of K-SVC: each layer is predicted from the same layer of the previous picture.
  fn delta_picture(&self, packetizer: &mut Packetizer, n: usize) -> Vec<gstreamer::Buffer> {
    packetizer.picture(&[
      LayerFrame {
        data: &self.base_deltas[n],
        spatial_layer: 0,
        inter_picture: true,
        inter_layer: false,
      },
      LayerFrame {
        data: &self.upper_deltas[n],
        spatial_layer: 1,
        inter_picture: true,
        inter_layer: false,
      },
    ])
  }
}

const DELTAS: usize = 20;

/// The upper layer of each delta picture is all a decoder needs: the base layer frame is superseded by it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_picture_of_a_k_svc_stream_is_delivered_and_no_keyframe_is_requested() {
  let Some(stream) = Stream::encode(DELTAS as u32) else {
    return;
  };
  let rig = Rig::new(ReceiveMode::Encoded);
  let mut packetizer = Packetizer::new();

  rig.push(stream.key_picture(&mut packetizer));
  for n in 0..DELTAS {
    rig.push(stream.delta_picture(&mut packetizer, n));
  }
  let output = rig.delivered(1 + DELTAS).await;

  assert_eq!(output.len(), 1 + DELTAS);
  assert_eq!(rig.force_key_units(), 0);
  // The keyframe picture holds both layers as a superframe, the others only the upper layer.
  assert!(output[0].0);
  assert_eq!(frames_of(&output[0].1), [&stream.base_key[..], &stream.upper_key[..]]);
  for (n, (key, bytes)) in output[1..].iter().enumerate() {
    assert!(!key, "picture {}", n + 1);
    assert_eq!(frames_of(bytes), [&stream.upper_deltas[n][..]], "picture {}", n + 1);
  }
}

/// The superframe builder follows the depayloader through the loss of a picture's last packet, which makes it
/// throw away what it had of the picture, and through the layer frames that come after.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_packet_costs_a_k_svc_stream_its_picture_and_nothing_else() {
  let Some(stream) = Stream::encode(DELTAS as u32) else {
    return;
  };
  let rig = Rig::new(ReceiveMode::Encoded);
  let mut packetizer = Packetizer::new();
  const LOST: usize = 6;

  rig.push(stream.key_picture(&mut packetizer));
  for n in 0..DELTAS {
    let mut packets = stream.delta_picture(&mut packetizer, n);
    if n == LOST {
      packets.pop();
    }
    rig.push(packets);
  }
  let output = rig.delivered(DELTAS).await;

  assert_eq!(output.len(), DELTAS);
  assert_eq!(rig.force_key_units(), 0);
  let expected = (0..DELTAS).filter(|n| *n != LOST).map(|n| &stream.upper_deltas[n][..]);
  for (n, (expected, (key, bytes))) in expected.zip(&output[1..]).enumerate() {
    assert!(!key, "picture {}", n + 1);
    assert_eq!(frames_of(bytes), [expected], "picture {}", n + 1);
  }
}

/// `rtpvp9depay` holds back inter pictures until the first keyframe whatever the properties say.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_reaches_the_consumer_before_the_first_keyframe() {
  let Some(stream) = Stream::encode(DELTAS as u32) else {
    return;
  };
  let rig = Rig::new(ReceiveMode::Encoded);
  let mut packetizer = Packetizer::new();

  // Joined in the middle of a group of pictures.
  for n in 0..3 {
    rig.push(stream.delta_picture(&mut packetizer, n));
  }
  tokio::time::sleep(Duration::from_millis(300)).await;
  assert!(rig.output.lock().unwrap().is_empty());

  rig.push(stream.key_picture(&mut packetizer));
  for n in 3..DELTAS {
    rig.push(stream.delta_picture(&mut packetizer, n));
  }
  let output = rig.delivered(1 + DELTAS - 3).await;
  assert_eq!(output.len(), 1 + DELTAS - 3);
  assert!(output[0].0, "the first thing to arrive is a keyframe");
  assert!(output[1..].iter().all(|(key, _)| !key));
}

/// Without the depayloader's own requests, loss reaches the sender through the chain's probe: in both modes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn packet_loss_requests_a_keyframe_from_a_vp9_chain_in_both_modes() {
  for mode in [ReceiveMode::Encoded, ReceiveMode::Decoded] {
    let rig = Rig::new(mode);
    assert_eq!(rig.force_key_units(), 0);
    // Whether the depayloader takes the event is its business; the probe in front of it has seen it.
    rig.src.push_event(gstreamer::event::CustomDownstream::new(
      gstreamer::Structure::builder("GstRTPPacketLost")
        .field("seqnum", 1u32)
        .build(),
    ));
    assert_eq!(rig.force_key_units(), 1, "{mode:?}");
  }
}
