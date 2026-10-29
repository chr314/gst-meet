use std::{
  collections::HashMap,
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
  pipeline::{
    chain::{self, ChainSpec},
    codec::Codec,
    owner::{self, Owner, StreamPart},
    rtp::poll_sender_reports,
    CodecName, SyncSample, SyncSampleSink,
  },
  source::MediaType,
};

fn codec(name: CodecName, pt: u8) -> Codec {
  Codec {
    name,
    pt,
    rtx_pt: None,
    rtcp_fbs: vec![],
  }
}

fn chain_for(name: CodecName, media_type: MediaType, mode: ReceiveMode) -> anyhow::Result<chain::Chain> {
  init();
  chain::build(
    &ChainSpec {
      codec: &codec(name, 96),
      media_type,
      mode,
      video_scale: (1280, 720),
      session: 1,
      stream_id: 7,
      generation: 12,
    },
    "chain_test",
  )
}

/// The factory names of the elements between the chain's pads, in data flow order.
fn flow(chain: &chain::Chain) -> Vec<String> {
  let mut names = Vec::new();
  let mut pad = chain.depayloader_sink.clone();
  loop {
    // The end of the chain is the internal pad of its ghost pad, or the bin itself, which has no factory.
    let Some(factory) = pad.parent_element().and_then(|element| element.factory()) else {
      return names;
    };
    let element = pad.parent_element().unwrap();
    names.push(factory.name().to_string());
    let Some(next) = element.static_pad("src").and_then(|src| src.peer()) else {
      return names;
    };
    pad = next;
  }
}

fn child(chain: &chain::Chain, factory: &str) -> gstreamer::Element {
  chain
    .bin
    .children()
    .into_iter()
    .find(|child| child.factory().is_some_and(|f| f.name() == factory))
    .unwrap_or_else(|| panic!("no {factory}"))
}

fn caps_of(chain: &chain::Chain, factory: &str) -> String {
  child(chain, factory).property::<gstreamer::Caps>("caps").to_string()
}

#[test]
fn encoded_chains_only_depayload_and_parse() {
  let chain = chain_for(CodecName::Opus, MediaType::Audio, ReceiveMode::Encoded).unwrap();
  assert_eq!(flow(&chain), ["rtpopusdepay", "queue"]);
  assert!(!child(&chain, "rtpopusdepay").has_property("request-keyframe"));

  let chain = chain_for(CodecName::Vp8, MediaType::Video, ReceiveMode::Encoded).unwrap();
  assert_eq!(flow(&chain), ["rtpvp8depay", "queue"]);

  let chain = chain_for(CodecName::H264, MediaType::Video, ReceiveMode::Encoded).unwrap();
  assert_eq!(flow(&chain), ["rtph264depay", "h264parse", "capsfilter", "queue"]);
  assert_eq!(
    caps_of(&chain, "capsfilter"),
    "video/x-h264, stream-format=(string)avc, alignment=(string)au"
  );
  assert_eq!(child(&chain, "h264parse").property::<i32>("config-interval"), -1);

  // The superframe builder stands between the depayloader and the parser.
  let chain = chain_for(CodecName::Vp9, MediaType::Video, ReceiveMode::Encoded).unwrap();
  let names = flow(&chain);
  assert_eq!(names[0], "rtpvp9depay");
  assert_eq!(&names[2..], ["vp9parse", "capsfilter", "queue"]);
  assert_eq!(
    caps_of(&chain, "capsfilter"),
    "video/x-vp9, alignment=(string)super-frame"
  );
}

#[test]
fn the_av1_chain_needs_the_av1_depayloader() {
  init();
  let available = ["rtpav1depay", "av1parse"]
    .iter()
    .all(|factory| gstreamer::ElementFactory::find(factory).is_some());
  let built = chain_for(CodecName::Av1, MediaType::Video, ReceiveMode::Encoded);
  assert_eq!(built.is_ok(), available, "{:?}", built.err());
  if let Ok(chain) = built {
    let names = flow(&chain);
    assert_eq!(
      (names[0].as_str(), names.last().unwrap().as_str()),
      ("rtpav1depay", "queue")
    );
    assert_eq!(
      caps_of(&chain, "capsfilter"),
      "video/x-av1, stream-format=(string)obu-stream, alignment=(string)tu"
    );
  }
}

#[test]
fn the_encoded_queue_never_drops() {
  let chain = chain_for(CodecName::Opus, MediaType::Audio, ReceiveMode::Encoded).unwrap();
  let queue = child(&chain, "queue");
  assert_eq!(queue.property::<u32>("max-size-buffers"), 0);
  assert_eq!(queue.property::<u32>("max-size-bytes"), 0);
  assert_eq!(queue.property::<u64>("max-size-time"), 2_000_000_000);
  let leaky = queue.property_value("leaky");
  assert_eq!(glib::EnumValue::from_value(&leaky).unwrap().1.nick(), "no");
}

#[test]
fn decoded_chains_decode_and_scale() {
  let chain = chain_for(CodecName::Opus, MediaType::Audio, ReceiveMode::Decoded).unwrap();
  assert_eq!(flow(&chain), ["rtpopusdepay", "queue", "opusdec", "queue"]);

  let chain = chain_for(CodecName::Vp8, MediaType::Video, ReceiveMode::Decoded).unwrap();
  assert_eq!(
    flow(&chain),
    [
      "rtpvp8depay",
      "queue",
      "vp8dec",
      "queue",
      "videoscale",
      "capsfilter",
      "videoconvert",
      "queue"
    ]
  );
  assert_eq!(
    caps_of(&chain, "capsfilter"),
    "video/x-raw, width=(int)[ 1, 1280 ], height=(int)[ 1, 720 ], pixel-aspect-ratio=(fraction)1/1"
  );
  assert!(child(&chain, "vp8dec").property::<bool>("automatic-request-sync-points"));
}

#[test]
fn video_depayloaders_ask_for_keyframes_after_loss_in_both_modes_except_vp9() {
  for mode in [ReceiveMode::Encoded, ReceiveMode::Decoded] {
    let chain = chain_for(CodecName::Vp8, MediaType::Video, mode).unwrap();
    let depayloader = child(&chain, "rtpvp8depay");
    assert!(depayloader.property::<bool>("request-keyframe"), "{mode:?}");
    assert!(depayloader.property::<bool>("wait-for-keyframe"), "{mode:?}");

    // rtpvp9depay would wait for a keyframe again at every upper layer of a K-SVC picture (see tests/ksvc.rs).
    let chain = chain_for(CodecName::Vp9, MediaType::Video, mode).unwrap();
    let depayloader = child(&chain, "rtpvp9depay");
    assert!(!depayloader.property::<bool>("request-keyframe"), "{mode:?}");
    assert!(!depayloader.property::<bool>("wait-for-keyframe"), "{mode:?}");
  }
}

#[test]
fn chains_are_marked_with_their_owner_from_the_start() {
  let chain = chain_for(CodecName::Opus, MediaType::Audio, ReceiveMode::Encoded).unwrap();
  assert_eq!(
    owner::of(&chain.bin),
    Some(Owner::Stream {
      session: 1,
      stream_id: 7,
      part: StreamPart::Chain(12)
    })
  );
  assert_eq!(chain.bin.name(), "chain_test");
  assert_eq!(chain.generation, 12);
}

fn push_sticky_events(src: &gstreamer::Pad) {
  assert!(src.push_event(gstreamer::event::StreamStart::new("test")));
  assert!(src.push_event(gstreamer::event::Caps::new(
    &gstreamer::Caps::builder("application/x-test").build()
  )));
  assert!(
    src.push_event(gstreamer::event::Segment::new(&gstreamer::FormattedSegment::<
      gstreamer::ClockTime,
    >::new()))
  );
}

#[test]
fn packet_loss_requests_a_keyframe_at_most_once_a_second() {
  init();
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
  let pipeline = gstreamer::Pipeline::new();
  let _null_on_drop = NullOnDrop(pipeline.clone());
  let identity = gstreamer::ElementFactory::make("identity").build().unwrap();
  let fakesink = gstreamer::ElementFactory::make("fakesink").build().unwrap();
  pipeline.add_many([&identity, &fakesink]).unwrap();
  identity.link(&fakesink).unwrap();
  chain::install_packet_lost_keyframe_probe(&identity.static_pad("sink").unwrap());
  src.link(&identity.static_pad("sink").unwrap()).unwrap();
  pipeline.set_state(gstreamer::State::Playing).unwrap();
  src.set_active(true).unwrap();
  push_sticky_events(&src);

  let lost = || {
    gstreamer::event::CustomDownstream::new(
      gstreamer::Structure::builder("GstRTPPacketLost")
        .field("seqnum", 1u32)
        .build(),
    )
  };
  assert!(src.push_event(gstreamer::event::CustomDownstream::new(
    gstreamer::Structure::new_empty("Other")
  )));
  assert_eq!(force_key_units.load(Ordering::SeqCst), 0);

  for _ in 0..3 {
    src.push_event(lost());
  }
  assert_eq!(force_key_units.load(Ordering::SeqCst), 1);
  std::thread::sleep(Duration::from_millis(1100));
  src.push_event(lost());
  assert_eq!(force_key_units.load(Ordering::SeqCst), 2);
}

#[test]
fn an_encoded_video_chain_requests_keyframes_on_packet_loss() {
  let chain = chain_for(CodecName::Vp8, MediaType::Video, ReceiveMode::Encoded).unwrap();
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
  src.link(&chain.sink).unwrap();
  chain.bin.set_state(gstreamer::State::Playing).unwrap();
  src.set_active(true).unwrap();
  push_sticky_events(&src);
  src.push_event(gstreamer::event::CustomDownstream::new(
    gstreamer::Structure::builder("GstRTPPacketLost")
      .field("seqnum", 1u32)
      .build(),
  ));
  assert!(force_key_units.load(Ordering::SeqCst) >= 1);
  chain.bin.set_state(gstreamer::State::Null).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rtp_mapping_samples_come_once_a_second() {
  init();
  let pipeline = gstreamer::parse::launch(
    "audiotestsrc is-live=true ! audioconvert ! audioresample ! opusenc ! rtpopuspay name=pay pt=111 ssrc=1234",
  )
  .unwrap()
  .downcast::<gstreamer::Pipeline>()
  .unwrap();
  let _null_on_drop = NullOnDrop(pipeline.clone());
  let chain = chain_for(CodecName::Opus, MediaType::Audio, ReceiveMode::Encoded).unwrap();
  let sink = gstreamer::ElementFactory::make("fakesink")
    .property("sync", false)
    .property("async", false)
    .build()
    .unwrap();
  pipeline
    .add_many([chain.bin.upcast_ref::<gstreamer::Element>(), &sink])
    .unwrap();
  chain.src.link(&sink.static_pad("sink").unwrap()).unwrap();
  pipeline
    .by_name("pay")
    .unwrap()
    .static_pad("src")
    .unwrap()
    .link(&chain.sink)
    .unwrap();

  let samples = Arc::new(Mutex::new(Vec::new()));
  let sample_sink = SyncSampleSink::default();
  {
    let samples = samples.clone();
    sample_sink.set(Arc::new(move |sample| samples.lock().unwrap().push(sample)));
  }
  chain::install_rtp_mapping_probe(
    &chain.depayloader_sink,
    5,
    "p1".into(),
    MediaType::Audio,
    1234,
    sample_sink,
  );

  pipeline.set_state(gstreamer::State::Playing).unwrap();
  tokio::time::sleep(Duration::from_millis(3300)).await;
  pipeline.set_state(gstreamer::State::Null).unwrap();

  let samples = samples.lock().unwrap();
  assert!(
    (3..=4).contains(&samples.len()),
    "{} samples: {:?}",
    samples.len(),
    samples
  );
  let mut previous: Option<(u32, u64)> = None;
  for sample in samples.iter() {
    let SyncSample::RtpMapping {
      stream_id,
      participant_id,
      media_type,
      ssrc,
      rtp_time,
      pts_running_time_ns,
    } = sample
    else {
      panic!("unexpected sample {sample:?}");
    };
    assert_eq!(
      (*stream_id, participant_id.as_str(), *media_type, *ssrc),
      (5, "p1", MediaType::Audio, 1234)
    );
    if let Some((previous_rtp, previous_ns)) = previous {
      let elapsed_ns = pts_running_time_ns - previous_ns;
      assert!(
        (900_000_000..1_300_000_000).contains(&elapsed_ns),
        "{elapsed_ns}ns between samples"
      );
      let expected = elapsed_ns as f64 * 48_000.0 / 1e9;
      let actual = rtp_time.wrapping_sub(previous_rtp) as f64;
      assert!(
        (actual - expected).abs() < 2400.0,
        "rtp advanced {actual}, pts advanced {expected}"
      );
    }
    previous = Some((*rtp_time, *pts_running_time_ns));
  }
}

/// Two rtpbins back to back: the receiving one must report the sender's reports, each once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_reports_are_reported_once_each() {
  init();
  let pipeline = gstreamer::parse::launch(
    "audiotestsrc is-live=true ! audioconvert ! audioresample ! opusenc ! rtpopuspay pt=111 ssrc=1234 ! \
       rtpbin_tx.send_rtp_sink_0 \
     rtpbin name=rtpbin_tx \
     rtpbin name=rtpbin_rx \
     rtpbin_tx.send_rtp_src_0 ! rtpbin_rx.recv_rtp_sink_0 \
     rtpbin_tx.send_rtcp_src_0 ! rtpbin_rx.recv_rtcp_sink_0",
  )
  .unwrap()
  .downcast::<gstreamer::Pipeline>()
  .unwrap();
  let _null_on_drop = NullOnDrop(pipeline.clone());
  let tx = pipeline.by_name("rtpbin_tx").unwrap();
  let rx = pipeline.by_name("rtpbin_rx").unwrap();

  rx.connect("request-pt-map", false, |_| {
    Some(
      gstreamer::Caps::builder("application/x-rtp")
        .field("media", "audio")
        .field("clock-rate", 48000)
        .field("encoding-name", "OPUS")
        .field("payload", 111)
        .build()
        .to_value(),
    )
  });
  let pipeline_weak = pipeline.downgrade();
  rx.connect_pad_added(move |_, pad| {
    if pad.direction() == gstreamer::PadDirection::Src {
      if let Some(pipeline) = pipeline_weak.upgrade() {
        let sink = gstreamer::ElementFactory::make("fakesink")
          .property("sync", false)
          .build()
          .unwrap();
        pipeline.add(&sink).unwrap();
        sink.sync_state_with_parent().unwrap();
        pad.link(&sink.static_pad("sink").unwrap()).unwrap();
      }
    }
  });
  // Don't wait the default 5 seconds for the first sender report.
  let session = tx
    .emit_by_name::<Option<glib::Object>>("get-internal-session", &[&0u32])
    .unwrap();
  session.set_property("rtcp-min-interval", 200_000_000u64);

  pipeline.set_state(gstreamer::State::Playing).unwrap();

  let mut seen = HashMap::new();
  let mut reports = Vec::new();
  let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
  while reports.len() < 2 && tokio::time::Instant::now() < deadline {
    tokio::time::sleep(Duration::from_millis(250)).await;
    reports.extend(poll_sender_reports(&rx, &mut seen));
  }
  assert!(
    reports.len() >= 2,
    "only {} sender report(s): {:?}",
    reports.len(),
    reports
  );
  let ntp_times: Vec<u64> = reports
    .iter()
    .map(|report| match report {
      SyncSample::SenderReport { ssrc, ntp_time, .. } => {
        assert_eq!(*ssrc, 1234);
        *ntp_time
      },
      other => panic!("unexpected sample {other:?}"),
    })
    .collect();
  assert!(ntp_times[0] != 0 && ntp_times[1] > ntp_times[0], "{ntp_times:?}");
  // A report is only returned once: whatever a poll right after returns must be newer.
  for report in poll_sender_reports(&rx, &mut seen) {
    let SyncSample::SenderReport { ntp_time, .. } = report else {
      panic!("unexpected sample");
    };
    assert!(ntp_time > *ntp_times.last().unwrap());
  }
}

#[test]
fn receive_support_follows_the_elements_that_are_installed() {
  init();
  let present = |factory: &str| gstreamer::ElementFactory::find(factory).is_some();
  for (name, depay, decoder, parser) in [
    (CodecName::Opus, "rtpopusdepay", "opusdec", None),
    (CodecName::Vp8, "rtpvp8depay", "vp8dec", None),
    (CodecName::Vp9, "rtpvp9depay", "vp9dec", Some("vp9parse")),
    (CodecName::H264, "rtph264depay", "avdec_h264", Some("h264parse")),
    (CodecName::Av1, "rtpav1depay", "av1dec", Some("av1parse")),
  ] {
    assert_eq!(
      name.receive_supported(ReceiveMode::Encoded),
      present(depay) && parser.is_none_or(present),
      "{name:?} encoded"
    );
    assert_eq!(
      name.receive_supported(ReceiveMode::Decoded),
      present(depay) && (present(decoder) || (name == CodecName::Av1 && present("dav1ddec"))),
      "{name:?} decoded"
    );
  }
  assert!(CodecName::parse_video("vp8") == Some(CodecName::Vp8));
  assert!(CodecName::parse_video("opus").is_none());
}
