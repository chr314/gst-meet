use std::{
  collections::{BTreeMap, HashMap},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, RwLock,
  },
  time::Duration,
};

use anyhow::{Context, Result};
use glib::prelude::ToValue as _;
use gstreamer::prelude::*;
use gstreamer_rtp::{prelude::RTPHeaderExtensionExt as _, RTPHeaderExtension};
use tracing::{debug, error, warn};

use super::{
  codec::{Codec, RTP_HDREXT_SSRC_AUDIO_LEVEL, RTP_HDREXT_TRANSPORT_CC},
  offer::HdrextIds,
  types::{SyncSample, SyncSampleSink},
};
use crate::source::MediaType;

const SENDER_REPORT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How long an SSRC waits to be cleared between two packets before it is cleared regardless.
const CLEAR_SSRC_FALLBACK: Duration = Duration::from_secs(1);

pub(super) fn build_rtpbin(latency: u32) -> Result<gstreamer::Element> {
  Ok(
    gstreamer::ElementFactory::make("rtpbin")
      .name("rtpbin")
      .property_from_str("rtp-profile", "savpf")
      .property("autoremove", false)
      .property("do-lost", true)
      .property("do-sync-event", true)
      .property("latency", latency)
      .build()?,
  )
}

pub(super) fn configure_twcc_feedback(rtpbin: &gstreamer::Element, session_id: u32) -> Result<()> {
  let rtpsession = rtpbin
    .emit_by_name::<Option<gstreamer::Element>>("get-session", &[&session_id])
    .context("rtpbin get-session returned null")?;
  let internal = rtpsession.property::<glib::Object>("internal-session");
  internal.set_property("twcc-feedback-interval", 100_000_000u64);
  internal.set_property("rtcp-reduced-size", true);
  debug!(
    "twcc feedback configured on session {} (interval=100ms, reduced-size RTCP)",
    session_id
  );
  Ok(())
}

/// Payloaders and depayloaders get their RTP header extensions on request, never automatically.
pub(super) fn connect_request_extension(element: &gstreamer::Element, description: &str) {
  let desc = description.to_owned();
  element.connect("request-extension", false, move |values| {
    let f = || {
      let ext_id: u32 = values[1].get()?;
      let ext_uri: String = values[2].get()?;
      debug!("{} requested extension: {} {}", desc, ext_id, ext_uri);
      let hdrext = RTPHeaderExtension::create_from_uri(&ext_uri).context("failed to create hdrext")?;
      hdrext.set_id(ext_id);
      Ok::<_, anyhow::Error>(hdrext)
    };
    match f() {
      Ok(hdrext) => Some(hdrext.to_value()),
      Err(e) => {
        warn!("request-extension: {:?}", e);
        None
      },
    }
  });
}

pub(super) fn connect_request_pt_map(rtpbin: &gstreamer::Element, codecs: Vec<Codec>, hdrexts: HdrextIds) {
  rtpbin.connect("request-pt-map", false, move |values| {
    let f = || {
      debug!("rtpbin request-pt-map {:?}", values);
      let pt = values[2].get::<u32>()? as u8;
      let mut caps = gstreamer::Caps::builder("application/x-rtp").field("payload", pt as i32);
      for codec in codecs.iter() {
        if codec.is(pt) {
          if codec.is_audio() {
            caps = caps
              .field("media", "audio")
              .field("encoding-name", "OPUS")
              .field("clock-rate", 48000);
            if let Some(hdrext) = hdrexts.audio_level {
              caps = caps.field(format!("extmap-{}", hdrext), RTP_HDREXT_SSRC_AUDIO_LEVEL);
            }
            if let Some(hdrext) = hdrexts.transport_cc_audio {
              caps = caps.field(format!("extmap-{}", hdrext), RTP_HDREXT_TRANSPORT_CC);
            }
          }
          else {
            caps = caps
              .field("media", "video")
              .field("clock-rate", 90000)
              .field("encoding-name", codec.encoding_name());
            for fb in &codec.rtcp_fbs {
              match (fb.type_.as_str(), fb.subtype.as_deref()) {
                ("nack", Some("pli")) => caps = caps.field("rtcp-fb-nack-pli", true),
                ("nack", None) => caps = caps.field("rtcp-fb-nack", true),
                ("ccm", Some("fir")) => caps = caps.field("rtcp-fb-ccm-fir", true),
                ("transport-cc", _) => caps = caps.field("rtcp-fb-transport-cc", true),
                ("goog-remb", _) => caps = caps.field("rtcp-fb-goog-remb", true),
                _ => {},
              }
            }
            if let Some(hdrext) = hdrexts.transport_cc_video {
              caps = caps.field(format!("extmap-{}", hdrext), RTP_HDREXT_TRANSPORT_CC);
            }
          }
          return Ok::<_, anyhow::Error>(Some(caps.build()));
        }
        else if codec.is_rtx(pt) {
          caps = caps
            .field("media", "video")
            .field("clock-rate", 90000)
            .field("encoding-name", "RTX")
            .field("apt", codec.pt);

          if let Some(hdrext) = hdrexts.transport_cc_video {
            caps = caps.field(format!("extmap-{}", hdrext), RTP_HDREXT_TRANSPORT_CC);
          }

          return Ok(Some(caps.build()));
        }
      }

      debug!("ignoring payload type {} (not negotiated)", pt);
      Ok(None)
    };
    match f() {
      Ok(Some(caps)) => {
        debug!("mapped pt to caps: {:?}", caps);
        Some(caps.to_value())
      },
      Ok(None) => Some(Option::<gstreamer::Caps>::None.to_value()),
      Err(e) => {
        error!("handling request-pt-map: {:?}", e);
        Some(Option::<gstreamer::Caps>::None.to_value())
      },
    }
  });
}

/// What `new-jitterbuffer` needs to know about a source before its stream exists. Filled by the session when
/// sources are announced.
#[derive(Clone, Default)]
pub(super) struct SourceKinds(Arc<RwLock<HashMap<u32, MediaType>>>);

impl SourceKinds {
  pub(super) fn insert(&self, ssrc: u32, media_type: MediaType) {
    self
      .0
      .write()
      .unwrap_or_else(|e| e.into_inner())
      .insert(ssrc, media_type);
  }

  pub(super) fn remove(&self, ssrc: u32) {
    self.0.write().unwrap_or_else(|e| e.into_inner()).remove(&ssrc);
  }

  fn get(&self, ssrc: u32) -> Option<MediaType> {
    self.0.read().unwrap_or_else(|e| e.into_inner()).get(&ssrc).copied()
  }
}

/// Enables retransmission requests on the jitterbuffers of video sources, and of unknown ones (the stream can
/// appear before its source is announced).
pub(super) fn connect_new_jitterbuffer(rtpbin: &gstreamer::Element, kinds: SourceKinds, latency: u32) {
  rtpbin.connect("new-jitterbuffer", false, move |values| {
    let f = || {
      let rtpjitterbuffer: gstreamer::Element = values[1].get()?;
      let session: u32 = values[2].get()?;
      let ssrc: u32 = values[3].get()?;
      debug!("new jitterbuffer created for session {} ssrc {}", session, ssrc);

      let enable_rtx = match kinds.get(ssrc) {
        Some(media_type) => media_type.is_video(),
        None => {
          debug!(
            "new-jitterbuffer for unknown ssrc {} — speculatively enabling RTX",
            ssrc
          );
          true
        },
      };

      if enable_rtx {
        debug!("enabling RTX for ssrc {}", ssrc);
        rtpjitterbuffer.set_property("do-retransmission", true);
        rtpjitterbuffer.set_property("drop-on-latency", false);
        rtpjitterbuffer.set_property("latency", latency);
      }
      Ok::<_, anyhow::Error>(())
    };
    if let Err(e) = f() {
      warn!("new-jitterbuffer: {:?}", e);
    }
    None
  });
}

/// The media-SSRC to RTX-SSRC associations of the sources announced so far, and the rtprtxreceive element they
/// are applied to once rtpbin has created it.
#[derive(Clone, Default)]
pub(super) struct RtxSsrcMap(Arc<Mutex<RtxSsrcMapInner>>);

#[derive(Default)]
struct RtxSsrcMapInner {
  associations: BTreeMap<u32, u32>,
  receiver: Option<gstreamer::Element>,
}

impl RtxSsrcMap {
  pub(super) fn insert(&self, ssrc: u32, rtx_ssrc: u32) {
    self.update(|associations| {
      associations.insert(ssrc, rtx_ssrc);
    });
  }

  pub(super) fn remove(&self, ssrc: u32) {
    self.update(|associations| {
      associations.remove(&ssrc);
    });
  }

  fn update(&self, f: impl FnOnce(&mut BTreeMap<u32, u32>)) {
    let mut inner = self.0.lock().unwrap_or_else(|e| e.into_inner());
    let before = inner.associations.clone();
    f(&mut inner.associations);
    if inner.associations != before {
      if let Some(receiver) = &inner.receiver {
        receiver.set_property("ssrc-map", Self::structure(&inner.associations));
      }
    }
  }

  pub(super) fn attach(&self, receiver: &gstreamer::Element) {
    let mut inner = self.0.lock().unwrap_or_else(|e| e.into_inner());
    if !inner.associations.is_empty() {
      receiver.set_property("ssrc-map", Self::structure(&inner.associations));
    }
    inner.receiver = Some(receiver.clone());
  }

  /// Field: the media SSRC as a string; value: its RTX SSRC as an unsigned integer.
  pub(super) fn structure(associations: &BTreeMap<u32, u32>) -> gstreamer::Structure {
    let mut builder = gstreamer::Structure::builder("application/x-rtp-ssrc-map");
    for (ssrc, rtx_ssrc) in associations {
      builder = builder.field(ssrc.to_string(), *rtx_ssrc);
    }
    builder.build()
  }
}

/// Frees the jitterbuffer and the pads of SSRCs that are gone (rtpbin keeps them as long as it lives because
/// `autoremove` is off).
///
/// Clearing an SSRC while rtpbin is pushing one of its packets can make rtpbin return an error to the transport
/// (GStreamer 1.28.7: rtpssrcdemux and the jitterbuffer of the SSRC race), which then stops receiving for the
/// whole session. The clearing therefore happens on the thread that delivers the packets, between two of them,
/// and by a task of ours only if no packet comes for a while, when there is nothing to race with.
pub(super) struct SsrcClearer {
  rtpbin: glib::WeakRef<gstreamer::Element>,
  pending: Mutex<Vec<u32>>,
  any_pending: AtomicBool,
}

impl SsrcClearer {
  pub(super) fn new(rtpbin: &gstreamer::Element) -> Arc<Self> {
    Arc::new(Self {
      rtpbin: rtpbin.downgrade(),
      pending: Mutex::new(Vec::new()),
      any_pending: AtomicBool::new(false),
    })
  }

  pub(super) fn install(self: &Arc<Self>, recv_rtp_sink: &gstreamer::Pad) {
    let clearer = self.clone();
    recv_rtp_sink.add_probe(
      gstreamer::PadProbeType::BUFFER | gstreamer::PadProbeType::BUFFER_LIST,
      move |_, _| {
        if clearer.any_pending.load(Ordering::SeqCst) {
          clearer.clear_pending();
        }
        gstreamer::PadProbeReturn::Ok
      },
    );
  }

  /// Must be called from within a tokio runtime.
  pub(super) fn request(self: &Arc<Self>, ssrc: u32) {
    {
      let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
      pending.push(ssrc);
      self.any_pending.store(true, Ordering::SeqCst);
    }
    let clearer = self.clone();
    tokio::spawn(async move {
      tokio::time::sleep(CLEAR_SSRC_FALLBACK).await;
      let _ = tokio::task::spawn_blocking(move || clearer.clear_pending()).await;
    });
  }

  fn clear_pending(&self) {
    let ssrcs = {
      let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
      self.any_pending.store(false, Ordering::SeqCst);
      std::mem::take(&mut *pending)
    };
    let Some(rtpbin) = self.rtpbin.upgrade() else {
      return;
    };
    for ssrc in ssrcs {
      debug!("clearing ssrc {}", ssrc);
      rtpbin.emit_by_name::<()>("clear-ssrc", &[&0u32, &ssrc]);
    }
  }
}

/// RTX on both directions of video session 0: `rtprtxsend` for our own video SSRC and `rtprtxreceive`.
pub(super) fn connect_request_aux(
  rtpbin: &gstreamer::Element,
  codecs: &[Codec],
  video_ssrc: u32,
  video_rtx_ssrc: u32,
  rtx_ssrc_map: RtxSsrcMap,
) {
  let pts: Vec<(String, u32)> = codecs
    .iter()
    .filter(|codec| codec.is_video())
    .flat_map(|codec| codec.rtx_pt.map(|rtx_pt| (codec.pt.to_string(), rtx_pt as u32)))
    .collect();

  {
    let pts = pts.clone();
    rtpbin.connect("request-aux-sender", false, move |values| {
      let f = || {
        let session: u32 = values[1].get()?;
        debug!("creating RTX sender for session {}", session);
        let ssrc_map = gstreamer::Structure::builder("application/x-rtp-ssrc-map")
          .field(video_ssrc.to_string(), video_rtx_ssrc)
          .build();
        let bin = build_rtx_bin("rtprtxsend", payload_type_map(&pts), Some(ssrc_map), session)?;
        Ok::<_, anyhow::Error>(Some(bin.to_value()))
      };
      match f() {
        Ok(o) => o,
        Err(e) => {
          warn!("request-aux-sender: {:?}", e);
          None
        },
      }
    });
  }

  rtpbin.connect("request-aux-receiver", false, move |values| {
    let f = || {
      let session: u32 = values[1].get()?;
      debug!("creating RTX receiver for session {}", session);
      let bin = build_rtx_bin("rtprtxreceive", payload_type_map(&pts), None, session)?;
      let receiver = bin.children().into_iter().next().context("RTX bin has no element")?;
      rtx_ssrc_map.attach(&receiver);
      Ok::<_, anyhow::Error>(Some(bin.to_value()))
    };
    match f() {
      Ok(o) => o,
      Err(e) => {
        warn!("request-aux-receiver: {:?}", e);
        None
      },
    }
  });
}

fn payload_type_map(pts: &[(String, u32)]) -> gstreamer::Structure {
  let mut pt_map = gstreamer::Structure::builder("application/x-rtp-pt-map");
  for (pt, rtx_pt) in pts {
    pt_map = pt_map.field(pt, *rtx_pt);
  }
  pt_map.build()
}

fn build_rtx_bin(
  element_name: &str,
  pt_map: gstreamer::Structure,
  ssrc_map: Option<gstreamer::Structure>,
  session: u32,
) -> Result<gstreamer::Bin> {
  let mut builder = gstreamer::ElementFactory::make(element_name).property("payload-type-map", pt_map);
  if let Some(ssrc_map) = ssrc_map {
    builder = builder.property("ssrc-map", ssrc_map);
  }
  let rtx_element = builder.build()?;
  let bin = gstreamer::Bin::new();
  bin.add(&rtx_element)?;
  for name in ["src", "sink"] {
    let pad = rtx_element
      .static_pad(name)
      .context(format!("{} has no {} pad", element_name, name))?;
    bin.add_pad(
      &gstreamer::GhostPad::builder_with_target(&pad)?
        .name(format!("{name}_{session}"))
        .build(),
    )?;
  }
  Ok(bin)
}

pub(super) fn poll_sender_reports(rtpbin: &gstreamer::Element, seen: &mut HashMap<u32, u64>) -> Vec<SyncSample> {
  let Some(session) = rtpbin.emit_by_name::<Option<gstreamer::Element>>("get-session", &[&0u32]) else {
    return Vec::new();
  };
  let internal = session.property::<glib::Object>("internal-session");
  let sources = internal.property::<glib::ValueArray>("sources");

  let mut reports = Vec::new();
  for source in sources.iter().filter_map(|value| value.get::<glib::Object>().ok()) {
    let stats = source.property::<Option<gstreamer::Structure>>("stats");
    let Some(stats) = stats else {
      continue;
    };
    if stats.get::<bool>("internal").unwrap_or(true) || !stats.get::<bool>("have-sr").unwrap_or(false) {
      continue;
    }
    let (Ok(ssrc), Ok(ntp_time), Ok(rtp_time)) = (
      stats.get::<u32>("ssrc"),
      stats.get::<u64>("sr-ntptime"),
      stats.get::<u32>("sr-rtptime"),
    ) else {
      continue;
    };
    if ntp_time != 0 && seen.insert(ssrc, ntp_time) != Some(ntp_time) {
      reports.push(SyncSample::SenderReport {
        ssrc,
        ntp_time,
        rtp_time,
      });
    }
  }
  reports
}

/// Holds only weak references; ends when the rtpbin is gone.
pub(super) fn spawn_sender_report_poller(
  rtpbin: &gstreamer::Element,
  sink: SyncSampleSink,
) -> tokio::task::JoinHandle<()> {
  let rtpbin = rtpbin.downgrade();
  tokio::spawn(async move {
    let mut seen = HashMap::new();
    let mut interval = tokio::time::interval(SENDER_REPORT_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
      interval.tick().await;
      let Some(rtpbin) = rtpbin.upgrade() else {
        break;
      };
      if rtpbin.current_state() == gstreamer::State::Null {
        continue;
      }
      for report in poll_sender_reports(&rtpbin, &mut seen) {
        sink.emit(|| report);
      }
    }
  })
}
