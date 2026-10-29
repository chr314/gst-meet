use std::{
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::Duration,
};

use anyhow::{Context, Result};
use gstreamer::{prelude::*, Bin, GhostPad};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::{
  elements::StreamBarrier,
  hdrext,
  offer::{RemoteSourceDesc, SessionOffer},
  owner::{self, Owner},
  rtp::{self, RtxSsrcMap, SourceKinds},
  streams::{self, PadEntry, PadKey, StreamHooks, StreamsHandle, StreamsParams},
  transport::TransportBins,
  LocalSsrcs, MediaConfig,
};
use crate::conference::ReceiveMode;

pub(super) struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
  fn drop(&mut self) {
    self.0.abort();
  }
}

pub(super) struct RtpSessionParams<'a> {
  pub(super) session: u64,
  pub(super) pipeline: &'a gstreamer::Pipeline,
  pub(super) offer: &'a SessionOffer,
  pub(super) config: &'a MediaConfig,
  pub(super) local_ssrcs: LocalSsrcs,
  pub(super) stream_ids: Arc<AtomicU64>,
  pub(super) hooks: StreamHooks,
  pub(super) transport: TransportBins,
}

pub(crate) struct RtpSession {
  session: u64,
  bin: Bin,
  rtpbin: gstreamer::Element,
  streams: StreamsHandle,
  kinds: SourceKinds,
  rtx: RtxSsrcMap,
  _sender_report_poller: Option<AbortOnDrop>,
}

impl RtpSession {
  /// The bin is not added to the pipeline yet. rtpbin's pads only appear once packets arrive.
  pub(super) fn new(params: RtpSessionParams) -> Result<Self> {
    let RtpSessionParams {
      session,
      pipeline,
      offer,
      config,
      local_ssrcs,
      stream_ids,
      hooks,
      transport,
    } = params;

    let bin = Bin::with_name(&format!("session_{session}"));
    owner::mark(&bin, Owner::Session { session });
    let rtpbin = rtp::build_rtpbin(config.buffer_size)?;
    bin.add_many([&rtpbin, transport.receive.upcast_ref(), transport.send.upcast_ref()])?;

    let kinds = SourceKinds::default();
    let rtx = RtxSsrcMap::default();
    register_sources(&kinds, &rtx, &offer.sources);

    rtp::connect_request_pt_map(&rtpbin, offer.codecs.clone(), offer.hdrexts);
    rtp::connect_new_jitterbuffer(&rtpbin, kinds.clone(), config.buffer_size);
    rtp::connect_request_aux(
      &rtpbin,
      &offer.codecs,
      local_ssrcs.video,
      local_ssrcs.video_rtx,
      rtx.clone(),
    );

    let sender_report_poller = (config.receive_mode == ReceiveMode::Encoded)
      .then(|| AbortOnDrop(rtp::spawn_sender_report_poller(&rtpbin, hooks.sync_sample.clone())));

    let clearer = rtp::SsrcClearer::new(&rtpbin);
    let streams = streams::spawn(StreamsParams {
      session,
      pipeline: pipeline.clone(),
      session_bin: bin.clone(),
      clearer: clearer.clone(),
      codecs: offer.codecs.clone(),
      mode: config.receive_mode,
      video_scale: (config.recv_video_scale_width, config.recv_video_scale_height),
      drain_timeout: config.stream_drain_timeout,
      stream_ids,
      hooks,
    });
    connect_pad_signals(&rtpbin, &bin, &streams);
    streams.sources_added(offer.sources.iter().map(|desc| desc.source.clone()).collect());

    let link = |name: &str| {
      rtpbin
        .request_pad_simple(name)
        .with_context(|| format!("rtpbin refused to create {name}"))
    };
    let transport_pad = |bin: &Bin, name: &str| {
      bin
        .static_pad(name)
        .with_context(|| format!("the transport has no {name} pad"))
    };
    let recv_rtp_sink = link("recv_rtp_sink_0")?;
    transport_pad(&transport.receive, "rtp_recv_src")?.link(&recv_rtp_sink)?;
    transport_pad(&transport.receive, "rtcp_recv_src")?.link(&link("recv_rtcp_sink_0")?)?;
    let send_rtp_sink = link("send_rtp_sink_0")?;
    rtpbin
      .static_pad("send_rtp_src_0")
      .context("rtpbin has no send_rtp_src_0 pad")?
      .link(&transport_pad(&transport.send, "rtp_send_sink")?)?;
    link("send_rtcp_src_0")?.link(&transport_pad(&transport.send, "rtcp_send_sink")?)?;
    bin.add_pad(
      &GhostPad::builder_with_target(&send_rtp_sink)?
        .name("send_rtp_sink")
        .build(),
    )?;

    rtp::configure_twcc_feedback(&rtpbin, 0)?;
    hdrext::install_normalizer_probe(&recv_rtp_sink);
    clearer.install(&recv_rtp_sink);

    Ok(Self {
      session,
      bin,
      rtpbin,
      streams,
      kinds,
      rtx,
      _sender_report_poller: sender_report_poller,
    })
  }

  pub(super) fn generation(&self) -> u64 {
    self.session
  }

  pub(super) fn bin(&self) -> &Bin {
    &self.bin
  }

  pub(super) fn send_sink_pad(&self) -> Result<gstreamer::Pad> {
    self
      .bin
      .static_pad("send_rtp_sink")
      .context("the session has no send_rtp_sink pad")
  }

  pub(super) fn streams(&self) -> StreamsHandle {
    self.streams.clone()
  }

  pub(crate) async fn sources_added(&self, sources: Vec<RemoteSourceDesc>) {
    register_sources(&self.kinds, &self.rtx, &sources);
    self
      .streams
      .sources_added(sources.into_iter().map(|desc| desc.source).collect());
  }

  pub(crate) async fn sources_removed(&self, ssrcs: Vec<u32>) {
    for ssrc in &ssrcs {
      self.kinds.remove(*ssrc);
      self.rtx.remove(*ssrc);
    }
    self.streams.sources_removed(ssrcs);
  }

  pub(crate) async fn participant_left(&self, participant_id: &str) {
    self.streams.participant_left(participant_id);
  }

  pub(crate) async fn drain(&self, timeout: Duration) -> Result<()> {
    self.streams.drain(timeout).await
  }

  /// Retires the streams and stops the controller. The bin is left in the pipeline.
  pub(super) async fn retire_streams(&self, timeout: Duration) {
    self.streams.shutdown(timeout).await;
  }

  /// One `GstStructure` per RTP source of session 0, as rtpsession reports them.
  pub(crate) fn rtp_source_stats(&self) -> Vec<gstreamer::Structure> {
    self
      .rtpbin
      .emit_by_name::<Option<gstreamer::Element>>("get-session", &[&0u32])
      .map(|session| session.property::<gstreamer::Structure>("stats"))
      .and_then(|stats| stats.get::<glib::ValueArray>("source-stats").ok())
      .map(|sources| sources.iter().filter_map(|source| source.get().ok()).collect())
      .unwrap_or_default()
  }
}

fn register_sources(kinds: &SourceKinds, rtx: &RtxSsrcMap, sources: &[RemoteSourceDesc]) {
  for desc in sources {
    kinds.insert(desc.source.ssrc, desc.source.media_type);
    if let Some(rtx_ssrc) = desc.rtx_ssrc {
      rtx.insert(desc.source.ssrc, rtx_ssrc);
    }
  }
}

/// A barrier and an active ghost pad per rtpbin source pad, in place before the pad's first buffer.
fn connect_pad_signals(rtpbin: &gstreamer::Element, bin: &Bin, streams: &StreamsHandle) {
  static SEQ: AtomicU64 = AtomicU64::new(0);

  {
    let bin = bin.downgrade();
    let streams = streams.clone();
    rtpbin.connect("pad-added", false, move |values| {
      let pad: gstreamer::Pad = values[1].get().ok()?;
      let key = PadKey::parse(&pad.name())?;
      let bin = bin.upgrade()?;
      match build_pad_entry(&bin, pad, key, SEQ.fetch_add(1, Ordering::SeqCst), &streams) {
        Ok(entry) => streams.pad_added(entry),
        Err(e) => warn!("failed to set up {:?}: {:#}", key, e),
      }
      None
    });
  }
  {
    let streams = streams.clone();
    rtpbin.connect("pad-removed", false, move |values| {
      let pad: gstreamer::Pad = values[1].get().ok()?;
      let key = PadKey::parse(&pad.name())?;
      debug!("rtpbin removed {:?}", key);
      streams.pad_removed(key, pad);
      None
    });
  }
}

fn build_pad_entry(
  bin: &Bin,
  rtpbin_pad: gstreamer::Pad,
  key: PadKey,
  seq: u64,
  streams: &StreamsHandle,
) -> Result<PadEntry> {
  let barrier = StreamBarrier::new(&format!("barrier_{}_{}_{}", key.ssrc, key.pt, seq));
  {
    let (on_data, on_failure) = (streams.clone(), streams.clone());
    barrier.set_callbacks(
      move || on_data.pad_data(key),
      move |flow| on_failure.pad_failed(key, flow),
    );
  }
  bin.add(&barrier)?;
  let result = (|| -> Result<GhostPad> {
    barrier.sync_state_with_parent()?;
    rtpbin_pad.link(&barrier.static_pad("sink").context("the barrier has no sink pad")?)?;
    let ghost = GhostPad::builder_with_target(&barrier.static_pad("src").context("the barrier has no src pad")?)?
      .name(format!("stream_{}_{}_{}", key.ssrc, key.pt, seq))
      .build();
    ghost.set_active(true)?;
    bin.add_pad(&ghost)?;
    // The barrier's own src pad stays linked to the ghost pad's internal pad; what is linked to the ghost pad is
    // what the barrier has to notice.
    barrier.track_downstream(&ghost);
    Ok(ghost)
  })();
  match result {
    Ok(ghost) => Ok(PadEntry {
      key,
      seq,
      rtpbin_pad,
      clearing: false,
      barrier,
      ghost,
    }),
    Err(e) => {
      let _ = barrier.set_state(gstreamer::State::Null);
      let _ = bin.remove(&barrier);
      Err(e)
    },
  }
}
