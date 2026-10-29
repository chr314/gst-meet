//! The media layer as the signaling sees it. `pipeline` implements these traits (below), so that the signaling can be
//! tested without GStreamer pipelines and network.

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use jitsi_xmpp_parsers::jingle_ice_udp::Transport as IceUdpTransport;
use tokio::sync::mpsc;

use crate::{
  pipeline::{
    ConferenceMedia, LocalIceCandidate, LocalSsrcs, LocalTransport, MediaSession, RemoteSourceDesc, SessionOffer,
  },
  xmpp::extdisco::Service,
};

#[async_trait]
pub(crate) trait Media: Send + Sync {
  fn pipeline(&self) -> gstreamer::Pipeline;
  fn local_ssrcs(&self) -> LocalSsrcs;
  fn receivable_video_codecs(&self) -> Vec<String>;
  fn audio_sink_element(&self) -> Option<gstreamer::Element>;
  fn video_sink_element(&self) -> Option<gstreamer::Element>;
  fn reevaluate_streams(&self);
  async fn start_session(&self, offer: &SessionOffer, ice_servers: &[Service]) -> Result<Box<dyn Session>>;
  async fn shutdown(&self);
}

#[async_trait]
pub(crate) trait Session: Send + Sync {
  fn generation(&self) -> u64;
  fn local_transport(&self) -> LocalTransport;
  async fn gather_candidates(
    &mut self,
    timeout: Duration,
  ) -> (Vec<LocalIceCandidate>, Option<mpsc::UnboundedReceiver<LocalIceCandidate>>);
  fn add_remote_candidates(&self, transport: &IceUdpTransport) -> Result<()>;
  async fn sources_added(&self, sources: Vec<RemoteSourceDesc>);
  async fn sources_removed(&self, ssrcs: Vec<u32>);
  async fn participant_left(&self, participant_id: &str);
  async fn drain(&self, timeout: Duration) -> Result<()>;
  async fn shutdown(self: Box<Self>, drain_timeout: Duration);
  fn rtp_source_stats(&self) -> Vec<gstreamer::Structure>;
}

#[async_trait]
impl Media for ConferenceMedia {
  fn pipeline(&self) -> gstreamer::Pipeline {
    ConferenceMedia::pipeline(self)
  }

  fn local_ssrcs(&self) -> LocalSsrcs {
    ConferenceMedia::local_ssrcs(self)
  }

  fn receivable_video_codecs(&self) -> Vec<String> {
    ConferenceMedia::receivable_video_codecs(self)
  }

  fn audio_sink_element(&self) -> Option<gstreamer::Element> {
    ConferenceMedia::audio_sink_element(self)
  }

  fn video_sink_element(&self) -> Option<gstreamer::Element> {
    ConferenceMedia::video_sink_element(self)
  }

  fn reevaluate_streams(&self) {
    ConferenceMedia::reevaluate_streams(self);
  }

  async fn start_session(&self, offer: &SessionOffer, ice_servers: &[Service]) -> Result<Box<dyn Session>> {
    Ok(Box::new(ConferenceMedia::start_session(self, offer, ice_servers).await?))
  }

  async fn shutdown(&self) {
    ConferenceMedia::shutdown(self).await;
  }
}

#[async_trait]
impl Session for MediaSession {
  fn generation(&self) -> u64 {
    MediaSession::generation(self)
  }

  fn local_transport(&self) -> LocalTransport {
    MediaSession::local_transport(self)
  }

  async fn gather_candidates(
    &mut self,
    timeout: Duration,
  ) -> (Vec<LocalIceCandidate>, Option<mpsc::UnboundedReceiver<LocalIceCandidate>>) {
    MediaSession::gather_candidates(self, timeout).await
  }

  fn add_remote_candidates(&self, transport: &IceUdpTransport) -> Result<()> {
    MediaSession::add_remote_candidates(self, transport)
  }

  async fn sources_added(&self, sources: Vec<RemoteSourceDesc>) {
    MediaSession::sources_added(self, sources).await;
  }

  async fn sources_removed(&self, ssrcs: Vec<u32>) {
    MediaSession::sources_removed(self, ssrcs).await;
  }

  async fn participant_left(&self, participant_id: &str) {
    MediaSession::participant_left(self, participant_id).await;
  }

  async fn drain(&self, timeout: Duration) -> Result<()> {
    MediaSession::drain(self, timeout).await
  }

  async fn shutdown(self: Box<Self>, drain_timeout: Duration) {
    MediaSession::shutdown(*self, drain_timeout).await;
  }

  fn rtp_source_stats(&self) -> Vec<gstreamer::Structure> {
    MediaSession::rtp_source_stats(self)
  }
}
