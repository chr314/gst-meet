use colibri::{Bitrate, Bitrates, ColibriMessage, PacketLoss};
use gstreamer::Structure;

use crate::{pipeline::LocalSsrcs, source::RemoteSource};

pub(crate) struct StatsContext<'a> {
  pub(crate) remote_sources: &'a [RemoteSource],
  pub(crate) local_ssrcs: LocalSsrcs,
  pub(crate) region: Option<&'a str>,
  pub(crate) max_enabled_resolution: Option<i32>,
}

/// `source_stats` are the statistics of the sources of the RTP session, one structure per SSRC.
pub(crate) fn endpoint_stats(source_stats: &[Structure], context: &StatsContext) -> ColibriMessage {
  let StatsContext {
    remote_sources,
    local_ssrcs,
    region,
    max_enabled_resolution,
  } = context;
  let remote = |stat: &Structure| {
    stat
      .get::<u32>("ssrc")
      .ok()
      .and_then(|ssrc| remote_sources.iter().find(|source| source.ssrc == ssrc))
  };
  let bitrate = |stat: &Structure| stat.get::<u64>("bitrate").unwrap_or_default();

  let download = |wanted: fn(&RemoteSource) -> bool| -> u64 {
    source_stats
      .iter()
      .filter(|stat| remote(stat).is_some_and(wanted))
      .map(bitrate)
      .sum()
  };
  let upload = |ssrc: u32| -> u64 {
    source_stats
      .iter()
      .filter(|stat| stat.get::<u32>("ssrc").ok() == Some(ssrc))
      .map(bitrate)
      .sum()
  };
  let audio_recv_bitrate = download(|source| source.media_type.is_audio());
  let video_recv_bitrate = download(|source| source.media_type.is_video());
  let audio_send_bitrate = upload(local_ssrcs.audio);
  let video_send_bitrate = upload(local_ssrcs.video);

  let received: Vec<&Structure> = source_stats.iter().filter(|stat| remote(stat).is_some()).collect();
  let recv_packets: u64 = received
    .iter()
    .filter_map(|stat| stat.get::<u64>("packets-received").ok())
    .sum();
  // Loss can be negative because of duplicate packets. Clamp it to zero, per source.
  let recv_lost: u64 = received
    .iter()
    .filter_map(|stat| stat.get::<i32>("packets-lost").ok())
    .map(|lost| u64::try_from(lost).unwrap_or_default())
    .sum();
  let recv_loss = if recv_packets + recv_lost == 0 {
    0.
  }
  else {
    recv_lost as f64 / (recv_packets + recv_lost) as f64
  };

  ColibriMessage::EndpointStats {
    from: None,
    bitrate: Bitrates {
      audio: Bitrate {
        upload: audio_send_bitrate / 1024,
        download: audio_recv_bitrate / 1024,
      },
      video: Bitrate {
        upload: video_send_bitrate / 1024,
        download: video_recv_bitrate / 1024,
      },
      total: Bitrate {
        upload: (audio_send_bitrate + video_send_bitrate) / 1024,
        download: (audio_recv_bitrate + video_recv_bitrate) / 1024,
      },
    },
    packet_loss: PacketLoss {
      total: (recv_loss * 100.) as u64,
      download: (recv_loss * 100.) as u64,
      upload: 0, // TODO
    },
    connection_quality: 100.0,
    jvb_rtt: Some(0), // TODO
    server_region: region.map(ToOwned::to_owned),
    max_enabled_resolution: *max_enabled_resolution,
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::source::MediaType;

  fn stat(ssrc: u32, bitrate: u64, packets_received: u64, packets_lost: i32) -> Structure {
    Structure::builder("application/x-rtp-source-stats")
      .field("ssrc", ssrc)
      .field("bitrate", bitrate)
      .field("packets-received", packets_received)
      .field("packets-lost", packets_lost)
      .build()
  }

  fn source(ssrc: u32, media_type: MediaType) -> RemoteSource {
    RemoteSource {
      participant_id: Some("a".to_owned()),
      ssrc,
      media_type,
      source_name: None,
    }
  }

  #[test]
  fn adds_up_what_is_received_and_what_is_sent() {
    gstreamer::init().unwrap();
    let sources = [source(10, MediaType::Audio), source(11, MediaType::Video)];
    let stats = [
      stat(10, 64 * 1024, 900, 100),
      stat(11, 1024 * 1024, 100, -5),
      // Ours, and one nobody announced.
      stat(1, 32 * 1024, 0, 0),
      stat(2, 512 * 1024, 0, 0),
      stat(99, 5 * 1024 * 1024, 1000, 1000),
    ];
    let message = endpoint_stats(
      &stats,
      &StatsContext {
        remote_sources: &sources,
        local_ssrcs: LocalSsrcs {
          audio: 1,
          video: 2,
          video_rtx: 3,
        },
        region: Some("eu"),
        max_enabled_resolution: Some(720),
      },
    );
    let ColibriMessage::EndpointStats {
      bitrate,
      packet_loss,
      server_region,
      max_enabled_resolution,
      ..
    } = message
    else {
      panic!("not endpoint stats");
    };
    assert_eq!((bitrate.audio.download, bitrate.audio.upload), (64, 32));
    assert_eq!((bitrate.video.download, bitrate.video.upload), (1024, 512));
    assert_eq!((bitrate.total.download, bitrate.total.upload), (1088, 544));
    // 100 lost of 1100 expected, the negative loss counts as none: 9 %.
    assert_eq!(packet_loss.download, 9);
    assert_eq!(server_region.as_deref(), Some("eu"));
    assert_eq!(max_enabled_resolution, Some(720));
  }

  #[test]
  fn nothing_received_is_no_loss() {
    gstreamer::init().unwrap();
    let message = endpoint_stats(
      &[],
      &StatsContext {
        remote_sources: &[],
        local_ssrcs: LocalSsrcs {
          audio: 1,
          video: 2,
          video_rtx: 3,
        },
        region: None,
        max_enabled_resolution: None,
      },
    );
    assert!(matches!(message, ColibriMessage::EndpointStats { packet_loss, .. } if packet_loss.total == 0));
  }
}
