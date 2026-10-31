//! Data structures for the messages JVB and its clients exchange on the Colibri WebSocket (the "bridge channel").
//!
//! The names and fields follow `BridgeChannelMessage.kt` of jitsi-videobridge. A message of a class this crate does
//! not know deserializes to [`ColibriMessage::Unknown`] instead of failing.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "colibriClass")]
pub enum ColibriMessage {
  #[serde(rename_all = "camelCase")]
  DominantSpeakerEndpointChangeEvent {
    dominant_speaker_endpoint: String,
    previous_speakers: Vec<String>,
  },
  #[serde(rename_all = "camelCase")]
  EndpointConnectivityStatusChangeEvent {
    endpoint: String,
    #[serde_as(as = "DisplayFromStr")]
    active: bool,
  },
  #[serde(rename_all = "camelCase")]
  EndpointMessage {
    from: Option<String>,
    to: Option<String>,
    msg_payload: serde_json::Value,
  },
  #[serde(rename_all = "camelCase")]
  EndpointStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    from: Option<String>,
    bitrate: Bitrates,
    packet_loss: PacketLoss,
    connection_quality: f32,
    #[serde(rename = "jvbRTT")]
    jvb_rtt: Option<f64>,
    server_region: Option<String>,
    max_enabled_resolution: Option<i32>,
  },
  #[serde(rename_all = "camelCase")]
  ForwardedSources { forwarded_sources: Vec<String> },
  #[serde(rename_all = "camelCase")]
  LastNChangedEvent { last_n: i32 },
  #[serde(rename_all = "camelCase")]
  LastNEndpointsChangeEvent { last_n_endpoints: Vec<String> },
  #[serde(rename_all = "camelCase")]
  PinnedEndpointChangedEvent { pinned_endpoint: Option<String> },
  #[serde(rename_all = "camelCase")]
  ReceiverVideoConstraint { max_frame_height: i32 },
  ReceiverVideoConstraints(ReceiverVideoConstraints),
  #[serde(rename_all = "camelCase")]
  SelectedEndpointsChangedEvent { selected_endpoints: Vec<String> },
  #[serde(rename_all = "camelCase")]
  SenderVideoConstraints { video_constraints: Constraints },
  #[serde(rename_all = "camelCase")]
  ServerHello { version: Option<String> },
  #[serde(rename_all = "camelCase")]
  VideoTypeMessage { video_type: VideoType },
  #[serde(rename_all = "camelCase")]
  SourceVideoTypeMessage { source_name: String, video_type: VideoType },
  #[serde(rename_all = "camelCase")]
  SenderSourceConstraints { source_name: String, max_height: i32 },
  #[serde(rename_all = "camelCase")]
  VideoSourcesMap { mapped_sources: Vec<VideoSourceMapping> },
  #[serde(rename_all = "camelCase")]
  AudioSourcesMap { mapped_sources: Vec<AudioSourceMapping> },
  #[serde(rename_all = "camelCase")]
  ConnectionStats { estimated_downlink_bandwidth: Option<u64> },
  /// A class this crate does not know. It cannot be sent.
  #[serde(other, skip_serializing)]
  Unknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum VideoType {
  Camera,
  Desktop,
  None,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VideoSourceMapping {
  pub source: String,
  pub owner: Option<String>,
  pub ssrc: u32,
  pub rtx: u32,
  pub video_type: VideoType,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AudioSourceMapping {
  pub source: String,
  pub owner: Option<String>,
  pub ssrc: u32,
}

/// What the bridge should send for a video source.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Constraints {
  /// `-1` for no limit, `0` to receive nothing.
  pub max_height: i32,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub max_frame_rate: Option<f64>,
}

impl Constraints {
  pub const UNLIMITED: Constraints = Constraints::height(-1);

  pub const fn height(max_height: i32) -> Self {
    Self {
      max_height,
      max_frame_rate: None,
    }
  }
}

/// What a receiver wants from the bridge. Every field is optional on the wire, and the bridge only changes what a
/// message mentions: a field that is left out keeps the value it has, also across reconnects of the WebSocket. To
/// take a value back, send the bridge's neutral one (`-1` for `last_n`, an empty list or map, `-1` for the assumed
/// bandwidth); [`Constraints`] have no neutral value and need the bridge's configured default instead.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReceiverVideoConstraints {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub last_n: Option<i32>,
  /// Source names.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub selected_sources: Option<Vec<String>>,
  /// Source names.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub on_stage_sources: Option<Vec<String>>,
  /// For the sources that have no entry in `constraints`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub default_constraints: Option<Constraints>,
  /// By source name.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub constraints: Option<HashMap<String, Constraints>>,
  /// The bandwidth the bridge is to assume instead of its own estimate. Negative: no assumption (the bridge ignores
  /// the value, and it keeps ignoring it until a non-negative one comes).
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub assumed_bandwidth_bps: Option<i64>,
}

impl ReceiverVideoConstraints {
  pub fn is_empty(&self) -> bool {
    *self == Self::default()
  }

  /// Takes over every field `update` sets.
  pub fn merge(&mut self, update: ReceiverVideoConstraints) {
    let ReceiverVideoConstraints {
      last_n,
      selected_sources,
      on_stage_sources,
      default_constraints,
      constraints,
      assumed_bandwidth_bps,
    } = update;
    self.last_n = last_n.or(self.last_n);
    self.selected_sources = selected_sources.or_else(|| self.selected_sources.take());
    self.on_stage_sources = on_stage_sources.or_else(|| self.on_stage_sources.take());
    self.default_constraints = default_constraints.or(self.default_constraints);
    self.constraints = constraints.or_else(|| self.constraints.take());
    self.assumed_bandwidth_bps = assumed_bandwidth_bps.or(self.assumed_bandwidth_bps);
  }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Bitrates {
  pub audio: Bitrate,
  pub video: Bitrate,
  #[serde(flatten)]
  pub total: Bitrate,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Bitrate {
  pub upload: u64,
  pub download: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PacketLoss {
  pub total: u64,
  pub download: u64,
  pub upload: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum JsonMessage {
  E2ePingRequest { id: i32 },
  E2ePingResponse { id: i32 },
}

#[cfg(test)]
mod tests {
  use serde_json::json;

  use super::*;

  fn parse(value: serde_json::Value) -> ColibriMessage {
    serde_json::from_value(value).unwrap()
  }

  #[test]
  fn receiver_video_constraints_use_the_names_of_the_bridge() {
    let message = ColibriMessage::ReceiverVideoConstraints(ReceiverVideoConstraints {
      last_n: Some(3),
      selected_sources: Some(vec!["abcd1234-v0".into()]),
      on_stage_sources: Some(vec!["abcd1234-v1".into()]),
      default_constraints: Some(Constraints::height(180)),
      constraints: Some(HashMap::from([(
        "abcd1234-v1".to_owned(),
        Constraints {
          max_height: 720,
          max_frame_rate: Some(15.0),
        },
      )])),
      assumed_bandwidth_bps: Some(2_000_000),
    });
    assert_eq!(
      serde_json::to_value(&message).unwrap(),
      json!({
        "colibriClass": "ReceiverVideoConstraints",
        "lastN": 3,
        "selectedSources": ["abcd1234-v0"],
        "onStageSources": ["abcd1234-v1"],
        "defaultConstraints": { "maxHeight": 180 },
        "constraints": { "abcd1234-v1": { "maxHeight": 720, "maxFrameRate": 15.0 } },
        "assumedBandwidthBps": 2000000,
      })
    );
  }

  #[test]
  fn a_negative_assumed_bandwidth_is_sent_as_it_is() {
    let message = ColibriMessage::ReceiverVideoConstraints(ReceiverVideoConstraints {
      assumed_bandwidth_bps: Some(-1),
      ..Default::default()
    });
    assert_eq!(
      serde_json::to_value(&message).unwrap(),
      json!({ "colibriClass": "ReceiverVideoConstraints", "assumedBandwidthBps": -1 })
    );
    assert_eq!(serde_json::from_value::<ColibriMessage>(serde_json::to_value(&message).unwrap()).unwrap(), message);
  }

  #[test]
  fn receiver_video_constraints_leave_out_what_is_not_set() {
    let message = ColibriMessage::ReceiverVideoConstraints(ReceiverVideoConstraints {
      last_n: Some(-1),
      ..Default::default()
    });
    assert_eq!(
      serde_json::to_value(&message).unwrap(),
      json!({ "colibriClass": "ReceiverVideoConstraints", "lastN": -1 })
    );
  }

  #[test]
  fn constraints_require_a_max_height() {
    assert!(serde_json::from_value::<Constraints>(json!({ "maxFrameRate": 30.0 })).is_err());
    assert_eq!(
      serde_json::from_value::<Constraints>(json!({ "maxHeight": 360 })).unwrap(),
      Constraints::height(360)
    );
  }

  #[test]
  fn merge_takes_over_only_what_the_update_sets() {
    let mut state = ReceiverVideoConstraints {
      last_n: Some(5),
      selected_sources: Some(vec!["a-v0".into()]),
      ..Default::default()
    };
    state.merge(ReceiverVideoConstraints {
      last_n: Some(2),
      assumed_bandwidth_bps: Some(1_000_000),
      ..Default::default()
    });
    assert_eq!(
      state,
      ReceiverVideoConstraints {
        last_n: Some(2),
        selected_sources: Some(vec!["a-v0".into()]),
        assumed_bandwidth_bps: Some(1_000_000),
        ..Default::default()
      }
    );
    assert!(!state.is_empty());
    assert!(ReceiverVideoConstraints::default().is_empty());
  }

  #[test]
  fn an_unknown_class_is_not_an_error() {
    assert_eq!(
      parse(json!({ "colibriClass": "SyntheticSourceSendingChangeEvent", "sourceName": "x-a0", "sending": true })),
      ColibriMessage::Unknown
    );
    assert!(serde_json::to_string(&ColibriMessage::Unknown).is_err());
  }

  #[test]
  fn parses_the_messages_of_the_bridge() {
    assert_eq!(
      parse(json!({ "colibriClass": "ForwardedSources", "forwardedSources": ["a-v0", "b-v0"] })),
      ColibriMessage::ForwardedSources {
        forwarded_sources: vec!["a-v0".into(), "b-v0".into()]
      }
    );
    assert_eq!(
      parse(json!({ "colibriClass": "EndpointConnectivityStatusChangeEvent", "endpoint": "a", "active": "true" })),
      ColibriMessage::EndpointConnectivityStatusChangeEvent {
        endpoint: "a".into(),
        active: true
      }
    );
    assert_eq!(
      parse(json!({ "colibriClass": "ServerHello" })),
      ColibriMessage::ServerHello { version: None }
    );
    assert_eq!(
      parse(json!({ "colibriClass": "ConnectionStats", "estimatedDownlinkBandwidth": 1234567 })),
      ColibriMessage::ConnectionStats {
        estimated_downlink_bandwidth: Some(1234567)
      }
    );
    assert_eq!(
      parse(json!({ "colibriClass": "SenderSourceConstraints", "sourceName": "me-v0", "maxHeight": 360 })),
      ColibriMessage::SenderSourceConstraints {
        source_name: "me-v0".into(),
        max_height: 360
      }
    );
  }

  #[test]
  fn source_mappings_have_the_owner_field() {
    let video = parse(json!({
      "colibriClass": "VideoSourcesMap",
      "mappedSources": [{ "source": "a-v0", "owner": "a", "ssrc": 1111, "rtx": 2222, "videoType": "camera" }],
    }));
    let ColibriMessage::VideoSourcesMap { mapped_sources } = video else {
      panic!("not a video sources map");
    };
    assert_eq!(mapped_sources[0].owner.as_deref(), Some("a"));
    assert_eq!(mapped_sources[0].video_type, VideoType::Camera);

    let audio = parse(json!({
      "colibriClass": "AudioSourcesMap",
      "mappedSources": [{ "source": "a-a0", "owner": "a", "ssrc": 3333 }],
    }));
    let ColibriMessage::AudioSourcesMap { mapped_sources } = audio else {
      panic!("not an audio sources map");
    };
    assert_eq!(mapped_sources[0].owner.as_deref(), Some("a"));
    assert_eq!(
      serde_json::to_value(&mapped_sources[0]).unwrap(),
      json!({ "source": "a-a0", "owner": "a", "ssrc": 3333 })
    );
  }

  #[test]
  fn endpoint_stats_round_trip() {
    let stats = ColibriMessage::EndpointStats {
      from: None,
      bitrate: Bitrates {
        audio: Bitrate {
          upload: 1,
          download: 2,
        },
        video: Bitrate {
          upload: 3,
          download: 4,
        },
        total: Bitrate {
          upload: 4,
          download: 6,
        },
      },
      packet_loss: PacketLoss {
        total: 0,
        download: 0,
        upload: 0,
      },
      connection_quality: 100.0,
      jvb_rtt: Some(12.5),
      server_region: None,
      max_enabled_resolution: Some(720),
    };
    let value = serde_json::to_value(&stats).unwrap();
    assert_eq!(value["jvbRTT"], 12.5);
    assert_eq!(value["bitrate"]["upload"], 4);
    assert_eq!(serde_json::from_value::<ColibriMessage>(value).unwrap(), stats);
  }
}
