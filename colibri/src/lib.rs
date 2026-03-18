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
        from: Option<String>,
        bitrate: Bitrates,
        packet_loss: PacketLoss,
        connection_quality: f32,
        #[serde(rename = "jvbRTT")]
        jvb_rtt: Option<i32>,
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
    #[serde(rename_all = "camelCase")]
    ReceiverVideoConstraints {
        #[serde(skip_serializing_if = "Option::is_none")]
        last_n: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        selected_endpoints: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        selected_sources: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        on_stage_endpoints: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        on_stage_sources: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        default_constraints: Option<Constraints>,
        #[serde(skip_serializing_if = "Option::is_none")]
        constraints: Option<HashMap<String, Constraints>>,
    },
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
    source: String,
    owner: Option<String>,
    ssrc: u32,
    rtx: u32,
    video_type: VideoType,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AudioSourceMapping {
    source: String,
    owned: Option<String>,
    ssrc: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Constraints {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ideal_height: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_height: Option<i32>,
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
