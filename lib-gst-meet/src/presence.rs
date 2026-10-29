use minidom::Element;
use xmpp_parsers::{muc::Muc, nick::Nick, ns};

use crate::{disco, source::MediaType};

#[derive(Debug, Clone)]
pub(crate) struct PresenceState {
  endpoint_id: String,
  nick: String,
  region: Option<String>,
  /// The video codecs we can receive, in order of preference.
  video_codecs: Vec<String>,
  extra_features: Vec<String>,
  audio_muted: bool,
  video_muted: bool,
}

impl PresenceState {
  pub(crate) fn new(
    endpoint_id: String,
    nick: String,
    region: Option<String>,
    video_codecs: Vec<String>,
    extra_features: Vec<String>,
  ) -> Self {
    Self {
      endpoint_id,
      nick,
      region,
      video_codecs,
      extra_features,
      audio_muted: false,
      video_muted: false,
    }
  }

  pub(crate) fn set_muted(&mut self, media_type: MediaType, muted: bool) {
    if media_type.is_audio() {
      self.audio_muted = muted;
    }
    else {
      self.video_muted = muted;
    }
  }

  /// The `SourceInfo` of our presence: the state of our sources by name.
  pub(crate) fn source_info(&self) -> String {
    serde_json::json!({
      format!("{}-a0", self.endpoint_id): { "muted": self.audio_muted },
      format!("{}-v0", self.endpoint_id): { "muted": self.video_muted },
    })
    .to_string()
  }

  /// Only the joining presence (`initial`) asks for the MUC extension: the room answers it with the occupants it has,
  /// and Prosody sends them all again if a later presence asks again.
  pub(crate) fn payloads(&self, initial: bool) -> Vec<Element> {
    let mut payloads = vec![];
    if initial {
      payloads.push(Muc::new().into());
    }
    payloads.extend(disco::presence_caps());
    payloads.extend([
      Element::builder("stats-id", ns::DEFAULT_NS).append("gst-meet").build(),
      Element::builder("jitsi_participant_codecType", ns::DEFAULT_NS)
        .append(self.video_codecs.first().map(|codec| codec.as_str()).unwrap_or(""))
        .build(),
      Element::builder("jitsi_participant_codecList", ns::DEFAULT_NS)
        .append(self.video_codecs.join(",").as_str())
        .build(),
      Element::builder("audiomuted", ns::DEFAULT_NS)
        .append(self.audio_muted.to_string())
        .build(),
      Element::builder("videomuted", ns::DEFAULT_NS)
        .append(self.video_muted.to_string())
        .build(),
      Element::builder("SourceInfo", ns::DEFAULT_NS)
        .append(self.source_info())
        .build(),
      Nick(self.nick.clone()).into(),
    ]);
    if let Some(region) = &self.region {
      payloads.extend([
        Element::builder("jitsi_participant_region", ns::DEFAULT_NS)
          .append(region.as_str())
          .build(),
        Element::builder("region", "http://jitsi.org/jitsi-meet")
          .attr(::minidom::rxml::xml_ncname!("id").to_owned(), region)
          .build(),
      ]);
    }
    payloads.extend(self.extra_features.iter().map(|var| {
      Element::builder("feature", ns::DISCO_INFO)
        .attr(::minidom::rxml::xml_ncname!("var").to_owned(), var.as_str())
        .build()
    }));
    payloads
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn state() -> PresenceState {
    PresenceState::new(
      "abcd1234".to_owned(),
      "Recorder".to_owned(),
      Some("eu".to_owned()),
      vec!["vp9".to_owned(), "vp8".to_owned()],
      vec!["urn:example:feature".to_owned()],
    )
  }

  fn text_of(payloads: &[Element], name: &str) -> String {
    payloads.iter().find(|payload| payload.name() == name).unwrap().text()
  }

  #[test]
  fn the_muc_extension_is_for_the_joining_presence_only() {
    let state = state();
    let has_muc = |payloads: &[Element]| payloads.iter().any(|payload| payload.is("x", ns::MUC));
    assert!(has_muc(&state.payloads(true)));
    assert!(!has_muc(&state.payloads(false)));
  }

  #[test]
  fn advertises_the_codecs_we_can_receive() {
    let payloads = state().payloads(false);
    assert_eq!(text_of(&payloads, "jitsi_participant_codecType"), "vp9");
    assert_eq!(text_of(&payloads, "jitsi_participant_codecList"), "vp9,vp8");
    assert_eq!(text_of(&payloads, "nick"), "Recorder");
    assert_eq!(text_of(&payloads, "jitsi_participant_region"), "eu");
    assert!(payloads.iter().any(|payload| payload.name() == "feature"));
  }

  #[test]
  fn muting_updates_the_source_info_and_the_legacy_elements() {
    let mut state = state();
    let source_info =
      |state: &PresenceState| serde_json::from_str::<serde_json::Value>(&state.source_info()).unwrap();
    assert_eq!(
      source_info(&state),
      serde_json::json!({ "abcd1234-a0": { "muted": false }, "abcd1234-v0": { "muted": false } })
    );

    state.set_muted(MediaType::Audio, true);
    assert_eq!(
      source_info(&state),
      serde_json::json!({ "abcd1234-a0": { "muted": true }, "abcd1234-v0": { "muted": false } })
    );
    let payloads = state.payloads(false);
    assert_eq!(text_of(&payloads, "audiomuted"), "true");
    assert_eq!(text_of(&payloads, "videomuted"), "false");
    assert_eq!(
      serde_json::from_str::<serde_json::Value>(&text_of(&payloads, "SourceInfo")).unwrap(),
      source_info(&state)
    );

    state.set_muted(MediaType::VideoScreenshare, true);
    state.set_muted(MediaType::Audio, false);
    assert_eq!(
      source_info(&state),
      serde_json::json!({ "abcd1234-a0": { "muted": false }, "abcd1234-v0": { "muted": true } })
    );
  }
}
