use anyhow::{bail, Result};
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
  /// Elements the application put in our presence, in the order they were first added.
  extensions: Vec<Element>,
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
      extensions: vec![],
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

  /// Puts `element` in our presence, in place of the one with the same name and namespace if there is one. Returns
  /// whether the presence changed. The elements this state writes itself cannot be replaced.
  pub(crate) fn set_extension(&mut self, element: Element) -> Result<bool> {
    let same = |other: &Element| other.name() == element.name() && other.ns() == element.ns();
    if self.own_payloads(true).iter().any(same) {
      bail!("<{}> in {} is part of the presence the library writes", element.name(), element.ns());
    }
    match self.extensions.iter_mut().find(|existing| same(existing)) {
      Some(existing) if *existing == element => Ok(false),
      Some(existing) => {
        *existing = element;
        Ok(true)
      },
      None => {
        self.extensions.push(element);
        Ok(true)
      },
    }
  }

  /// Takes the element with this name and namespace out of our presence. Returns whether there was one.
  pub(crate) fn remove_extension(&mut self, name: &str, namespace: &str) -> bool {
    let before = self.extensions.len();
    self
      .extensions
      .retain(|element| !(element.name() == name && element.ns() == namespace));
    self.extensions.len() != before
  }

  /// Only the joining presence (`initial`) asks for the MUC extension: the room answers it with the occupants it has,
  /// and Prosody sends them all again if a later presence asks again.
  pub(crate) fn payloads(&self, initial: bool) -> Vec<Element> {
    let mut payloads = self.own_payloads(initial);
    payloads.extend(self.extensions.iter().cloned());
    payloads
  }

  fn own_payloads(&self, initial: bool) -> Vec<Element> {
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
    if !self.extra_features.is_empty() {
      payloads.push(
        Element::builder("features", ns::DEFAULT_NS)
          .append_all(self.extra_features.iter().map(|var| {
            Element::builder("feature", ns::DEFAULT_NS)
              .attr(::minidom::rxml::xml_ncname!("var").to_owned(), var.as_str())
              .build()
          }))
          .build(),
      );
    }
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
  }

  #[test]
  fn extra_features_are_wrapped_in_features() {
    let payloads = state().payloads(false);
    let features = payloads.iter().find(|payload| payload.is("features", ns::DEFAULT_NS)).unwrap();
    let vars: Vec<_> = features
      .children()
      .filter(|child| child.is("feature", ns::DEFAULT_NS))
      .filter_map(|child| child.attr("var"))
      .collect();
    assert_eq!(vars, ["urn:example:feature"]);
    assert!(!payloads.iter().any(|payload| payload.name() == "feature"));
  }

  #[test]
  fn no_features_element_without_extra_features() {
    let mut state = state();
    state.extra_features.clear();
    assert!(!state.payloads(false).iter().any(|payload| payload.name() == "features"));
  }

  fn plain_element(name: &str, value: &str) -> Element {
    Element::builder(name, ns::DEFAULT_NS).append(value).build()
  }

  #[test]
  fn extensions_follow_the_presence_the_library_writes() {
    let mut state = state();
    assert!(state.set_extension(plain_element("session_id", "s1")).unwrap());
    assert!(state.set_extension(plain_element("mode", "file")).unwrap());

    let payloads = state.payloads(false);
    let names: Vec<_> = payloads.iter().rev().take(2).map(|payload| payload.name()).collect();
    assert_eq!(names, ["mode", "session_id"]);
    assert_eq!(text_of(&payloads, "session_id"), "s1");
    assert_eq!(text_of(&state.payloads(true), "session_id"), "s1");
  }

  #[test]
  fn an_extension_replaces_the_one_with_its_name_and_namespace() {
    let mut state = state();
    assert!(state.set_extension(plain_element("session_id", "s1")).unwrap());
    assert!(!state.set_extension(plain_element("session_id", "s1")).unwrap());
    assert!(state.set_extension(plain_element("session_id", "s2")).unwrap());
    assert!(state
      .set_extension(Element::builder("session_id", "urn:example").append("s3").build())
      .unwrap());

    let payloads = state.payloads(false);
    let session_ids: Vec<_> = payloads
      .iter()
      .filter(|payload| payload.name() == "session_id")
      .map(|payload| (payload.ns(), payload.text()))
      .collect();
    assert_eq!(
      session_ids,
      [
        (ns::DEFAULT_NS.to_owned(), "s2".to_owned()),
        ("urn:example".to_owned(), "s3".to_owned())
      ]
    );
  }

  #[test]
  fn removing_an_extension() {
    let mut state = state();
    state.set_extension(plain_element("session_id", "s1")).unwrap();
    assert!(!state.remove_extension("session_id", "urn:example"));
    assert!(state.remove_extension("session_id", ns::DEFAULT_NS));
    assert!(!state.remove_extension("session_id", ns::DEFAULT_NS));
    assert!(!state.payloads(false).iter().any(|payload| payload.name() == "session_id"));
  }

  #[test]
  fn the_elements_the_library_writes_cannot_be_replaced() {
    let mut state = state();
    assert!(state.set_extension(plain_element("audiomuted", "true")).is_err());
    assert!(state.set_extension(Muc::new().into()).is_err());
    assert_eq!(text_of(&state.payloads(false), "audiomuted"), "false");
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
