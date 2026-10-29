use std::collections::BTreeMap;

use crate::{
  error::SourceEvent,
  pipeline::RemoteSourceDesc,
  source::RemoteSource,
};

/// The media sources of the conference that the focus has announced to us, by SSRC. It outlives the media sessions:
/// a new session starts out with the sources that are known.
#[derive(Debug, Default)]
pub(crate) struct SourceMap {
  sources: BTreeMap<u32, RemoteSourceDesc>,
}

impl SourceMap {
  pub(crate) fn descriptions(&self) -> Vec<RemoteSourceDesc> {
    self.sources.values().cloned().collect()
  }

  pub(crate) fn remote_sources(&self) -> Vec<RemoteSource> {
    self.sources.values().map(|desc| desc.source.clone()).collect()
  }

  /// Makes `sources` the whole set, as a new session does. A source that changed is removed and added.
  pub(crate) fn replace(&mut self, sources: Vec<RemoteSourceDesc>) -> Vec<SourceEvent> {
    let mut events = vec![];
    let new: BTreeMap<u32, RemoteSourceDesc> = sources.into_iter().map(|desc| (desc.source.ssrc, desc)).collect();
    for (ssrc, old) in &self.sources {
      if new.get(ssrc).map(|desc| &desc.source) != Some(&old.source) {
        events.push(SourceEvent::Removed(old.source.clone()));
      }
    }
    for (ssrc, desc) in &new {
      if self.sources.get(ssrc).map(|old| &old.source) != Some(&desc.source) {
        events.push(SourceEvent::Added(desc.source.clone()));
      }
    }
    self.sources = new;
    events
  }

  pub(crate) fn add(&mut self, sources: Vec<RemoteSourceDesc>) -> Vec<SourceEvent> {
    let mut events = vec![];
    for desc in sources {
      match self.sources.insert(desc.source.ssrc, desc.clone()) {
        Some(old) if old.source == desc.source => continue,
        Some(old) => events.push(SourceEvent::Removed(old.source)),
        None => {},
      }
      events.push(SourceEvent::Added(desc.source));
    }
    events
  }

  /// Sources that are not known are skipped.
  pub(crate) fn remove(&mut self, ssrcs: &[u32]) -> Vec<SourceEvent> {
    ssrcs
      .iter()
      .filter_map(|ssrc| self.sources.remove(ssrc))
      .map(|desc| SourceEvent::Removed(desc.source))
      .collect()
  }

  pub(crate) fn remove_participant(&mut self, participant_id: &str) -> Vec<SourceEvent> {
    let ssrcs: Vec<u32> = self
      .sources
      .values()
      .filter(|desc| desc.source.participant_id.as_deref() == Some(participant_id))
      .map(|desc| desc.source.ssrc)
      .collect();
    self.remove(&ssrcs)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::source::MediaType;

  pub(crate) fn desc(participant: &str, ssrc: u32, media_type: MediaType, name: &str) -> RemoteSourceDesc {
    RemoteSourceDesc {
      source: RemoteSource {
        participant_id: Some(participant.to_owned()),
        ssrc,
        media_type,
        source_name: Some(name.to_owned()),
      },
      rtx_ssrc: None,
    }
  }

  #[test]
  fn add_and_remove_report_what_changed() {
    let mut map = SourceMap::default();
    let events = map.add(vec![
      desc("a", 1, MediaType::Audio, "a-a0"),
      desc("a", 2, MediaType::Video, "a-v0"),
    ]);
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], SourceEvent::Added(source) if source.ssrc == 1));

    // The same source again is not news; a different one under the same SSRC replaces it.
    assert!(map.add(vec![desc("a", 1, MediaType::Audio, "a-a0")]).is_empty());
    let events = map.add(vec![desc("b", 1, MediaType::Audio, "b-a0")]);
    assert!(matches!(&events[0], SourceEvent::Removed(source) if source.participant_id.as_deref() == Some("a")));
    assert!(matches!(&events[1], SourceEvent::Added(source) if source.participant_id.as_deref() == Some("b")));

    assert_eq!(map.remove(&[2, 99]).len(), 1);
    assert_eq!(map.remote_sources().len(), 1);
  }

  #[test]
  fn replace_is_a_diff() {
    let mut map = SourceMap::default();
    map.add(vec![
      desc("a", 1, MediaType::Audio, "a-a0"),
      desc("b", 3, MediaType::Audio, "b-a0"),
    ]);
    let events = map.replace(vec![
      desc("a", 1, MediaType::Audio, "a-a0"),
      desc("c", 4, MediaType::Video, "c-v0"),
    ]);
    assert_eq!(events.len(), 2);
    assert!(matches!(&events[0], SourceEvent::Removed(source) if source.ssrc == 3));
    assert!(matches!(&events[1], SourceEvent::Added(source) if source.ssrc == 4));
  }

  #[test]
  fn a_participant_takes_its_sources_along() {
    let mut map = SourceMap::default();
    map.add(vec![
      desc("a", 1, MediaType::Audio, "a-a0"),
      desc("a", 2, MediaType::Video, "a-v0"),
      desc("b", 3, MediaType::Audio, "b-a0"),
    ]);
    assert_eq!(map.remove_participant("a").len(), 2);
    assert_eq!(map.remote_sources().len(), 1);
    assert!(map.remove_participant("a").is_empty());
  }
}
