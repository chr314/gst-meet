/// A media source announced by the bridge for one participant (or for the bridge itself, with no
/// `participant_id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSource {
  pub participant_id: Option<String>,
  pub ssrc: u32,
  pub media_type: MediaType,
  pub source_name: Option<String>,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
#[repr(C)]
pub enum MediaType {
  Audio,
  Video,
  AudioScreenshare,
  VideoScreenshare,
}

impl MediaType {
  pub fn classify(media: &str, video_type: Option<&str>) -> MediaType {
    match (media, video_type) {
      ("audio", Some("desktop")) => MediaType::AudioScreenshare,
      ("audio", _) => MediaType::Audio,
      ("video", Some("desktop")) => MediaType::VideoScreenshare,
      _ => MediaType::Video,
    }
  }

  pub fn is_audio(self) -> bool {
    matches!(self, MediaType::Audio | MediaType::AudioScreenshare)
  }

  pub fn is_video(self) -> bool {
    matches!(self, MediaType::Video | MediaType::VideoScreenshare)
  }
}
