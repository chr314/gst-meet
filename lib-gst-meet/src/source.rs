#[derive(Debug, Clone)]
pub(crate) struct Source {
  pub(crate) ssrc: u32,
  pub(crate) participant_id: Option<String>,
  pub(crate) media_type: MediaType,
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

  pub(crate) fn jitsi_muted_presence_element_name(self) -> &'static str {
    match self {
      MediaType::Video | MediaType::VideoScreenshare => "videomuted",
      MediaType::Audio | MediaType::AudioScreenshare => "audiomuted",
    }
  }
}
