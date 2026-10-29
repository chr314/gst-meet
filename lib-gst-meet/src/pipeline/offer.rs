use anyhow::{bail, Context, Result};
use ring::digest;
use sha2::{Digest as _, Sha224, Sha256, Sha384, Sha512};
use xmpp_parsers::{hashes::Algo, jingle_dtls_srtp::Setup, jingle_ice_udp::Candidate};

use super::codec::Codec;
use crate::source::RemoteSource;

pub(crate) struct SessionOffer {
  pub(crate) sid: String,
  pub(crate) bridge_session_id: Option<String>,
  /// Already filtered to the codecs that can be received and are configured.
  pub(crate) codecs: Vec<Codec>,
  pub(crate) hdrexts: HdrextIds,
  pub(crate) sources: Vec<RemoteSourceDesc>,
  pub(crate) ice: RemoteIce,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct HdrextIds {
  pub(crate) audio_level: Option<u16>,
  pub(crate) transport_cc_audio: Option<u16>,
  pub(crate) transport_cc_video: Option<u16>,
}

#[derive(Debug, Clone)]
pub(crate) struct RemoteSourceDesc {
  pub(crate) source: RemoteSource,
  /// The retransmission SSRC of a FID group.
  pub(crate) rtx_ssrc: Option<u32>,
}

pub(crate) struct RemoteIce {
  pub(crate) ufrag: String,
  pub(crate) pwd: String,
  pub(crate) candidates: Vec<Candidate>,
  pub(crate) fingerprint: Fingerprint,
  pub(crate) setup: Setup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashAlgo {
  Sha1,
  Sha224,
  Sha256,
  Sha384,
  Sha512,
}

impl HashAlgo {
  /// The algorithm of an `a=fingerprint` hash name. Only the SHA-1/2 family is used by DTLS-SRTP peers.
  pub(crate) fn from_xmpp(algo: &Algo) -> Result<Self> {
    Ok(match algo {
      Algo::Sha_1 => Self::Sha1,
      Algo::Sha_256 => Self::Sha256,
      Algo::Sha_512 => Self::Sha512,
      Algo::Unknown(name) => match name.to_ascii_lowercase().as_str() {
        "sha-1" => Self::Sha1,
        "sha-224" => Self::Sha224,
        "sha-256" => Self::Sha256,
        "sha-384" => Self::Sha384,
        "sha-512" => Self::Sha512,
        _ => bail!("unsupported fingerprint hash: {name}"),
      },
      other => bail!("unsupported fingerprint hash: {other:?}"),
    })
  }

  pub(crate) fn digest(self, data: &[u8]) -> Vec<u8> {
    match self {
      Self::Sha1 => digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, data)
        .as_ref()
        .to_vec(),
      Self::Sha224 => Sha224::digest(data).to_vec(),
      Self::Sha256 => Sha256::digest(data).to_vec(),
      Self::Sha384 => Sha384::digest(data).to_vec(),
      Self::Sha512 => Sha512::digest(data).to_vec(),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Fingerprint {
  pub(crate) algo: HashAlgo,
  pub(crate) value: Vec<u8>,
}

/// The participant of an `<ssrc-info owner=…>`: the endpoint id, which is the resource of the occupant JID.
/// Sources owned by the bridge itself have none.
pub(crate) fn participant_id_for_owner(owner: String) -> Result<Option<String>> {
  if owner == "jvb" {
    Ok(None)
  }
  else {
    Ok(Some(if owner.contains('/') {
      owner.split('/').nth(1).context("invalid ssrc-info owner")?.to_owned()
    }
    else {
      owner
    }))
  }
}
