use std::sync::{
  atomic::{AtomicU8, Ordering},
  Arc,
};

use anyhow::{bail, Context, Result};
use gstreamer::prelude::*;
use itertools::Itertools as _;
use jitsi_xmpp_parsers::jingle_dtls_srtp::Fingerprint as SignaledFingerprint;
use sha2::{Digest as _, Sha256};
use tracing::{debug, error};
use xmpp_parsers::jingle_dtls_srtp::Setup;

use super::offer::{Fingerprint, HashAlgo};

pub(super) struct DtlsIdentity {
  /// Certificate and private key, as `dtlssrtpdec` takes them.
  pub(super) pem: String,
  /// SHA-256 of the certificate.
  pub(super) fingerprint: Vec<u8>,
}

impl DtlsIdentity {
  pub(super) fn generate() -> Result<Self> {
    let key_pair = rcgen::KeyPair::generate()?;
    let params = rcgen::CertificateParams::new(vec!["gst-meet".to_owned()])?;
    let cert = params.self_signed(&key_pair)?;
    let fingerprint = Sha256::digest(cert.der()).to_vec();
    debug!("local DTLS fingerprint: {}", hex_colon(&fingerprint));
    Ok(Self {
      pem: format!("{}\n{}", cert.pem(), key_pair.serialize_pem()),
      fingerprint,
    })
  }
}

/// Checks the first certificate of `pem_text` against `expected`, with the hash algorithm the offer named.
pub(super) fn verify_fingerprint(expected: &Fingerprint, pem_text: &str) -> Result<()> {
  let blocks = pem::parse_many(pem_text).context("parsing peer DTLS PEM")?;
  let der = blocks
    .iter()
    .find(|block| block.tag() == "CERTIFICATE")
    .map(|block| block.contents())
    .context("no CERTIFICATE block in peer PEM")?;
  let actual = expected.algo.digest(der);
  if actual != expected.value {
    bail!(
      "expected {:?} {}, got {}",
      expected.algo,
      hex_colon(&expected.value),
      hex_colon(&actual)
    );
  }
  Ok(())
}

/// Of the fingerprints of a transport (RFC 8122 allows one per hash function) the one with the strongest hash we
/// can check: the certificate is verified against that one. Also returns the DTLS role it names, if it does,
/// otherwise the first of the others to name one.
pub(crate) fn pick_fingerprint(fingerprints: &[SignaledFingerprint]) -> Result<(Fingerprint, Option<Setup>)> {
  if fingerprints.is_empty() {
    bail!("the offer has no DTLS fingerprint");
  }
  let mut usable = Vec::new();
  let mut rejected = Vec::new();
  for fingerprint in fingerprints {
    match HashAlgo::from_xmpp(&fingerprint.hash) {
      Ok(algo) if fingerprint.value.len() == algo.digest(&[]).len() => usable.push((algo, fingerprint)),
      Ok(algo) => rejected.push(format!("a {:?} fingerprint of {} bytes", algo, fingerprint.value.len())),
      Err(e) => rejected.push(format!("{e}")),
    }
  }
  let Some(&(algo, chosen)) = usable.iter().max_by_key(|(algo, _)| strength(*algo)) else {
    bail!(
      "none of the DTLS fingerprints of the offer can be used: {}",
      rejected.join(", ")
    );
  };
  let setup = chosen
    .setup
    .clone()
    .or_else(|| fingerprints.iter().find_map(|fingerprint| fingerprint.setup.clone()));
  Ok((
    Fingerprint {
      algo,
      value: chosen.value.clone(),
    },
    setup,
  ))
}

fn strength(algo: HashAlgo) -> u8 {
  match algo {
    HashAlgo::Sha1 => 1,
    HashAlgo::Sha224 => 2,
    HashAlgo::Sha256 => 3,
    HashAlgo::Sha384 => 4,
    HashAlgo::Sha512 => 5,
  }
}

fn hex_colon(bytes: &[u8]) -> String {
  hex::encode_upper(bytes)
    .as_bytes()
    .chunks(2)
    .map(|chunk| std::str::from_utf8(chunk).expect("hex output is valid utf-8"))
    .join(":")
}

const PENDING: u8 = 0;
const VERIFIED: u8 = 1;
const REJECTED: u8 = 2;

/// Keeps what the peer sends from being used, and what we send from reaching it, until its certificate has
/// matched the signaled fingerprint.
///
/// `dtlssrtpdec` accepts any certificate (it is up to the application to look at `peer-pem`), so after a mismatch
/// both directions are keyed: nothing that is sent must be encrypted to an impostor, nor anything it sends be
/// believed.
pub(super) struct FingerprintGate {
  state: AtomicU8,
  expected: Fingerprint,
  decoder: glib::WeakRef<gstreamer::Element>,
  on_mismatch: Box<dyn Fn(String) + Send + Sync>,
}

impl FingerprintGate {
  /// The certificate shows up in `peer-pem` during the handshake, before the keys that decrypt anything exist.
  fn check(&self) -> u8 {
    let state = self.state.load(Ordering::SeqCst);
    if state != PENDING {
      return state;
    }
    let Some(decoder) = self.decoder.upgrade() else {
      return PENDING;
    };
    let Some(pem) = decoder
      .property::<Option<String>>("peer-pem")
      .filter(|pem| !pem.is_empty())
    else {
      return PENDING;
    };
    match verify_fingerprint(&self.expected, &pem) {
      Ok(()) => {
        if self
          .state
          .compare_exchange(PENDING, VERIFIED, Ordering::SeqCst, Ordering::SeqCst)
          .is_ok()
        {
          debug!("DTLS peer fingerprint verified");
        }
        VERIFIED
      },
      Err(e) => {
        if self
          .state
          .compare_exchange(PENDING, REJECTED, Ordering::SeqCst, Ordering::SeqCst)
          .is_ok()
        {
          error!("DTLS peer fingerprint verification failed: {:#}", e);
          (self.on_mismatch)(format!("{e:#}"));
        }
        REJECTED
      },
    }
  }

  /// Drops every buffer that passes `pad` until the peer's certificate has been verified, and for good if it does
  /// not match.
  pub(super) fn gate_pad(self: &Arc<Self>, pad: &gstreamer::Pad) {
    let gate = self.clone();
    pad.add_probe(
      gstreamer::PadProbeType::BUFFER | gstreamer::PadProbeType::BUFFER_LIST,
      move |_, _| {
        if gate.check() == VERIFIED {
          gstreamer::PadProbeReturn::Ok
        }
        else {
          gstreamer::PadProbeReturn::Drop
        }
      },
    );
  }

  /// What `encoder` is given to send on the sink pad `name` (`rtp_sink_0`, `rtcp_sink_0`) is held back like
  /// what the decoder produces. The handshake itself comes out of the encoder's `src` pad and is not touched.
  pub(super) fn gate_encoder_sink(self: &Arc<Self>, encoder: &gstreamer::Element, name: &str) -> Result<()> {
    let pad = encoder
      .pads()
      .into_iter()
      .find(|pad| pad.name() == name)
      .with_context(|| format!("dtlssrtpenc has no {name} pad"))?;
    self.gate_pad(&pad);
    Ok(())
  }
}

/// Drops everything `decoder` outputs until the peer's certificate has been verified, and reports a mismatch
/// through `on_mismatch` (once). The gate is also what holds back the encoder that goes with the decoder.
pub(super) fn gate_decoder(
  decoder: &gstreamer::Element,
  expected: Fingerprint,
  on_mismatch: impl Fn(String) + Send + Sync + 'static,
) -> Result<Arc<FingerprintGate>> {
  let gate = Arc::new(FingerprintGate {
    state: AtomicU8::new(PENDING),
    expected,
    decoder: decoder.downgrade(),
    on_mismatch: Box::new(on_mismatch),
  });

  for name in ["rtp_src", "rtcp_src"] {
    let pad = decoder
      .static_pad(name)
      .with_context(|| format!("dtlssrtpdec has no {name} pad"))?;
    gate.gate_pad(&pad);
  }
  // A mismatch is reported when the certificate arrives, whether or not anything follows it.
  decoder.connect_notify(Some("peer-pem"), {
    let gate = gate.clone();
    move |_, _| {
      gate.check();
    }
  });
  Ok(gate)
}
