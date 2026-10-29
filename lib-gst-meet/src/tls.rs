//! TLS for the WebSocket connections and for the TURN-over-TLS tunnel.
//!
//! Cargo unifies the features of a dependency graph, so more than one `tls-*` feature can be on without anybody
//! having asked for it. They compose:
//!
//! - `tls-rustls-native-roots` and `tls-rustls-webpki-roots` select rustls; if both are on, a server is trusted if
//!   either set of roots trusts it.
//! - `tls-native` and `tls-native-vendored` select native-tls.
//! - If rustls and native-tls are both on, rustls is used: it is the default, and the only one of the two that
//!   can be built into every binary regardless of the system's TLS library.

use anyhow::Result;
use tokio_tungstenite::Connector;

#[cfg(not(any(
  feature = "tls-rustls-native-roots",
  feature = "tls-rustls-webpki-roots",
  feature = "tls-native",
  feature = "tls-native-vendored"
)))]
compile_error!(
  "lib-gst-meet needs a TLS backend: enable one of the features tls-rustls-native-roots, tls-rustls-webpki-roots, \
   tls-native or tls-native-vendored"
);

#[cfg(all(
  any(feature = "tls-native", feature = "tls-native-vendored"),
  not(any(feature = "tls-rustls-native-roots", feature = "tls-rustls-webpki-roots"))
))]
pub(crate) use native_backend::TlsClient;
#[cfg(any(feature = "tls-rustls-native-roots", feature = "tls-rustls-webpki-roots"))]
pub(crate) use rustls_backend::TlsClient;

fn check_insecure(insecure: bool) -> Result<()> {
  #[cfg(not(feature = "tls-insecure"))]
  if insecure {
    anyhow::bail!("Insecure TLS mode can only be enabled if the tls-insecure feature was enabled at compile time.");
  }
  let _ = insecure;
  Ok(())
}

#[cfg(any(feature = "tls-rustls-native-roots", feature = "tls-rustls-webpki-roots"))]
mod rustls_backend {
  use std::sync::Arc;

  use anyhow::{Context, Result};
  use tokio::net::TcpStream;
  use tokio_tungstenite::Connector;
  #[cfg(feature = "tls-rustls-native-roots")]
  use tracing::{debug, warn};

  #[cfg(feature = "tls-rustls-native-roots")]
  use super::Cache;

  #[cfg(feature = "tls-rustls-native-roots")]
  const NATIVE_ROOTS_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(3600);

  pub(crate) struct TlsClient {
    config: Arc<rustls::ClientConfig>,
  }

  impl TlsClient {
    /// Reads the system's root certificates if `tls-rustls-native-roots` is enabled, which blocks (unless they were
    /// read recently): call it from a blocking context.
    pub(crate) fn new(insecure: bool) -> Result<Self> {
      super::check_insecure(insecure)?;
      Self::with_roots(roots(insecure)?, insecure)
    }

    fn with_roots(roots: rustls::RootCertStore, insecure: bool) -> Result<Self> {
      #[cfg_attr(not(feature = "tls-insecure"), allow(unused_mut))]
      let mut config = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .context("failed to select TLS protocol versions")?
        .with_root_certificates(roots)
        .with_no_client_auth();
      #[cfg(feature = "tls-insecure")]
      if insecure {
        config
          .dangerous()
          .set_certificate_verifier(Arc::new(InsecureServerCertVerifier(crypto_provider())));
      }
      let _ = insecure;
      Ok(Self {
        config: Arc::new(config),
      })
    }

    #[cfg(test)]
    pub(crate) fn trusting(roots: Vec<rustls::pki_types::CertificateDer<'static>>) -> Result<Self> {
      let mut store = rustls::RootCertStore::empty();
      for root in roots {
        store.add(root)?;
      }
      Self::with_roots(store, false)
    }

    pub(crate) fn connector(&self) -> Connector {
      Connector::Rustls(self.config.clone())
    }

    pub(crate) async fn connect(
      &self,
      host: &str,
      tcp: TcpStream,
    ) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
      let server_name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .with_context(|| format!("invalid TLS server name {host}"))?;
      tokio_rustls::TlsConnector::from(self.config.clone())
        .connect(server_name, tcp)
        .await
        .with_context(|| format!("TLS handshake with {host} failed"))
    }
  }

  fn roots(insecure: bool) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    #[cfg(feature = "tls-rustls-webpki-roots")]
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    #[cfg(feature = "tls-rustls-native-roots")]
    match native_roots() {
      Ok(native) => roots.extend(native.roots.iter().cloned()),
      // Nothing to verify with is only an error if something is verified.
      Err(e) if roots.is_empty() && !insecure => return Err(e),
      Err(e) => warn!("not using the native root certificates: {:#}", e),
    }
    let _ = insecure;
    Ok(roots)
  }

  #[cfg(feature = "tls-rustls-native-roots")]
  fn native_roots() -> Result<Arc<rustls::RootCertStore>> {
    static CACHE: Cache<Arc<rustls::RootCertStore>> = Cache::new();
    CACHE.get_or_load(NATIVE_ROOTS_MAX_AGE, || {
      let native_certs = rustls_native_certs::load_native_certs();
      for error in &native_certs.errors {
        warn!("error while loading native root certificates: {}", error);
      }
      let mut roots = rustls::RootCertStore::empty();
      let (added, ignored) = roots.add_parsable_certificates(native_certs.certs);
      if ignored > 0 {
        warn!("ignored {} unparseable native root certificates", ignored);
      }
      debug!("loaded {} native root certificates", added);
      if added == 0 {
        anyhow::bail!("no native root certificates could be loaded");
      }
      Ok(Arc::new(roots))
    })
  }

  fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    // Explicit provider: ClientConfig::builder() panics when the feature unification of the dependency graph
    // enables more than one rustls crypto backend.
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
  }

  /// Accepts any server certificate, but still checks handshake signatures against the presented key.
  #[cfg(feature = "tls-insecure")]
  #[derive(Debug)]
  struct InsecureServerCertVerifier(Arc<rustls::crypto::CryptoProvider>);

  #[cfg(feature = "tls-insecure")]
  impl rustls::client::danger::ServerCertVerifier for InsecureServerCertVerifier {
    fn verify_server_cert(
      &self,
      _end_entity: &rustls::pki_types::CertificateDer<'_>,
      _intermediates: &[rustls::pki_types::CertificateDer<'_>],
      _server_name: &rustls::pki_types::ServerName<'_>,
      _ocsp_response: &[u8],
      _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
      Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
      &self,
      message: &[u8],
      cert: &rustls::pki_types::CertificateDer<'_>,
      dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
      &self,
      message: &[u8],
      cert: &rustls::pki_types::CertificateDer<'_>,
      dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      self.0.signature_verification_algorithms.supported_schemes()
    }
  }
}

#[cfg(all(
  any(feature = "tls-native", feature = "tls-native-vendored"),
  not(any(feature = "tls-rustls-native-roots", feature = "tls-rustls-webpki-roots"))
))]
mod native_backend {
  use anyhow::{Context, Result};
  use tokio::net::TcpStream;
  use tokio_tungstenite::Connector;

  pub(crate) struct TlsClient {
    connector: native_tls::TlsConnector,
  }

  impl TlsClient {
    pub(crate) fn new(insecure: bool) -> Result<Self> {
      super::check_insecure(insecure)?;
      let mut builder = native_tls::TlsConnector::builder();
      builder.min_protocol_version(Some(native_tls::Protocol::Tlsv12));
      #[cfg(feature = "tls-insecure")]
      if insecure {
        builder.danger_accept_invalid_certs(true);
        builder.danger_accept_invalid_hostnames(true);
      }
      Ok(Self {
        connector: builder.build().context("failed to build native TLS connector")?,
      })
    }

    pub(crate) fn connector(&self) -> Connector {
      Connector::NativeTls(self.connector.clone())
    }

    pub(crate) async fn connect(&self, host: &str, tcp: TcpStream) -> Result<tokio_native_tls::TlsStream<TcpStream>> {
      tokio_native_tls::TlsConnector::from(self.connector.clone())
        .connect(host, tcp)
        .await
        .with_context(|| format!("TLS handshake with {host} failed"))
    }
  }
}

/// May read the system's root certificates (once an hour at most), which blocks.
pub(crate) fn wss_connector(insecure: bool) -> Result<Connector> {
  TlsClient::new(insecure).map(|client| client.connector())
}

/// Remembers a value that is expensive to produce, for a while. Failures are not remembered.
#[cfg(any(feature = "tls-rustls-native-roots", test))]
struct Cache<T> {
  slot: std::sync::Mutex<Option<(std::time::Instant, T)>>,
}

#[cfg(any(feature = "tls-rustls-native-roots", test))]
impl<T: Clone> Cache<T> {
  const fn new() -> Self {
    Self {
      slot: std::sync::Mutex::new(None),
    }
  }

  /// Concurrent callers wait for the one that loads, rather than all loading.
  fn get_or_load(&self, max_age: std::time::Duration, load: impl FnOnce() -> Result<T>) -> Result<T> {
    let mut slot = self.slot.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((loaded, value)) = &*slot {
      if loaded.elapsed() < max_age {
        return Ok(value.clone());
      }
    }
    let value = load()?;
    *slot = Some((std::time::Instant::now(), value.clone()));
    Ok(value)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::Cell, time::Duration};

  use super::*;

  #[cfg(feature = "tls-insecure")]
  #[test]
  fn builds_an_insecure_connector() {
    assert!(wss_connector(true).is_ok());
  }

  #[cfg(not(feature = "tls-insecure"))]
  #[test]
  fn refuses_insecure_mode_without_the_feature() {
    assert!(wss_connector(true).is_err());
    assert!(TlsClient::new(true).is_err());
  }

  #[cfg(feature = "tls-rustls-webpki-roots")]
  #[test]
  fn builds_a_connector_with_the_bundled_roots() {
    assert!(matches!(wss_connector(false), Ok(Connector::Rustls(_))));
  }

  #[cfg(any(feature = "tls-rustls-native-roots", feature = "tls-rustls-webpki-roots"))]
  #[test]
  fn rustls_is_used_whenever_it_is_enabled() {
    if let Ok(connector) = wss_connector(cfg!(feature = "tls-insecure")) {
      assert!(matches!(connector, Connector::Rustls(_)));
    }
  }

  #[test]
  fn a_loaded_value_is_kept_until_it_is_too_old_and_failures_are_not_kept() {
    let cache = Cache::new();
    let loads = Cell::new(0);
    let load = || {
      loads.set(loads.get() + 1);
      Ok(loads.get())
    };
    let hour = Duration::from_secs(3600);
    assert_eq!(cache.get_or_load(hour, load).unwrap(), 1);
    assert_eq!(cache.get_or_load(hour, load).unwrap(), 1);
    assert_eq!(cache.get_or_load(Duration::ZERO, load).unwrap(), 2, "too old");
    assert_eq!(cache.get_or_load(hour, load).unwrap(), 2);

    let failing = Cache::<u32>::new();
    assert!(failing.get_or_load(hour, || anyhow::bail!("no roots")).is_err());
    assert_eq!(failing.get_or_load(hour, || Ok(7)).unwrap(), 7, "tried again");
  }
}
