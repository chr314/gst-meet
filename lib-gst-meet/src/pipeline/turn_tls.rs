//! libnice speaks TURN over UDP and plain TCP only. For `turns:` servers a listener on the loopback interface
//! stands in for the server: libnice connects to it over TCP, and every connection it accepts is relayed over
//! TLS to the real server.

use std::{future::Future, io, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{
  io::{copy_bidirectional, AsyncRead, AsyncWrite},
  net::{TcpListener, TcpStream},
  task::{JoinHandle, JoinSet},
};
use tracing::{debug, warn};

/// How long to wait after a failed `accept`, which doubles with every failure in a row up to the maximum.
const ACCEPT_BACKOFF_MIN: Duration = Duration::from_millis(50);
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);
/// How long a connection of libnice may wait for the server to be reached and the TLS handshake to be through,
/// each. libnice has its own timeouts, but only for the TCP connection to the tunnel, which is up at once.
const SETUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Closes the listener and every relayed connection when dropped.
pub(super) struct TlsTunnel {
  local_addr: SocketAddr,
  task: JoinHandle<()>,
}

impl Drop for TlsTunnel {
  fn drop(&mut self) {
    self.task.abort();
  }
}

/// Where the connections of libnice come from.
pub(super) trait Accept: Send + 'static {
  fn accept(&mut self) -> impl Future<Output = io::Result<TcpStream>> + Send;
}

impl Accept for TcpListener {
  async fn accept(&mut self) -> io::Result<TcpStream> {
    TcpListener::accept(self).await.map(|(stream, _)| stream)
  }
}

impl TlsTunnel {
  /// `wrap` turns the TCP connection to `target` into the stream that is relayed to libnice.
  pub(super) async fn start<Wrap, Fut, Stream>(target: SocketAddr, wrap: Wrap) -> Result<Self>
  where
    Wrap: Fn(TcpStream) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Stream>> + Send,
    Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static,
  {
    let listener = TcpListener::bind(("127.0.0.1", 0))
      .await
      .context("failed to bind the TURN tunnel")?;
    let local_addr = listener.local_addr()?;
    Ok(Self::serve(listener, local_addr, target, SETUP_TIMEOUT, wrap))
  }

  pub(super) fn serve<Wrap, Fut, Stream>(
    mut acceptor: impl Accept,
    local_addr: SocketAddr,
    target: SocketAddr,
    setup_timeout: Duration,
    wrap: Wrap,
  ) -> Self
  where
    Wrap: Fn(TcpStream) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Stream>> + Send,
    Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static,
  {
    debug!("TURN tunnel on {} to {}", local_addr, target);

    let wrap = Arc::new(wrap);
    let task = tokio::spawn(async move {
      // Dropping the set, when this task is aborted, aborts the relays too.
      let mut relays = JoinSet::new();
      let mut failures = 0u32;
      loop {
        tokio::select! {
          accepted = acceptor.accept() => {
            let mut local = match accepted {
              Ok(local) => {
                failures = 0;
                local
              },
              Err(e) => {
                // These are transient (out of file descriptors, a connection that was reset before it was
                // accepted): the other relays carry the allocation of the session and must not go with it.
                failures = failures.saturating_add(1);
                let backoff = accept_backoff(failures);
                if failures == 1 || failures & 31 == 0 {
                  warn!(
                    "TURN tunnel on {} failed to accept a connection ({} in a row), retrying in {:?}: {}",
                    local_addr, failures, backoff, e
                  );
                }
                tokio::time::sleep(backoff).await;
                continue;
              },
            };
            let wrap = wrap.clone();
            relays.spawn(async move {
              let remote = match tokio::time::timeout(setup_timeout, TcpStream::connect(target)).await {
                Ok(Ok(remote)) => remote,
                Ok(Err(e)) => {
                  warn!("TURN tunnel to {} could not connect: {}", target, e);
                  return;
                },
                Err(_) => {
                  warn!("TURN tunnel to {} could not connect within {:?}", target, setup_timeout);
                  return;
                },
              };
              let mut remote = match tokio::time::timeout(setup_timeout, wrap(remote)).await {
                Ok(Ok(remote)) => remote,
                Ok(Err(e)) => {
                  warn!("TURN tunnel to {} could not be established: {:#}", target, e);
                  return;
                },
                Err(_) => {
                  warn!("TURN tunnel to {} was not established within {:?}", target, setup_timeout);
                  return;
                },
              };
              match copy_bidirectional(&mut local, &mut remote).await {
                Ok((up, down)) => debug!("TURN tunnel connection to {} ended after {} up, {} down", target, up, down),
                Err(e) => debug!("TURN tunnel connection to {} ended: {}", target, e),
              }
            });
          },
          Some(_) = relays.join_next(), if !relays.is_empty() => {},
        }
      }
    });
    Self { local_addr, task }
  }

  /// Where libnice should connect.
  pub(super) fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

fn accept_backoff(failures: u32) -> Duration {
  ACCEPT_BACKOFF_MIN
    .saturating_mul(1 << failures.saturating_sub(1).min(8))
    .min(ACCEPT_BACKOFF_MAX)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn the_accept_backoff_doubles_up_to_a_limit() {
    let backoffs: Vec<_> = (1..=7).map(accept_backoff).collect();
    assert_eq!(
      backoffs,
      [50, 100, 200, 400, 800, 1000, 1000].map(Duration::from_millis)
    );
    assert_eq!(accept_backoff(u32::MAX), ACCEPT_BACKOFF_MAX);
  }
}
