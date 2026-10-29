use std::{convert::TryFrom, time::Duration};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use jid::FullJid;
use minidom::Element;
use tokio::{
  sync::mpsc,
  time::{self, MissedTickBehavior},
};
use tracing::warn;
use xmpp_parsers::iq::Iq;

use crate::{stanza_filter::StanzaFilter, xmpp::iq::RequestError};

#[derive(Debug)]
pub(crate) struct Pinger {
  jid: FullJid,
  tx: mpsc::Sender<Element>,
}

impl Pinger {
  pub(crate) fn new(jid: FullJid, tx: mpsc::Sender<Element>) -> Pinger {
    Pinger { jid, tx }
  }
}

#[async_trait]
impl StanzaFilter for Pinger {
  #[tracing::instrument(level = "trace")]
  fn filter(&self, element: &Element) -> bool {
    element.is("iq", "jabber:client") && element.has_child("ping", "urn:xmpp:ping")
  }

  #[tracing::instrument(level = "trace", err)]
  async fn take(&self, element: Element) -> Result<()> {
    let iq = Iq::try_from(element)?;
    let result_iq = Iq::empty_result(
      iq.from().cloned().ok_or_else(|| anyhow!("iq missing from"))?,
      iq.id().to_owned(),
    )
    .with_from(self.jid.clone().into());
    self.tx.send(result_iq.into()).await?;
    Ok(())
  }
}

/// Mirrors lib-jitsi-meet (`strophe.ping.ts`): a ping every ten seconds unless the server was heard from within them,
/// five seconds to answer it, and two pings in a row without an answer end the connection.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Keepalive {
  pub(crate) interval: Duration,
  pub(crate) timeout: Duration,
  pub(crate) max_failures: u32,
}

impl Default for Keepalive {
  fn default() -> Self {
    Self {
      interval: Duration::from_secs(10),
      timeout: Duration::from_secs(5),
      max_failures: 2,
    }
  }
}

#[async_trait]
pub(crate) trait Server: Send + Sync {
  async fn idle_for(&self) -> Duration;

  /// Any answer shows that the server is there, an error reply included.
  async fn ping(&self, timeout: Duration) -> Result<(), RequestError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
  /// The server did not answer `max_failures` pings in a row.
  Dead,
  Closed,
}

pub(crate) async fn keepalive(server: &dyn Server, settings: Keepalive) -> Verdict {
  let mut ticks = time::interval(settings.interval);
  ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
  // The first tick completes immediately.
  ticks.tick().await;
  let mut failures = 0;
  loop {
    ticks.tick().await;
    if server.idle_for().await < settings.interval {
      failures = 0;
      continue;
    }
    match server.ping(settings.timeout).await {
      Ok(()) => failures = 0,
      Err(RequestError::ConnectionClosed) => return Verdict::Closed,
      Err(RequestError::Timeout) => {
        failures += 1;
        if failures >= settings.max_failures {
          return Verdict::Dead;
        }
        warn!(
          "the XMPP server did not answer a ping within {:?} ({} of {})",
          settings.timeout, failures, settings.max_failures
        );
      },
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{
    collections::VecDeque,
    sync::{
      atomic::{AtomicU32, Ordering},
      Mutex,
    },
  };

  use super::*;

  const FAST: Keepalive = Keepalive {
    interval: Duration::from_millis(20),
    timeout: Duration::from_millis(5),
    max_failures: 2,
  };

  /// A server whose answers are scripted; once the script is used up it answers every ping.
  struct Scripted {
    idle: Mutex<Duration>,
    answers: Mutex<VecDeque<Result<(), RequestError>>>,
    pings: AtomicU32,
  }

  impl Scripted {
    fn new(answers: impl IntoIterator<Item = Result<(), RequestError>>) -> Self {
      Self {
        idle: Mutex::new(Duration::from_secs(60)),
        answers: Mutex::new(answers.into_iter().collect()),
        pings: AtomicU32::new(0),
      }
    }
  }

  #[async_trait]
  impl Server for Scripted {
    async fn idle_for(&self) -> Duration {
      *self.idle.lock().unwrap()
    }

    async fn ping(&self, _timeout: Duration) -> Result<(), RequestError> {
      self.pings.fetch_add(1, Ordering::SeqCst);
      self.answers.lock().unwrap().pop_front().unwrap_or(Ok(()))
    }
  }

  async fn verdict(server: &Scripted) -> Verdict {
    tokio::time::timeout(Duration::from_secs(5), keepalive(server, FAST))
      .await
      .expect("a verdict")
  }

  #[tokio::test]
  async fn a_server_that_does_not_answer_twice_in_a_row_is_dead() {
    let server = Scripted::new([Err(RequestError::Timeout), Err(RequestError::Timeout)]);
    assert_eq!(verdict(&server).await, Verdict::Dead);
    assert_eq!(server.pings.load(Ordering::SeqCst), 2);
  }

  #[tokio::test]
  async fn an_answer_in_between_starts_the_count_again() {
    let server = Scripted::new([
      Err(RequestError::Timeout),
      Ok(()),
      Err(RequestError::Timeout),
      Ok(()),
      Err(RequestError::Timeout),
      Err(RequestError::Timeout),
    ]);
    assert_eq!(verdict(&server).await, Verdict::Dead);
    assert_eq!(server.pings.load(Ordering::SeqCst), 6);
  }

  #[tokio::test]
  async fn a_server_that_is_heard_from_is_not_pinged() {
    let server = Scripted::new([Err(RequestError::Timeout)]);
    *server.idle.lock().unwrap() = Duration::ZERO;
    let verdict = tokio::time::timeout(Duration::from_millis(150), keepalive(&server, FAST)).await;
    assert!(verdict.is_err(), "no verdict while the server talks");
    assert_eq!(server.pings.load(Ordering::SeqCst), 0);
  }

  #[tokio::test]
  async fn a_closed_connection_ends_the_watch() {
    let server = Scripted::new([Err(RequestError::ConnectionClosed)]);
    assert_eq!(verdict(&server).await, Verdict::Closed);
  }
}
