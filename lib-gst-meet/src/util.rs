use std::time::Duration;

use tokio::task::JoinHandle;
use uuid::Uuid;

pub(crate) fn generate_id() -> String {
  Uuid::new_v4().to_string()
}

pub(crate) struct AbortOnDrop(pub(crate) JoinHandle<()>);

impl Drop for AbortOnDrop {
  fn drop(&mut self) {
    self.0.abort();
  }
}

/// Delays that double from `first` up to `max`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Backoff {
  first: Duration,
  next: Duration,
  max: Duration,
}

impl Backoff {
  pub(crate) fn new(first: Duration, max: Duration) -> Self {
    Self { first, next: first, max }
  }

  pub(crate) fn next_delay(&mut self) -> Duration {
    let delay = self.next;
    self.next = (self.next * 2).min(self.max);
    delay
  }

  pub(crate) fn reset(&mut self) {
    self.next = self.first;
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn the_delays_double_up_to_the_maximum() {
    let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(30));
    let delays: Vec<u64> = (0..8).map(|_| backoff.next_delay().as_secs()).collect();
    assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30, 30]);
    backoff.reset();
    assert_eq!(backoff.next_delay(), Duration::from_secs(1));
  }
}
