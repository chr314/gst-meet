//! Delivers every callback of the application, in order, from one task. Nothing in the library waits for a callback
//! or calls one with a library lock held: events are queued, and the queue is all they share with the application.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::{
  sync::{mpsc, oneshot},
  task_local,
};
use tracing::{error, warn};

use crate::conference::BoxedResultFuture;

pub(crate) type Callback<T> = Arc<dyn Fn(T) -> BoxedResultFuture + Send + Sync>;

task_local! {
  static IN_CALLBACK: ();
}

enum Job {
  Run(Pin<Box<dyn Future<Output = ()> + Send>>),
  Stop(oneshot::Sender<()>),
}

#[derive(Debug)]
pub(crate) struct EventDispatcher {
  tx: mpsc::UnboundedSender<Job>,
}

impl EventDispatcher {
  /// Must be called from within a tokio runtime.
  pub(crate) fn spawn() -> Self {
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
      while let Some(job) = rx.recv().await {
        match job {
          // A task per callback keeps a panic in one from taking the others with it.
          Job::Run(callback) => {
            if let Err(e) = tokio::spawn(IN_CALLBACK.scope((), callback)).await {
              error!("a callback panicked: {:?}", e);
            }
          },
          Job::Stop(done) => {
            let _ = done.send(());
            break;
          },
        }
      }
    });
    Self { tx }
  }

  /// `name` is for the log.
  pub(crate) fn dispatch<T: Send + 'static>(&self, name: &'static str, callback: Callback<T>, argument: T) {
    self.dispatch_then(name, callback, argument, || {});
  }

  /// Queues `callback(argument)` and `then` to run once the callback is done, whether it succeeded or not.
  pub(crate) fn dispatch_then<T: Send + 'static>(
    &self,
    name: &'static str,
    callback: Callback<T>,
    argument: T,
    then: impl FnOnce() + Send + 'static,
  ) {
    let job = Job::Run(Box::pin(async move {
      if let Err(e) = callback(argument).await {
        warn!("{} callback failed: {:?}", name, e);
      }
      then();
    }));
    // Fails only after shutdown, when nobody is interested any more.
    let _ = self.tx.send(job);
  }

  /// Delivers what is queued, then stops the dispatcher; later events are dropped. Waits for that at most
  /// `timeout`, and not at all when called by a callback, which would be waiting for itself.
  pub(crate) async fn shutdown(&self, timeout: Duration) {
    let (done_tx, done_rx) = oneshot::channel();
    if self.tx.send(Job::Stop(done_tx)).is_err() || IN_CALLBACK.try_with(|_| ()).is_ok() {
      return;
    }
    if tokio::time::timeout(timeout, done_rx).await.is_err() {
      warn!("the callbacks did not finish within {:?} of the shutdown", timeout);
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Mutex;

  use anyhow::bail;

  use super::*;

  fn recording(log: &Arc<Mutex<Vec<String>>>, delay: Duration) -> Callback<String> {
    let log = log.clone();
    Arc::new(move |event: String| {
      let log = log.clone();
      Box::pin(async move {
        tokio::time::sleep(delay).await;
        log.lock().unwrap().push(event);
        Ok(())
      })
    })
  }

  #[tokio::test]
  async fn delivers_in_order_even_when_a_callback_is_slow() {
    let dispatcher = EventDispatcher::spawn();
    let log = Arc::new(Mutex::new(vec![]));
    let slow = recording(&log, Duration::from_millis(50));
    let fast = recording(&log, Duration::ZERO);
    dispatcher.dispatch("slow", slow, "first".to_owned());
    dispatcher.dispatch("fast", fast.clone(), "second".to_owned());
    dispatcher.dispatch("fast", fast, "third".to_owned());
    dispatcher.shutdown(Duration::from_secs(5)).await;
    assert_eq!(*log.lock().unwrap(), ["first", "second", "third"]);
  }

  #[tokio::test]
  async fn queueing_does_not_wait_for_the_callbacks() {
    let dispatcher = EventDispatcher::spawn();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let blocked: Callback<()> = Arc::new(move |()| {
      let release_rx = release_rx.lock().unwrap().take();
      Box::pin(async move {
        if let Some(release_rx) = release_rx {
          let _ = release_rx.await;
        }
        Ok(())
      })
    });
    dispatcher.dispatch("blocked", blocked.clone(), ());
    // Returns at once although the first callback is still running.
    let (queued_tx, queued_rx) = oneshot::channel();
    dispatcher.dispatch_then("after", blocked, (), move || {
      let _ = queued_tx.send(());
    });
    assert!(tokio::time::timeout(Duration::from_millis(50), queued_rx).await.is_err());
    release_tx.send(()).unwrap();
    dispatcher.shutdown(Duration::from_secs(5)).await;
  }

  #[tokio::test]
  async fn a_callback_may_take_the_lock_its_producer_holds_while_queueing() {
    let dispatcher = Arc::new(EventDispatcher::spawn());
    let state = Arc::new(Mutex::new(0u32));
    let seen = Arc::new(Mutex::new(vec![]));
    let callback: Callback<u32> = {
      let (state, seen) = (state.clone(), seen.clone());
      Arc::new(move |event| {
        let (state, seen) = (state.clone(), seen.clone());
        Box::pin(async move {
          seen.lock().unwrap().push((event, *state.lock().unwrap()));
          Ok(())
        })
      })
    };
    {
      let mut locked = state.lock().unwrap();
      *locked = 7;
      dispatcher.dispatch("event", callback, 1);
      // The producer is not waiting for the callback: it releases the lock, then the callback proceeds.
    }
    dispatcher.shutdown(Duration::from_secs(5)).await;
    assert_eq!(*seen.lock().unwrap(), [(1, 7)]);
  }

  #[tokio::test]
  async fn a_failing_or_panicking_callback_does_not_stop_the_others() {
    let dispatcher = EventDispatcher::spawn();
    let log = Arc::new(Mutex::new(vec![]));
    let failing: Callback<String> = Arc::new(|_| Box::pin(async { bail!("on purpose") }));
    let panicking: Callback<String> = Arc::new(|_| Box::pin(async { panic!("on purpose") }));
    dispatcher.dispatch("failing", failing, String::new());
    dispatcher.dispatch("panicking", panicking, String::new());
    dispatcher.dispatch("fine", recording(&log, Duration::ZERO), "still here".to_owned());
    dispatcher.shutdown(Duration::from_secs(5)).await;
    assert_eq!(*log.lock().unwrap(), ["still here"]);
  }

  #[tokio::test]
  async fn a_callback_can_shut_the_dispatcher_down_without_waiting_for_itself() {
    let dispatcher = Arc::new(EventDispatcher::spawn());
    let (done_tx, done_rx) = oneshot::channel();
    let done_tx = Arc::new(Mutex::new(Some(done_tx)));
    let callback: Callback<()> = {
      let dispatcher = dispatcher.clone();
      Arc::new(move |()| {
        let (dispatcher, done_tx) = (dispatcher.clone(), done_tx.clone());
        Box::pin(async move {
          dispatcher.shutdown(Duration::from_secs(30)).await;
          let _ = done_tx.lock().unwrap().take().unwrap().send(());
          Ok(())
        })
      })
    };
    dispatcher.dispatch("leaving", callback, ());
    tokio::time::timeout(Duration::from_secs(5), done_rx).await.unwrap().unwrap();
  }

  #[tokio::test]
  async fn events_after_the_shutdown_are_dropped() {
    let dispatcher = EventDispatcher::spawn();
    let log = Arc::new(Mutex::new(vec![]));
    dispatcher.shutdown(Duration::from_secs(5)).await;
    dispatcher.dispatch("late", recording(&log, Duration::ZERO), "late".to_owned());
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(log.lock().unwrap().is_empty());
  }
}
