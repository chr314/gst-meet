use std::thread::{self, JoinHandle};

use anyhow::{Context, Result};

/// The context passed in by the application, which iterates it itself, or a context of ours with a thread that
/// does.
pub(super) struct MainContext {
  context: glib::MainContext,
  owned: Option<(glib::MainLoop, Option<JoinHandle<()>>)>,
}

impl MainContext {
  pub(super) fn new(context: Option<glib::MainContext>) -> Result<Self> {
    match context {
      Some(context) => Ok(Self { context, owned: None }),
      None => Self::spawn(|| {}),
    }
  }

  /// `before_run` runs on the new thread before it starts iterating the context.
  fn spawn(before_run: impl FnOnce() + Send + 'static) -> Result<Self> {
    let context = glib::MainContext::new();
    let main_loop = glib::MainLoop::new(Some(&context), false);
    let thread = thread::Builder::new()
      .name("gst-meet-glib".to_owned())
      .spawn({
        let context = context.clone();
        let main_loop = main_loop.clone();
        move || {
          before_run();
          if context.with_thread_default(|| main_loop.run()).is_err() {
            tracing::error!("the GLib main context is owned by another thread");
          }
        }
      })
      .context("failed to start the GLib main loop thread")?;
    Ok(Self {
      context,
      owned: Some((main_loop, Some(thread))),
    })
  }

  pub(super) fn context(&self) -> &glib::MainContext {
    &self.context
  }

  /// Stops the thread we started, if any, and waits for it.
  pub(super) fn stop(&mut self) {
    let Some((main_loop, thread)) = &mut self.owned else {
      return;
    };
    // `g_main_loop_quit` is lost when the thread has not started iterating yet, because `g_main_loop_run` marks the
    // loop as running unconditionally. Quitting from a source of the context cannot be lost: the source is
    // dispatched by that very iteration.
    let main_loop = main_loop.clone();
    self.context.invoke(move || main_loop.quit());
    if let Some(thread) = thread.take() {
      // Stopped from a callback on the loop's own thread: the loop ends when that returns.
      if thread.thread().id() != thread::current().id() {
        let _ = thread.join();
      }
    }
  }
}

impl Drop for MainContext {
  fn drop(&mut self) {
    self.stop();
  }
}

#[cfg(test)]
mod tests {
  use std::{sync::mpsc, time::Duration};

  use super::*;

  /// Runs `f` on a thread of its own and fails (rather than hangs) when it does not return.
  fn within(timeout: Duration, f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
      f();
      let _ = tx.send(());
    });
    rx.recv_timeout(timeout).expect("the call did not return");
  }

  #[test]
  fn stopping_before_the_thread_iterates_the_context_does_not_hang() {
    within(Duration::from_secs(10), || {
      let mut main = MainContext::spawn(|| thread::sleep(Duration::from_millis(300))).unwrap();
      main.stop();
    });
  }

  #[test]
  fn dropping_before_the_thread_iterates_the_context_does_not_hang() {
    within(Duration::from_secs(10), || {
      let main = MainContext::spawn(|| thread::sleep(Duration::from_millis(300))).unwrap();
      drop(main);
    });
  }

  #[test]
  fn stopping_twice_and_stopping_from_the_loop_itself_are_harmless() {
    within(Duration::from_secs(10), || {
      let mut main = MainContext::new(None).unwrap();
      main.stop();
      main.stop();

      // The thread stops itself: waiting for it there would wait for the caller.
      let main = std::sync::Arc::new(std::sync::Mutex::new(MainContext::new(None).unwrap()));
      let context = main.lock().unwrap().context().clone();
      let (tx, rx) = mpsc::channel();
      context.invoke({
        let main = main.clone();
        move || {
          main.lock().unwrap().stop();
          let _ = tx.send(());
        }
      });
      rx.recv_timeout(Duration::from_secs(5))
        .expect("stop() returned on the loop thread");
      main.lock().unwrap().stop();
    });
  }
}
