use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use colibri::{ColibriMessage, JsonMessage};
use futures::{
  sink::SinkExt,
  stream::{SplitSink, StreamExt},
};
use rand::Rng;
use tokio::{
  net::TcpStream,
  select,
  sync::{mpsc, watch},
  task::JoinHandle,
  time::{sleep, timeout, Instant},
};
use tokio_tungstenite::{
  tungstenite::{
    http::{Request, Uri},
    Message,
  },
  MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, info, warn};

use crate::{tls::wss_connector, util::Backoff};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const FIRST_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
/// A connection that held at least this long was a good one: the delays start over after it.
const STABLE_AFTER: Duration = Duration::from_secs(30);
const SEND_QUEUE: usize = 64;

type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) struct ColibriParams {
  pub(crate) url: String,
  pub(crate) tls_insecure: bool,
  /// Ours, to recognise the E2E pings that are for us.
  pub(crate) endpoint_id: String,
  /// Called for every message but the E2E pings; it must not block.
  pub(crate) on_message: Arc<dyn Fn(ColibriMessage) + Send + Sync>,
  /// What is sent first on every connection: the state the bridge has lost. Called once the channel counts as
  /// connected, so that anything set from then on is sent after these messages.
  pub(crate) on_connected: Arc<dyn Fn() -> Vec<ColibriMessage> + Send + Sync>,
  pub(crate) reconnect: Backoff,
}

impl ColibriParams {
  pub(crate) fn default_reconnect() -> Backoff {
    Backoff::new(FIRST_RECONNECT_DELAY, MAX_RECONNECT_DELAY)
  }
}

/// The channel of a session: connects, and connects again whenever the connection is lost, until it is dropped. A
/// lost connection is not the end of the conference: the bridge may restart or a network may flap.
pub(crate) struct ColibriChannel {
  sender: ColibriSender,
  task: JoinHandle<()>,
}

impl ColibriChannel {
  pub(crate) fn spawn(params: ColibriParams) -> Self {
    let (tx, rx) = mpsc::channel(SEND_QUEUE);
    let (connected_tx, connected) = watch::channel(false);
    Self {
      sender: ColibriSender { tx, connected },
      task: tokio::spawn(supervise(params, rx, connected_tx)),
    }
  }

  pub(crate) fn sender(&self) -> ColibriSender {
    self.sender.clone()
  }
}

impl Drop for ColibriChannel {
  fn drop(&mut self) {
    self.task.abort();
  }
}

#[derive(Clone)]
pub(crate) struct ColibriSender {
  tx: mpsc::Sender<ColibriMessage>,
  connected: watch::Receiver<bool>,
}

impl ColibriSender {
  pub(crate) fn is_connected(&self) -> bool {
    *self.connected.borrow()
  }

  /// Fails if the channel is not connected at the moment: nothing is queued for the next connection, as the state
  /// that matters is sent on it anyway.
  pub(crate) async fn send(&self, message: ColibriMessage) -> Result<()> {
    if !self.is_connected() {
      bail!("the colibri channel is not connected");
    }
    self
      .tx
      .send(message)
      .await
      .map_err(|_| anyhow!("the colibri channel is closed"))
  }

  /// A sender that is always connected, and what it sends.
  #[cfg(test)]
  pub(crate) fn detached() -> (Self, mpsc::Receiver<ColibriMessage>) {
    let (tx, rx) = mpsc::channel(SEND_QUEUE);
    (
      Self {
        tx,
        connected: watch::channel(true).1,
      },
      rx,
    )
  }
}

async fn supervise(
  params: ColibriParams,
  mut send_rx: mpsc::Receiver<ColibriMessage>,
  connected_tx: watch::Sender<bool>,
) {
  let mut backoff = params.reconnect;
  loop {
    match timeout(CONNECT_TIMEOUT, connect(&params)).await {
      Ok(Ok(websocket)) => {
        info!("Connected Colibri WebSocket to {}", params.url);
        let connected_at = Instant::now();
        match serve(websocket, &params, &mut send_rx, &connected_tx).await {
          Ok(()) => debug!("colibri websocket closed"),
          Err(e) => warn!("colibri websocket failed: {:#}", e),
        }
        let _ = connected_tx.send(false);
        if connected_at.elapsed() >= STABLE_AFTER {
          backoff.reset();
        }
      },
      Ok(Err(e)) => warn!("failed to connect the Colibri WebSocket to {}: {:#}", params.url, e),
      Err(_) => warn!("timed out connecting the Colibri WebSocket to {}", params.url),
    }
    let delay = backoff.next_delay();
    debug!("reconnecting the colibri websocket in {:?}", delay);
    sleep(delay).await;
  }
}

async fn connect(params: &ColibriParams) -> Result<WebSocket> {
  let uri: Uri = params.url.parse().context("invalid WebSocket URL")?;
  let host = uri.host().context("invalid WebSocket URL: missing host")?;
  let mut key = [0u8; 16];
  rand::rng().fill_bytes(&mut key);
  let request = Request::get(&uri)
    .header(
      "sec-websocket-key",
      base64::engine::general_purpose::STANDARD.encode(key),
    )
    .header("sec-websocket-version", "13")
    .header("host", host)
    // TODO: the server should probably not enforce this since non-browser clients are now possible
    .header("origin", format!("https://{}", host))
    .header("connection", "Upgrade")
    .header("upgrade", "websocket")
    .body(())?;
  let connector = (uri.scheme_str() == Some("wss"))
    .then(|| wss_connector(params.tls_insecure))
    .transpose()
    .context("failed to build TLS connector")?;
  let (websocket, _) = tokio_tungstenite::connect_async_tls_with_config(request, None, true, connector).await?;
  Ok(websocket)
}

/// Runs one connection. Returns when it ends, or when the queue of messages to send has no senders left.
async fn serve(
  websocket: WebSocket,
  params: &ColibriParams,
  send_rx: &mut mpsc::Receiver<ColibriMessage>,
  connected_tx: &watch::Sender<bool>,
) -> Result<()> {
  let (mut sink, mut stream) = websocket.split();
  // Whatever was queued belongs to a connection that is gone.
  while send_rx.try_recv().is_ok() {}
  // Connected first, greeting second: what changes in between is queued behind the greeting, and what changed
  // before is in it. The other way round, a change that falls between working out the greeting and marking the
  // channel as connected is in neither (the one who made it saw a channel that was down, and left it to the
  // greeting).
  let _ = connected_tx.send(true);
  for message in (params.on_connected)() {
    send(&mut sink, &message).await?;
  }

  loop {
    select! {
      frame = stream.next() => match frame {
        Some(Ok(Message::Text(text))) => {
          debug!("Colibri <<< {}", text);
          if let Some(reply) = handle_text(params, &text) {
            send(&mut sink, &reply).await?;
          }
        },
        Some(Ok(Message::Binary(data))) => {
          debug!("received unexpected {} byte binary frame on colibri websocket", data.len());
        },
        Some(Ok(Message::Close(frame))) => {
          debug!(
            "received close frame on colibri websocket: {}",
            frame.map_or_else(|| "no reason".into(), |frame| frame.reason.to_string())
          );
          return Ok(());
        },
        Some(Ok(Message::Frame(_) | Message::Ping(_) | Message::Pong(_))) => {},
        Some(Err(e)) => return Err(e.into()),
        None => return Ok(()),
      },
      message = send_rx.recv() => match message {
        Some(message) => send(&mut sink, &message).await?,
        None => return Ok(()),
      },
    }
  }
}

async fn send(sink: &mut SplitSink<WebSocket, Message>, message: &ColibriMessage) -> Result<()> {
  let json = serde_json::to_string(message).context("failed to serialise a colibri message")?;
  debug!("Colibri >>> {}", json);
  sink.send(Message::Text(json.into())).await?;
  Ok(())
}

/// Hands a message on to the application, unless it is for the library. Returns what is to be sent back.
fn handle_text(params: &ColibriParams, text: &str) -> Option<ColibriMessage> {
  let message = match serde_json::from_str::<ColibriMessage>(text) {
    Ok(message) => message,
    Err(e) => {
      warn!("failed to parse frame on colibri websocket: {:?}\nframe: {}", e, text);
      return None;
    },
  };
  match &message {
    ColibriMessage::EndpointMessage {
      to: Some(to),
      from,
      msg_payload,
    } if *to == params.endpoint_id => {
      if let Ok(JsonMessage::E2ePingRequest { id }) = serde_json::from_value(msg_payload.clone()) {
        return serde_json::to_value(JsonMessage::E2ePingResponse { id })
          .ok()
          .map(|msg_payload| ColibriMessage::EndpointMessage {
            from: None,
            to: from.clone(),
            msg_payload,
          });
      }
    },
    ColibriMessage::VideoSourcesMap { mapped_sources } => {
      warn!(
        "received VideoSourcesMap with {} entries despite not negotiating ssrc-rewriting; ignoring",
        mapped_sources.len()
      );
      return None;
    },
    ColibriMessage::AudioSourcesMap { mapped_sources } => {
      warn!(
        "received AudioSourcesMap with {} entries despite not negotiating ssrc-rewriting; ignoring",
        mapped_sources.len()
      );
      return None;
    },
    _ => {},
  }
  (params.on_message)(message);
  None
}

#[cfg(test)]
mod tests {
  use std::sync::{atomic::Ordering, Mutex};

  use colibri::ReceiverVideoConstraints;
  use serde_json::json;
  use tokio::net::TcpListener;

  use super::*;

  async fn accept(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    let (tcp, _) = listener.accept().await.unwrap();
    tokio_tungstenite::accept_async(tcp).await.unwrap()
  }

  async fn next_json(websocket: &mut WebSocketStream<TcpStream>) -> serde_json::Value {
    loop {
      match timeout(Duration::from_secs(10), websocket.next()).await.unwrap().unwrap().unwrap() {
        Message::Text(text) => return serde_json::from_str(&text).unwrap(),
        _ => continue,
      }
    }
  }

  fn state() -> Vec<ColibriMessage> {
    vec![
      ColibriMessage::ReceiverVideoConstraints(ReceiverVideoConstraints {
        last_n: Some(2),
        ..Default::default()
      }),
      ColibriMessage::SourceVideoTypeMessage {
        source_name: "me-v0".to_owned(),
        video_type: colibri::VideoType::Camera,
      },
    ]
  }

  #[tokio::test]
  async fn reconnects_and_sends_the_state_again() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let received = Arc::new(Mutex::new(vec![]));
    let channel = ColibriChannel::spawn(ColibriParams {
      url: format!("ws://{}/colibri-ws/jvb/abc", listener.local_addr().unwrap()),
      tls_insecure: false,
      endpoint_id: "me".to_owned(),
      on_message: {
        let received = received.clone();
        Arc::new(move |message| received.lock().unwrap().push(message))
      },
      on_connected: Arc::new(state),
      reconnect: Backoff::new(Duration::from_millis(10), Duration::from_millis(50)),
    });
    let sender = channel.sender();
    assert!(!sender.is_connected());
    assert!(sender.send(ColibriMessage::Unknown).await.is_err());

    for round in 0..2 {
      let mut server = accept(&listener).await;
      assert_eq!(
        next_json(&mut server).await,
        json!({ "colibriClass": "ReceiverVideoConstraints", "lastN": 2 }),
        "round {round}"
      );
      assert_eq!(next_json(&mut server).await["colibriClass"], "SourceVideoTypeMessage");

      let wait_connected = async {
        while !sender.is_connected() {
          sleep(Duration::from_millis(5)).await;
        }
      };
      timeout(Duration::from_secs(5), wait_connected).await.unwrap();

      sender
        .send(ColibriMessage::LastNChangedEvent { last_n: round })
        .await
        .unwrap();
      assert_eq!(next_json(&mut server).await["lastN"], round);

      // Incoming: the bridge's own message goes to the application, an E2E ping is answered.
      server
        .send(Message::Text(
          json!({ "colibriClass": "ForwardedSources", "forwardedSources": ["a-v0"] }).to_string().into(),
        ))
        .await
        .unwrap();
      server
        .send(Message::Text(
          json!({
            "colibriClass": "EndpointMessage",
            "from": "other",
            "to": "me",
            "msgPayload": { "type": "e2e-ping-request", "id": 7 },
          })
          .to_string()
          .into(),
        ))
        .await
        .unwrap();
      let reply = next_json(&mut server).await;
      assert_eq!(reply["to"], "other");
      assert_eq!(reply["msgPayload"], json!({ "type": "e2e-ping-response", "id": 7 }));

      // The bridge goes away; the channel does not.
      server.close(None).await.unwrap();
      while let Some(Ok(_)) = server.next().await {}
      drop(server);
      timeout(Duration::from_secs(5), async {
        while sender.is_connected() {
          sleep(Duration::from_millis(5)).await;
        }
      })
      .await
      .unwrap();
    }

    let received = received.lock().unwrap();
    assert_eq!(received.len(), 2);
    assert!(received
      .iter()
      .all(|message| matches!(message, ColibriMessage::ForwardedSources { .. })));
  }

  #[tokio::test]
  async fn keeps_trying_until_the_bridge_is_there() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/colibri-ws", listener.local_addr().unwrap());
    drop(listener);
    let channel = ColibriChannel::spawn(ColibriParams {
      url: url.clone(),
      tls_insecure: false,
      endpoint_id: "me".to_owned(),
      on_message: Arc::new(|_| {}),
      on_connected: Arc::new(Vec::new),
      reconnect: Backoff::new(Duration::from_millis(10), Duration::from_millis(20)),
    });
    sleep(Duration::from_millis(100)).await;
    assert!(!channel.sender().is_connected());

    let listener = TcpListener::bind(url.trim_start_matches("ws://").split('/').next().unwrap())
      .await
      .unwrap();
    let _server = accept(&listener).await;
    let sender = channel.sender();
    timeout(Duration::from_secs(5), async {
      while !sender.is_connected() {
        sleep(Duration::from_millis(5)).await;
      }
    })
    .await
    .unwrap();
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn a_change_made_while_the_greeting_is_worked_out_is_not_lost() {
    use std::sync::{atomic::AtomicI32, OnceLock};

    use tokio::sync::Notify;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = Arc::new(AtomicI32::new(1));
    let probe: Arc<OnceLock<ColibriSender>> = Arc::default();
    let working = Arc::new(Notify::new());
    let channel = ColibriChannel::spawn(ColibriParams {
      url: format!("ws://{}/colibri-ws/jvb/abc", listener.local_addr().unwrap()),
      tls_insecure: false,
      endpoint_id: "me".to_owned(),
      on_message: Arc::new(|_| {}),
      on_connected: {
        let (state, working) = (state.clone(), working.clone());
        Arc::new(move || {
          let last_n = state.load(Ordering::SeqCst);
          // The application changes its mind while this is being worked out.
          working.notify_one();
          std::thread::sleep(Duration::from_millis(300));
          vec![ColibriMessage::LastNChangedEvent { last_n }]
        })
      },
      reconnect: Backoff::new(Duration::from_millis(10), Duration::from_millis(50)),
    });
    probe.set(channel.sender()).ok().unwrap();

    // What a setter does: change the state, then send it unless the channel is down, in which case the channel
    // sends the state when it comes up.
    let setter = tokio::spawn({
      let (state, probe, working) = (state.clone(), probe.clone(), working.clone());
      async move {
        working.notified().await;
        state.store(9, Ordering::SeqCst);
        let sender = probe.get().unwrap();
        if sender.is_connected() {
          sender.send(ColibriMessage::LastNChangedEvent { last_n: 9 }).await.unwrap();
        }
      }
    });

    let mut server = accept(&listener).await;
    let mut last = 0;
    timeout(Duration::from_secs(5), async {
      while last != 9 {
        last = next_json(&mut server).await["lastN"].as_i64().unwrap();
      }
    })
    .await
    .expect("the bridge is told what the application wants in the end");
    setter.await.unwrap();
  }

  #[tokio::test]
  async fn the_channel_counts_as_connected_when_the_greeting_is_worked_out() {
    use std::sync::OnceLock;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let probe: Arc<OnceLock<ColibriSender>> = Arc::default();
    let seen = Arc::new(Mutex::new(vec![]));
    let channel = ColibriChannel::spawn(ColibriParams {
      url: format!("ws://{}/colibri-ws/jvb/abc", listener.local_addr().unwrap()),
      tls_insecure: false,
      endpoint_id: "me".to_owned(),
      on_message: Arc::new(|_| {}),
      on_connected: {
        let (probe, seen) = (probe.clone(), seen.clone());
        Arc::new(move || {
          seen.lock().unwrap().push(probe.get().unwrap().is_connected());
          vec![]
        })
      },
      reconnect: Backoff::new(Duration::from_millis(10), Duration::from_millis(50)),
    });
    probe.set(channel.sender()).ok().unwrap();

    for round in 1..=2 {
      let mut server = accept(&listener).await;
      timeout(Duration::from_secs(5), async {
        while seen.lock().unwrap().len() < round {
          sleep(Duration::from_millis(5)).await;
        }
      })
      .await
      .unwrap();
      server.close(None).await.unwrap();
      while let Some(Ok(_)) = server.next().await {}
    }
    assert_eq!(*seen.lock().unwrap(), [true, true]);
  }
}
