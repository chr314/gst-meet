use std::{collections::HashMap, convert::TryFrom, fmt, future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::Engine as _;
use futures::{
  sink::{Sink, SinkExt},
  stream::{Stream, StreamExt, TryStreamExt},
};
use jid::{BareJid, FullJid, Jid};
use minidom::Element;
use rand::Rng;
use tokio::{
  sync::{mpsc, oneshot, Mutex, Notify},
  time::{timeout, Instant},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::{
  http::{Request, Uri},
  Message,
};
use tracing::{debug, error, info, warn};
use xmpp_parsers::{
  bind::{BindQuery, BindResponse},
  disco::{DiscoInfoQuery, DiscoInfoResult},
  iq::{Iq, IqPayload},
  ns,
  sasl::{Auth, Mechanism, Success},
  stanza_error::{DefinedCondition, ErrorType},
  websocket::Open,
};

use crate::{
  error::ConnectionError,
  muc::error_condition,
  pinger::{self, Keepalive, Pinger, Verdict},
  stanza_filter::{StanzaFilter, StanzaFilterId},
  tls::wss_connector,
  util::{generate_id, AbortOnDrop},
  xmpp::{
    self,
    iq::{error_iq, request_header, stanza_error, IqResponse, RequestError},
  },
};

const EXTERNAL_SERVICES_TIMEOUT: Duration = Duration::from_secs(5);

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

const SASL: &str = "urn:ietf:params:xml:ns:xmpp-sasl";
const STREAMS: &str = "http://etherx.jabber.org/streams";
const STREAM_ERRORS: &str = "urn:ietf:params:xml:ns:xmpp-streams";

#[derive(Debug, Clone, Copy)]
enum ConnectionState {
  OpeningPreAuthentication,
  ReceivingFeaturesPreAuthentication,
  Authenticating,
  OpeningPostAuthentication,
  ReceivingFeaturesPostAuthentication,
  Binding,
  Discovering,
  DiscoveringExternalServices,
  Idle,
}

type SharedFilter = Arc<dyn StanzaFilter + Send + Sync>;

struct PendingRequest {
  /// Whom the request went to. An answer from anybody else is not ours.
  to: Option<Jid>,
  tx: oneshot::Sender<IqResponse>,
}

struct ConnectionInner {
  state: ConnectionState,
  jid: Option<FullJid>,
  xmpp_domain: BareJid,
  authentication: Authentication,
  external_services: Vec<xmpp::extdisco::Service>,
  connected_tx: Option<oneshot::Sender<Result<(), ConnectionError>>>,
  stanza_filters: Vec<(StanzaFilterId, SharedFilter)>,
  next_filter_id: u64,
  pending_requests: HashMap<String, PendingRequest>,
  last_received: Instant,
  closed: Option<ConnectionError>,
  /// Pings the server; stops with the connection.
  keepalive: Option<AbortOnDrop>,
}

impl fmt::Debug for ConnectionInner {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ConnectionInner")
      .field("state", &self.state)
      .field("jid", &self.jid)
      .finish()
  }
}

pub(crate) struct PendingResponse {
  id: String,
  rx: oneshot::Receiver<IqResponse>,
  inner: Arc<Mutex<ConnectionInner>>,
}

impl PendingResponse {
  pub(crate) async fn wait(self, timeout_after: Duration) -> Result<IqResponse, RequestError> {
    match timeout(timeout_after, self.rx).await {
      Ok(Ok(response)) => Ok(response),
      // The sender is dropped when the read loop ends.
      Ok(Err(_)) => Err(RequestError::ConnectionClosed),
      Err(_) => {
        self.inner.lock().await.pending_requests.remove(&self.id);
        Err(RequestError::Timeout)
      },
    }
  }
}

#[derive(Debug, Clone)]
pub struct Connection {
  pub(crate) tx: mpsc::Sender<Element>,
  inner: Arc<Mutex<ConnectionInner>>,
  pub(crate) tls_insecure: bool,
  /// Ends the socket without the closing handshake, for a connection that is dead.
  abort: Arc<Notify>,
  keepalive: Keepalive,
}

#[derive(Debug, Clone)]
pub enum Authentication {
  Anonymous,
  Plain { username: String, password: String },
  Jwt { token: String },
}

impl Connection {
  pub async fn new(
    websocket_url: &str,
    xmpp_domain: &str,
    authentication: Authentication,
    room_name: &str,
    tls_insecure: bool,
  ) -> Result<(Self, impl Future<Output = ()>)> {
    let websocket_url: Uri = match &authentication {
      Authentication::Plain { .. } => websocket_url.parse().context("invalid WebSocket URL")?,
      Authentication::Jwt { token } => format!("{}?room={}&token={}", websocket_url, room_name, token)
        .parse()
        .context("invalid WebSocket URL")?,
      Authentication::Anonymous => websocket_url.parse().context("invalid WebSocket URL")?,
    };
    let xmpp_domain: BareJid = xmpp_domain.parse().context("invalid XMPP domain")?;

    info!("Connecting XMPP WebSocket to {}", websocket_url);
    let mut key = [0u8; 16];
    rand::rng().fill_bytes(&mut key);
    let request = Request::get(&websocket_url)
      .header("sec-websocket-protocol", "xmpp")
      .header(
        "sec-websocket-key",
        base64::engine::general_purpose::STANDARD.encode(key),
      )
      .header("sec-websocket-version", "13")
      .header(
        "host",
        websocket_url.host().context("invalid WebSocket URL: missing host")?,
      )
      .header("connection", "Upgrade")
      .header("upgrade", "websocket")
      .body(())
      .context("failed to build WebSocket request")?;
    let (websocket, _response) = tokio_tungstenite::connect_async_tls_with_config(
      request,
      None,
      true,
      Some(wss_connector(tls_insecure).context("failed to build TLS connector")?),
    )
    .await
    .context("failed to connect XMPP WebSocket")?;
    let (sink, stream) = websocket.split();
    Ok(Self::over(
      sink,
      stream,
      xmpp_domain,
      authentication,
      tls_insecure,
      Keepalive::default(),
    ))
  }

  /// The future it returns drives the connection and is to be spawned.
  fn over<Si, St>(
    sink: Si,
    stream: St,
    xmpp_domain: BareJid,
    authentication: Authentication,
    tls_insecure: bool,
    keepalive: Keepalive,
  ) -> (Self, impl Future<Output = ()>)
  where
    Si: Sink<Message> + Unpin,
    Si::Error: std::error::Error + Send + Sync + 'static,
    St: Stream<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
  {
    let (tx, rx) = mpsc::channel(64);

    let inner = Arc::new(Mutex::new(ConnectionInner::new(
      ConnectionState::OpeningPreAuthentication,
      None,
      xmpp_domain,
      authentication,
    )));
    let abort = Arc::new(Notify::new());

    let connection = Self {
      tx: tx.clone(),
      inner: inner.clone(),
      tls_insecure,
      abort: abort.clone(),
      keepalive,
    };

    let writer = Connection::write_loop(rx, sink);
    let reader = Connection::read_loop(inner.clone(), tx, stream);

    let background = async move {
      let reason = tokio::select! {
        res = reader => match res {
          Ok(()) => ConnectionError::Closed("the stream ended".to_owned()),
          Err(e) => {
            error!("fatal (in read loop): {}", e);
            e
          },
        },
        res = writer => match res {
          Ok(()) => ConnectionError::Closed("the stream was closed".to_owned()),
          Err(e) => {
            error!("fatal (in write loop): {:?}", e);
            ConnectionError::Closed(format!("{e:#}"))
          },
        },
        _ = abort.notified() => ConnectionError::Closed("the connection was dropped".to_owned()),
      };
      Connection::close(&inner, reason).await;
    };

    (connection, background)
  }

  /// If the connection has ended already, the filter hears about that straight away.
  pub async fn add_stanza_filter(&self, stanza_filter: impl StanzaFilter + Send + Sync + 'static) -> StanzaFilterId {
    let filter: SharedFilter = Arc::new(stanza_filter);
    let mut locked_inner = self.inner.lock().await;
    let id = StanzaFilterId(locked_inner.next_filter_id);
    locked_inner.next_filter_id += 1;
    if locked_inner.closed.is_some() {
      drop(locked_inner);
      filter.connection_closed();
    }
    else {
      locked_inner.stanza_filters.push((id, filter));
    }
    id
  }

  /// Returns whether there was such a filter.
  pub async fn remove_stanza_filter(&self, id: StanzaFilterId) -> bool {
    let mut locked_inner = self.inner.lock().await;
    let count = locked_inner.stanza_filters.len();
    locked_inner.stanza_filters.retain(|(filter_id, _)| *filter_id != id);
    locked_inner.stanza_filters.len() != count
  }

  /// Ends the XMPP stream and closes the WebSocket; the background future then completes. Conferences on this
  /// connection see the connection end.
  pub async fn disconnect(&self) {
    let _ = self.tx.send(Element::builder("close", ns::WEBSOCKET).build()).await;
  }

  pub async fn send(&self, element: Element) -> Result<(), RequestError> {
    if self.inner.lock().await.closed.is_some() {
      return Err(RequestError::ConnectionClosed);
    }
    self.tx.send(element).await.map_err(|_| RequestError::ConnectionClosed)
  }

  /// Sets the connection up: authenticates and binds a resource. It fails with a [`ConnectionError`] as soon as the
  /// server refuses a step or the connection ends, and when the server takes longer than 30 seconds.
  pub async fn connect(&self) -> Result<()> {
    self.connect_within(CONNECT_TIMEOUT).await
  }

  pub(crate) async fn connect_within(&self, timeout_after: Duration) -> Result<()> {
    let (tx, rx) = oneshot::channel();

    {
      let mut locked_inner = self.inner.lock().await;
      if let Some(reason) = &locked_inner.closed {
        return Err(reason.clone().into());
      }
      if matches!(locked_inner.state, ConnectionState::Idle) {
        return Ok(());
      }
      locked_inner.connected_tx = Some(tx);
    }
    let open = Open::new(self.inner.lock().await.xmpp_domain.clone());
    if self.tx.send(open.into()).await.is_err() {
      return Err(self.closed_reason().await.into());
    }

    match timeout(timeout_after, rx).await {
      Ok(Ok(Ok(()))) => {
        self.start_keepalive().await;
        Ok(())
      },
      Ok(Ok(Err(e))) => Err(e.into()),
      // The setup was dropped without a result: the connection is gone.
      Ok(Err(_)) => Err(self.closed_reason().await.into()),
      Err(_) => {
        self.abort(ConnectionError::Timeout).await;
        Err(ConnectionError::Timeout.into())
      },
    }
  }

  async fn closed_reason(&self) -> ConnectionError {
    self
      .inner
      .lock()
      .await
      .closed
      .clone()
      .unwrap_or_else(|| ConnectionError::Closed("the connection is closed".to_owned()))
  }

  /// Ends the connection for `reason` at once, also if the socket does not respond any more: what waits for the
  /// connection or is told of its end hears of the end, and the background future completes.
  pub(crate) async fn abort(&self, reason: ConnectionError) {
    Self::close(&self.inner, reason).await;
    self.abort.notify_one();
  }

  /// Pings the server from now on, and ends the connection when it does not answer.
  async fn start_keepalive(&self) {
    let connection = self.clone();
    let task = tokio::spawn(async move {
      let verdict = tokio::select! {
        verdict = pinger::keepalive(&connection, connection.keepalive) => verdict,
        // The background future is gone.
        _ = connection.tx.closed() => return,
      };
      if verdict == Verdict::Dead {
        warn!("the XMPP server does not answer pings, closing the connection");
        connection
          .abort(ConnectionError::Closed("the server did not answer the pings".to_owned()))
          .await;
      }
    });
    let mut locked_inner = self.inner.lock().await;
    if locked_inner.closed.is_none() {
      locked_inner.keepalive = Some(AbortOnDrop(task));
    }
    else {
      task.abort();
    }
  }

  pub async fn jid(&self) -> Option<FullJid> {
    let locked_inner = self.inner.lock().await;
    locked_inner.jid.clone()
  }

  /// The external services (STUN and TURN servers) the server announced when the connection was set up.
  pub async fn external_services(&self) -> Vec<xmpp::extdisco::Service> {
    let locked_inner = self.inner.lock().await;
    locked_inner.external_services.clone()
  }

  /// Asks the server for its external services again, as the credentials of TURN servers expire. Falls back to the
  /// ones it announced earlier if it does not answer.
  pub(crate) async fn external_services_fresh(&self) -> Vec<xmpp::extdisco::Service> {
    match self.fetch_external_services().await {
      Ok(services) => {
        self.inner.lock().await.external_services = services.clone();
        services
      },
      Err(e) => {
        warn!("failed to refresh the external services, using the earlier ones: {:#}", e);
        self.external_services().await
      },
    }
  }

  async fn fetch_external_services(&self) -> Result<Vec<xmpp::extdisco::Service>> {
    let (jid, xmpp_domain) = {
      let locked_inner = self.inner.lock().await;
      (
        locked_inner.jid.clone().context("not connected (no JID)")?,
        locked_inner.xmpp_domain.clone(),
      )
    };
    let iq = Iq::from_get(generate_id(), xmpp::extdisco::ServicesQuery {})
      .with_from(jid.into())
      .with_to(xmpp_domain.into());
    match self.request(iq, EXTERNAL_SERVICES_TIMEOUT).await? {
      IqResponse::Result(Some(services)) => Ok(xmpp::extdisco::ServicesResult::try_from(services)?.services),
      IqResponse::Result(None) => anyhow::bail!("the answer has no services"),
      IqResponse::Error(error) => anyhow::bail!("the server refused: {:?}", error),
    }
  }

  pub async fn request(&self, iq: Iq, timeout_after: Duration) -> Result<IqResponse, RequestError> {
    self.send_request(iq).await?.wait(timeout_after).await
  }

  /// Sends `iq` and returns what to wait for the answer with. Splitting the two guarantees that the IQ is on its
  /// way before anything the caller sends next.
  pub(crate) async fn send_request(&self, iq: Iq) -> Result<PendingResponse, RequestError> {
    let id = iq.id().to_owned();
    let (tx, rx) = oneshot::channel();
    {
      let mut locked_inner = self.inner.lock().await;
      if locked_inner.closed.is_some() {
        return Err(RequestError::ConnectionClosed);
      }
      locked_inner
        .pending_requests
        .insert(id.clone(), PendingRequest { to: iq.to().cloned(), tx });
    }
    if self.tx.send(iq.into()).await.is_err() {
      self.inner.lock().await.pending_requests.remove(&id);
      return Err(RequestError::ConnectionClosed);
    }
    Ok(PendingResponse {
      id,
      rx,
      inner: self.inner.clone(),
    })
  }

  async fn write_loop<S>(rx: mpsc::Receiver<Element>, mut sink: S) -> Result<()>
  where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
  {
    let mut rx = ReceiverStream::new(rx);
    while let Some(element) = rx.next().await {
      let mut bytes = Vec::new();
      element.write_to(&mut bytes)?;
      let xml = String::from_utf8(bytes)?;
      #[cfg(feature = "syntax-highlighting")]
      {
        let ps = syntect::parsing::SyntaxSet::load_defaults_newlines();
        let ts = syntect::highlighting::ThemeSet::load_defaults();
        let mut highlighted = false;
        if let Some(syntax) = ps.find_syntax_by_extension("xml") {
          let mut h = syntect::easy::HighlightLines::new(syntax, &ts.themes["Solarized (dark)"]);
          if let Ok(ranges) = h.highlight_line(&xml, &ps) {
            let escaped = syntect::util::as_24_bit_terminal_escaped(&ranges[..], false);
            debug!("XMPP    \x1b[32;1m>>> {}\x1b[0m", escaped);
            highlighted = true;
          }
        }
        if !highlighted {
          debug!("XMPP    >>> {}", xml);
        }
      }
      #[cfg(not(feature = "syntax-highlighting"))]
      debug!("XMPP    >>> {}", xml);
      sink.send(Message::Text(xml.into())).await?;
      if element.is("close", ns::WEBSOCKET) {
        sink.send(Message::Close(None)).await?;
        break;
      }
    }
    Ok(())
  }

  /// Reads stanzas until the socket fails, then tells everybody that waits for one that none will come.
  async fn read_loop<S>(
    inner: Arc<Mutex<ConnectionInner>>,
    tx: mpsc::Sender<Element>,
    stream: S,
  ) -> Result<(), ConnectionError>
  where
    S: Stream<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
  {
    let result = Self::read_stanzas(&inner, &tx, stream).await;
    let reason = match &result {
      Err(e) => e.clone(),
      Ok(()) => ConnectionError::Closed("the stream ended".to_owned()),
    };
    Self::close(&inner, reason).await;
    result
  }

  async fn read_stanzas<S>(
    inner: &Arc<Mutex<ConnectionInner>>,
    tx: &mpsc::Sender<Element>,
    mut stream: S,
  ) -> Result<(), ConnectionError>
  where
    S: Stream<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
  {
    loop {
      let message = stream
        .try_next()
        .await
        .map_err(|e| ConnectionError::Closed(e.to_string()))?
        .ok_or_else(|| ConnectionError::Closed("unexpected EOF".to_owned()))?;
      let element: Element = match message {
        Message::Text(xml) => {
          #[cfg(feature = "syntax-highlighting")]
          {
            let ps = syntect::parsing::SyntaxSet::load_defaults_newlines();
            let ts = syntect::highlighting::ThemeSet::load_defaults();
            let mut highlighted = false;
            if let Some(syntax) = ps.find_syntax_by_extension("xml") {
              let mut h = syntect::easy::HighlightLines::new(syntax, &ts.themes["Solarized (dark)"]);
              if let Ok(ranges) = h.highlight_line(&xml, &ps) {
                let escaped = syntect::util::as_24_bit_terminal_escaped(&ranges[..], false);
                debug!("XMPP    \x1b[31;1m<<< {}\x1b[0m", escaped);
                highlighted = true;
              }
            }
            if !highlighted {
              debug!("XMPP    <<< {}", xml);
            }
          }
          #[cfg(not(feature = "syntax-highlighting"))]
          debug!("XMPP    <<< {}", xml);
          xml
            .parse()
            .map_err(|e| ConnectionError::Protocol(format!("unparseable stanza: {e}")))?
        },
        Message::Close(_) => return Err(ConnectionError::Closed("the server closed the WebSocket".to_owned())),
        _ => {
          warn!("unexpected non-text message on XMPP WebSocket stream: {:?}", message);
          continue;
        },
      };

      if element.is("close", ns::WEBSOCKET) {
        return Err(ConnectionError::Closed("the server closed the XMPP stream".to_owned()));
      }
      if let Some(error) = stream_error(&element) {
        warn!("the XMPP server reports an error on the stream: {}", error);
        if !matches!(inner.lock().await.state, ConnectionState::Idle) {
          return Err(error);
        }
      }

      let idle = {
        let mut locked_inner = inner.lock().await;
        locked_inner.last_received = Instant::now();
        matches!(locked_inner.state, ConnectionState::Idle)
      };
      if idle {
        Self::route(inner, tx, element).await;
      }
      else {
        Self::negotiate(inner, tx, element).await?;
      }
    }
  }

  /// A step of setting up the connection. Whatever goes wrong here ends it.
  async fn negotiate(
    inner: &Arc<Mutex<ConnectionInner>>,
    tx: &mpsc::Sender<Element>,
    element: Element,
  ) -> Result<(), ConnectionError> {
    fn protocol(e: impl fmt::Display) -> ConnectionError {
      ConnectionError::Protocol(e.to_string())
    }
    async fn send(tx: &mpsc::Sender<Element>, element: impl Into<Element>) -> Result<(), ConnectionError> {
      tx.send(element.into())
        .await
        .map_err(|_| ConnectionError::Closed("the connection is closing".to_owned()))
    }

    let mut locked_inner = inner.lock().await;

    use ConnectionState::*;
    match locked_inner.state {
      OpeningPreAuthentication => {
        Open::try_from(element).map_err(protocol)?;
        info!("Connected XMPP WebSocket");
        locked_inner.state = ReceivingFeaturesPreAuthentication;
      },
      ReceivingFeaturesPreAuthentication => {
        let auth = match &locked_inner.authentication {
          Authentication::Anonymous => Auth {
            mechanism: Mechanism::Anonymous,
            data: vec![],
          },
          Authentication::Plain { username, password } => {
            let mut data = Vec::with_capacity(username.len() + password.len() + 2);
            data.push(0u8);
            data.extend_from_slice(username.as_bytes());
            data.push(0u8);
            data.extend_from_slice(password.as_bytes());
            Auth {
              mechanism: Mechanism::Plain,
              data,
            }
          },
          Authentication::Jwt { .. } => Auth {
            mechanism: Mechanism::Anonymous,
            data: vec![],
          },
        };
        send(tx, auth).await?;
        locked_inner.state = Authenticating;
      },
      Authenticating => {
        if element.is("failure", SASL) {
          return Err(sasl_failure(&element));
        }
        Success::try_from(element).map_err(protocol)?;

        let open = Open::new(locked_inner.xmpp_domain.clone());
        send(tx, open).await?;
        locked_inner.state = OpeningPostAuthentication;
      },
      OpeningPostAuthentication => {
        Open::try_from(element).map_err(protocol)?;
        match &locked_inner.authentication {
          Authentication::Anonymous => info!("Logged in anonymously"),
          Authentication::Plain { .. } => info!("Logged in with PLAIN"),
          Authentication::Jwt { .. } => info!("Logged in with JWT"),
        }
        locked_inner.state = ReceivingFeaturesPostAuthentication;
      },
      ReceivingFeaturesPostAuthentication => {
        let iq = Iq::from_set(generate_id(), BindQuery::new(None));
        send(tx, iq).await?;
        locked_inner.state = Binding;
      },
      Binding => match Iq::try_from(element) {
        Ok(iq) => {
          let (_, payload) = iq.split();
          let jid = match payload {
            IqPayload::Result(Some(element)) => FullJid::from(BindResponse::try_from(element).map_err(protocol)?),
            IqPayload::Error(error) => return Err(ConnectionError::BindFailed(describe_stanza_error(&error))),
            _ => return Err(ConnectionError::BindFailed("the answer has no JID".to_owned())),
          };
          info!("My JID: {}", jid);
          locked_inner.jid = Some(jid.clone());

          let pinger_id = StanzaFilterId(locked_inner.next_filter_id);
          locked_inner.next_filter_id += 1;
          locked_inner
            .stanza_filters
            .push((pinger_id, Arc::new(Pinger::new(jid.clone(), tx.clone()))));

          let iq = Iq::from_get(generate_id(), DiscoInfoQuery { node: None })
            .with_from(jid.clone().into())
            .with_to(locked_inner.xmpp_domain.clone().into());
          send(tx, iq).await?;
          locked_inner.state = Discovering;
        },
        Err(e) => debug!("received unexpected element while waiting for bind response: {}", e),
      },
      Discovering => {
        let (_, payload) = Iq::try_from(element).map_err(protocol)?.split();
        match payload {
          IqPayload::Result(Some(element)) => {
            DiscoInfoResult::try_from(element).map_err(protocol)?;
          },
          IqPayload::Error(error) => return Err(ConnectionError::DiscoveryFailed(describe_stanza_error(&error))),
          _ => return Err(ConnectionError::DiscoveryFailed("the answer has no features".to_owned())),
        }

        let jid = locked_inner
          .jid
          .as_ref()
          .ok_or_else(|| protocol("missing jid"))?
          .clone();
        let iq = Iq::from_get(generate_id(), xmpp::extdisco::ServicesQuery {})
          .with_from(jid.into())
          .with_to(locked_inner.xmpp_domain.clone().into());
        send(tx, iq).await?;
        locked_inner.state = DiscoveringExternalServices;
      },
      DiscoveringExternalServices => {
        let (_, payload) = Iq::try_from(element).map_err(protocol)?.split();
        if let IqPayload::Result(Some(element)) = payload {
          let services = xmpp::extdisco::ServicesResult::try_from(element).map_err(protocol)?;
          debug!("external services: {:?}", services.services);
          locked_inner.external_services = services.services;
        }
        else {
          warn!("discovering external services failed: STUN/TURN will not work");
        }

        // Nobody may wait for it any more: that is no reason to end the connection.
        if let Some(tx) = locked_inner.connected_tx.take() {
          let _ = tx.send(Ok(()));
        }
        locked_inner.state = Idle;
      },
      Idle => unreachable!("the stanzas of an established connection are routed"),
    }
    Ok(())
  }

  /// Hands a stanza to whoever waits for it. A filter that fails is logged; it does not end the connection.
  async fn route(inner: &Arc<Mutex<ConnectionInner>>, tx: &mpsc::Sender<Element>, element: Element) {
    let (handler, jid) = {
      let mut locked_inner = inner.lock().await;
      if let Some((id, from, response)) = IqResponse::parse(&element) {
        let ours = locked_inner
          .pending_requests
          .get(&id)
          .is_some_and(|pending| match (&pending.to, &from) {
            (Some(to), Some(from)) => to == from,
            _ => true,
          });
        if ours {
          if let Some(pending) = locked_inner.pending_requests.remove(&id) {
            let _ = pending.tx.send(response);
          }
          return;
        }
      }
      (
        locked_inner
          .stanza_filters
          .iter()
          .find(|(_, filter)| filter.filter(&element))
          .map(|(_, filter)| filter.clone()),
        locked_inner.jid.clone(),
      )
    };

    match handler {
      Some(handler) => {
        if let Err(e) = handler.take(element).await {
          warn!("failed to handle a stanza: {:?}", e);
        }
      },
      None => {
        if let (Some((_, id, from)), Some(jid)) = (request_header(&element), jid) {
          debug!("rejecting a request nobody handles: {:?}", element);
          let error = stanza_error(
            ErrorType::Cancel,
            DefinedCondition::ServiceUnavailable,
            "this client does not handle that request",
          );
          let _ = tx.send(error_iq(from, &id, &jid, error)).await;
        }
      },
    }
  }

  /// The connection has ended, for `reason`: fail what waits for it (the setup, the requests) and tell the filters.
  /// Only the first reason counts.
  async fn close(inner: &Arc<Mutex<ConnectionInner>>, reason: ConnectionError) {
    let (filters, connecting, _keepalive) = {
      let mut locked_inner = inner.lock().await;
      if locked_inner.closed.is_some() {
        return;
      }
      locked_inner.closed = Some(reason.clone());
      locked_inner.pending_requests.clear();
      (
        locked_inner
          .stanza_filters
          .iter()
          .map(|(_, filter)| filter.clone())
          .collect::<Vec<SharedFilter>>(),
        locked_inner.connected_tx.take(),
        locked_inner.keepalive.take(),
      )
    };
    if let Some(connecting) = connecting {
      let _ = connecting.send(Err(reason));
    }
    for filter in filters {
      filter.connection_closed();
    }
  }
}

#[async_trait]
impl pinger::Server for Connection {
  async fn idle_for(&self) -> Duration {
    self.inner.lock().await.last_received.elapsed()
  }

  async fn ping(&self, timeout_after: Duration) -> Result<(), RequestError> {
    // An error answer shows that the server is there as well as a result does.
    self
      .request(Iq::from_get(generate_id(), xmpp_parsers::ping::Ping), timeout_after)
      .await
      .map(|_| ())
  }
}

fn sasl_failure(failure: &Element) -> ConnectionError {
  ConnectionError::AuthenticationFailed {
    condition: failure
      .children()
      .find(|child| child.name() != "text")
      .map_or_else(|| "undefined-condition".to_owned(), |condition| condition.name().to_owned()),
    text: failure
      .children()
      .find(|child| child.name() == "text")
      .map(Element::text)
      .filter(|text| !text.is_empty()),
  }
}

fn stream_error(element: &Element) -> Option<ConnectionError> {
  if !element.is("error", STREAMS) {
    return None;
  }
  Some(ConnectionError::StreamError {
    condition: element
      .children()
      .find(|child| child.ns() == STREAM_ERRORS && child.name() != "text")
      .map_or_else(|| "undefined-condition".to_owned(), |condition| condition.name().to_owned()),
    text: element
      .children()
      .find(|child| child.ns() == STREAM_ERRORS && child.name() == "text")
      .map(Element::text)
      .filter(|text| !text.is_empty()),
  })
}

fn describe_stanza_error(error: &xmpp_parsers::stanza_error::StanzaError) -> String {
  let element: Element = error.clone().into();
  let condition = error_condition(&element).unwrap_or_else(|| "undefined-condition".to_owned());
  match error.texts.values().next() {
    Some(text) if !text.is_empty() => format!("{condition}: {text}"),
    _ => condition,
  }
}

impl ConnectionInner {
  fn new(state: ConnectionState, jid: Option<FullJid>, xmpp_domain: BareJid, authentication: Authentication) -> Self {
    Self {
      state,
      jid,
      xmpp_domain,
      authentication,
      external_services: vec![],
      connected_tx: None,
      stanza_filters: vec![],
      next_filter_id: 0,
      pending_requests: HashMap::new(),
      last_received: Instant::now(),
      closed: None,
      keepalive: None,
    }
  }
}

#[cfg(test)]
impl Connection {
  /// A connection that is set up but has no socket: what the test sends is `rx`, and the stanzas it should see are
  /// fed in with [`receive`](Self::receive).
  pub(crate) fn detached(jid: FullJid, xmpp_domain: &str) -> (Self, mpsc::Receiver<Element>) {
    let (tx, rx) = mpsc::channel(64);
    let connection = Self {
      tx,
      inner: Arc::new(Mutex::new(ConnectionInner::new(
        ConnectionState::Idle,
        Some(jid),
        xmpp_domain.parse().unwrap(),
        Authentication::Anonymous,
      ))),
      tls_insecure: false,
      abort: Arc::default(),
      keepalive: Keepalive::default(),
    };
    (connection, rx)
  }

  pub(crate) async fn receive(&self, element: Element) {
    Self::route(&self.inner, &self.tx, element).await;
  }

  pub(crate) async fn end(&self) {
    Self::close(&self.inner, ConnectionError::Closed("ended by the test".to_owned())).await;
  }
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::{AtomicUsize, Ordering};

  use anyhow::bail;

  use super::*;

  const JID: &str = "me@meet.example/res";

  fn connection() -> (Connection, mpsc::Receiver<Element>) {
    Connection::detached(JID.parse().unwrap(), "meet.example")
  }

  fn stanza(xml: &str) -> Element {
    xml.parse().unwrap()
  }

  #[derive(Default)]
  struct Recorder {
    taken: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    fail: bool,
  }

  #[async_trait]
  impl StanzaFilter for Recorder {
    fn filter(&self, element: &Element) -> bool {
      element.is("message", "jabber:client")
    }

    async fn take(&self, _element: Element) -> Result<()> {
      self.taken.fetch_add(1, Ordering::SeqCst);
      if self.fail {
        bail!("on purpose");
      }
      Ok(())
    }

    fn connection_closed(&self) {
      self.closed.fetch_add(1, Ordering::SeqCst);
    }
  }

  #[tokio::test]
  async fn a_failing_filter_does_not_stop_the_stanzas() {
    let (connection, _rx) = connection();
    let taken = Arc::new(AtomicUsize::new(0));
    connection
      .add_stanza_filter(Recorder {
        taken: taken.clone(),
        fail: true,
        ..Default::default()
      })
      .await;
    for _ in 0..3 {
      connection
        .receive(stanza("<message xmlns='jabber:client' from='a@b'/>"))
        .await;
    }
    assert_eq!(taken.load(Ordering::SeqCst), 3);
  }

  #[tokio::test]
  async fn filters_hear_about_the_end_of_the_connection_and_can_be_removed() {
    let (connection, _rx) = connection();
    let (closed, removed_closed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    connection
      .add_stanza_filter(Recorder {
        closed: closed.clone(),
        ..Default::default()
      })
      .await;
    let removed = connection
      .add_stanza_filter(Recorder {
        closed: removed_closed.clone(),
        ..Default::default()
      })
      .await;
    assert!(connection.remove_stanza_filter(removed).await);
    assert!(!connection.remove_stanza_filter(removed).await);

    connection.end().await;
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    assert_eq!(removed_closed.load(Ordering::SeqCst), 0);

    let late = Arc::new(AtomicUsize::new(0));
    connection
      .add_stanza_filter(Recorder {
        closed: late.clone(),
        ..Default::default()
      })
      .await;
    assert_eq!(late.load(Ordering::SeqCst), 1);
  }

  #[tokio::test]
  async fn a_request_gets_the_answer_with_its_id_from_the_right_sender() {
    let (connection, mut rx) = connection();
    let iq = Iq::from_get("req-1", xmpp_parsers::ping::Ping).with_to("focus@auth.meet.example/focus".parse().unwrap());
    let request = tokio::spawn({
      let connection = connection.clone();
      async move { connection.request(iq, Duration::from_secs(5)).await }
    });
    let sent = rx.recv().await.unwrap();
    assert_eq!(sent.attr("id"), Some("req-1"));

    // Somebody else's answer is not ours; neither is another id.
    connection
      .receive(stanza(
        "<iq xmlns='jabber:client' type='result' id='req-1' from='mallory@meet.example/x'/>",
      ))
      .await;
    connection
      .receive(stanza(
        "<iq xmlns='jabber:client' type='result' id='req-2' from='focus@auth.meet.example/focus'/>",
      ))
      .await;
    assert!(!request.is_finished());

    connection
      .receive(stanza(
        "<iq xmlns='jabber:client' type='result' id='req-1' from='focus@auth.meet.example/focus'><conference xmlns='http://jitsi.org/protocol/focus' ready='true'/></iq>",
      ))
      .await;
    let response = request.await.unwrap().unwrap();
    assert!(matches!(response, IqResponse::Result(Some(payload)) if payload.name() == "conference"));
  }

  #[tokio::test]
  async fn a_request_fails_when_the_connection_ends_or_nobody_answers() {
    let (connection, _rx) = connection();
    let waiting = tokio::spawn({
      let connection = connection.clone();
      async move {
        connection
          .request(Iq::from_get("a", xmpp_parsers::ping::Ping), Duration::from_secs(5))
          .await
      }
    });
    tokio::task::yield_now().await;
    while !waiting.is_finished() {
      connection.end().await;
      tokio::task::yield_now().await;
    }
    assert_eq!(waiting.await.unwrap().unwrap_err(), RequestError::ConnectionClosed);
    assert_eq!(
      connection
        .request(Iq::from_get("b", xmpp_parsers::ping::Ping), Duration::from_secs(5))
        .await
        .unwrap_err(),
      RequestError::ConnectionClosed
    );

    let (connection, _rx) = self::connection();
    assert_eq!(
      connection
        .request(Iq::from_get("c", xmpp_parsers::ping::Ping), Duration::from_millis(20))
        .await
        .unwrap_err(),
      RequestError::Timeout
    );
  }

  #[tokio::test]
  async fn a_request_nobody_handles_is_rejected() {
    let (connection, mut rx) = connection();
    connection
      .receive(stanza(
        "<iq xmlns='jabber:client' type='get' id='q1' from='room@muc.meet.example/someone'><query xmlns='urn:example'/></iq>",
      ))
      .await;
    let reply = rx.recv().await.unwrap();
    assert_eq!(reply.attr("type"), Some("error"));
    assert_eq!(reply.attr("id"), Some("q1"));
    assert_eq!(reply.attr("to"), Some("room@muc.meet.example/someone"));
    assert!(reply
      .get_child("error", "jabber:client")
      .unwrap()
      .has_child("service-unavailable", "urn:ietf:params:xml:ns:xmpp-stanzas"));

    // Answers nobody waits for are dropped silently.
    connection
      .receive(stanza("<iq xmlns='jabber:client' type='result' id='q2' from='a@b'/>"))
      .await;
    assert!(rx.try_recv().is_err());
  }

  #[tokio::test]
  async fn the_cached_services_are_used_when_the_server_cannot_be_asked() {
    let (connection, _rx) = connection();
    connection.inner.lock().await.external_services = vec![xmpp::extdisco::Service {
      r#type: "stun".to_owned(),
      host: "stun.meet.example".to_owned(),
      port: Some(3478),
      transport: None,
      username: None,
      password: None,
    }];
    connection.end().await;
    let services = connection.external_services_fresh().await;
    assert_eq!(services.len(), 1);
    assert_eq!(services[0].host, "stun.meet.example");
  }

  #[tokio::test]
  async fn the_services_come_from_the_server_or_from_the_cache() {
    let (connection, mut rx) = connection();
    let refreshed = tokio::spawn({
      let connection = connection.clone();
      async move { connection.external_services_fresh().await }
    });
    let request = rx.recv().await.unwrap();
    let id = request.attr("id").unwrap().to_owned();
    assert_eq!(request.attr("to"), Some("meet.example"));
    connection
      .receive(stanza(&format!(
        "<iq xmlns='jabber:client' type='result' id='{id}' from='meet.example'>\
           <services xmlns='urn:xmpp:extdisco:2'><service type='turn' host='turn.example' port='3478' username='u' password='p'/></services>\
         </iq>"
      )))
      .await;
    let services = refreshed.await.unwrap();
    assert_eq!(services.len(), 1);
    assert_eq!(connection.external_services().await.len(), 1);
  }

  use std::{
    pin::Pin,
    task::{Context as TaskContext, Poll},
  };

  use tokio::task::JoinHandle;
  use tokio_stream::wrappers::UnboundedReceiverStream;

  const OPEN: &str = "<open xmlns='urn:ietf:params:xml:ns:xmpp-framing' from='meet.example' id='s1' version='1.0'/>";
  const FEATURES: &str = "<stream:features xmlns:stream='http://etherx.jabber.org/streams'/>";

  #[derive(Debug)]
  struct SocketClosed;

  impl fmt::Display for SocketClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      f.write_str("the socket is closed")
    }
  }

  impl std::error::Error for SocketClosed {}

  struct ChannelSink(mpsc::UnboundedSender<Message>);

  impl Sink<Message> for ChannelSink {
    type Error = SocketClosed;

    fn poll_ready(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), SocketClosed>> {
      Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), SocketClosed> {
      self.0.send(item).map_err(|_| SocketClosed)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), SocketClosed>> {
      Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Result<(), SocketClosed>> {
      Poll::Ready(Ok(()))
    }
  }

  /// The XMPP server on the other end of the socket.
  struct Peer {
    to_client: mpsc::UnboundedSender<std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>,
    from_client: mpsc::UnboundedReceiver<Message>,
  }

  impl Peer {
    async fn expect(&mut self, what: &str) -> Element {
      loop {
        match timeout(Duration::from_secs(5), self.from_client.recv())
          .await
          .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
          .unwrap_or_else(|| panic!("the client is gone, waiting for {what}"))
        {
          Message::Text(xml) => return xml.as_str().parse().unwrap(),
          _ => continue,
        }
      }
    }

    fn send(&self, xml: &str) {
      self.to_client.send(Ok(Message::Text(xml.into()))).unwrap();
    }

    fn answer(&self, request: &Element, content: &str) {
      self.send(&format!(
        "<iq xmlns='jabber:client' type='result' id='{}' from='meet.example'>{content}</iq>",
        request.attr("id").unwrap()
      ));
    }

    /// The server's side of the setup, up to the SASL step.
    async fn open(&mut self) {
      assert!(self.expect("the open").await.is("open", ns::WEBSOCKET));
      self.send(OPEN);
      self.send(FEATURES);
      assert!(self.expect("the auth").await.is("auth", SASL));
    }

    /// … up to the binding of the resource.
    async fn authenticate(&mut self) -> Element {
      self.open().await;
      self.send("<success xmlns='urn:ietf:params:xml:ns:xmpp-sasl'/>");
      assert!(self.expect("the second open").await.is("open", ns::WEBSOCKET));
      self.send(OPEN);
      self.send(FEATURES);
      self.expect("the bind").await
    }

    /// … up to the service discovery.
    async fn bind(&mut self) -> Element {
      let bind = self.authenticate().await;
      self.answer(
        &bind,
        "<bind xmlns='urn:ietf:params:xml:ns:xmpp-bind'><jid>me@meet.example/res</jid></bind>",
      );
      self.expect("the disco").await
    }

    async fn serve_setup(&mut self) {
      let disco = self.bind().await;
      self.answer(
        &disco,
        "<query xmlns='http://jabber.org/protocol/disco#info'><identity category='server' type='im'/>\
           <feature var='urn:xmpp:ping'/></query>",
      );
      let services = self.expect("the external services").await;
      self.answer(&services, "<services xmlns='urn:xmpp:extdisco:2'/>");
    }

    fn error_for(&self, request: &Element, condition: &str) {
      self.send(&format!(
        "<iq xmlns='jabber:client' type='error' id='{}' from='meet.example'>\
           <error type='cancel'><{condition} xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
             <text xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'>on purpose</text></error></iq>",
        request.attr("id").unwrap()
      ));
    }
  }

  fn socket(keepalive: Keepalive) -> (Connection, Peer, JoinHandle<()>) {
    let (to_client, client_stream) = mpsc::unbounded_channel();
    let (client_sink, from_client) = mpsc::unbounded_channel();
    let (connection, background) = Connection::over(
      ChannelSink(client_sink),
      UnboundedReceiverStream::new(client_stream),
      "meet.example".parse().unwrap(),
      Authentication::Jwt {
        token: "token".to_owned(),
      },
      false,
      keepalive,
    );
    (
      connection,
      Peer {
        to_client,
        from_client,
      },
      tokio::spawn(background),
    )
  }

  /// Runs `connect` against a server that follows `script`.
  async fn connect_to<F, Fut>(script: F) -> (Result<()>, Connection, JoinHandle<()>)
  where
    F: FnOnce(Peer) -> Fut,
    Fut: Future<Output = Peer> + Send + 'static,
  {
    let (connection, peer, background) = socket(Keepalive::default());
    let server = tokio::spawn(script(peer));
    let result = timeout(Duration::from_secs(5), connection.connect())
      .await
      .expect("connect returns instead of waiting for ever");
    let _peer = server.await.unwrap();
    (result, connection, background)
  }

  fn failure_of(result: Result<()>) -> ConnectionError {
    result
      .expect_err("the connection is refused")
      .downcast_ref::<ConnectionError>()
      .expect("the error is a ConnectionError")
      .clone()
  }

  #[tokio::test]
  async fn a_connection_is_set_up_and_knows_its_jid_and_services() {
    let (result, connection, background) = connect_to(|mut peer| async move {
      let disco = peer.bind().await;
      peer.answer(
        &disco,
        "<query xmlns='http://jabber.org/protocol/disco#info'><identity category='server' type='im'/></query>",
      );
      let services = peer.expect("the external services").await;
      peer.answer(
        &services,
        "<services xmlns='urn:xmpp:extdisco:2'><service type='stun' host='stun.meet.example' port='3478'/></services>",
      );
      peer
    })
    .await;
    result.unwrap();
    assert_eq!(connection.jid().await.unwrap().to_string(), "me@meet.example/res");
    assert_eq!(connection.external_services().await[0].host, "stun.meet.example");
    // Connecting again is not a new setup.
    connection.connect().await.unwrap();
    assert!(!background.is_finished());
  }

  #[tokio::test]
  async fn a_sasl_failure_is_the_error_of_connect_with_the_reason_of_the_server() {
    let (result, connection, background) = connect_to(|mut peer| async move {
      peer.open().await;
      peer.send(
        "<failure xmlns='urn:ietf:params:xml:ns:xmpp-sasl'><not-authorized/>\
           <text xml:lang='en'>JWT is expired</text></failure>",
      );
      peer
    })
    .await;
    assert_eq!(
      failure_of(result),
      ConnectionError::AuthenticationFailed {
        condition: "not-authorized".to_owned(),
        text: Some("JWT is expired".to_owned()),
      }
    );
    timeout(Duration::from_secs(5), background).await.unwrap().unwrap();
    assert_eq!(
      connection
        .request(Iq::from_get("a", xmpp_parsers::ping::Ping), Duration::from_secs(1))
        .await
        .unwrap_err(),
      RequestError::ConnectionClosed
    );
    assert!(matches!(
      failure_of(connection.connect().await),
      ConnectionError::AuthenticationFailed { .. }
    ));
  }

  #[tokio::test]
  async fn a_stream_error_is_the_error_of_connect() {
    let (result, ..) = connect_to(|mut peer| async move {
      peer.open().await;
      peer.send(
        "<stream:error xmlns:stream='http://etherx.jabber.org/streams'>\
           <host-unknown xmlns='urn:ietf:params:xml:ns:xmpp-streams'/></stream:error>",
      );
      peer
    })
    .await;
    assert_eq!(
      failure_of(result),
      ConnectionError::StreamError {
        condition: "host-unknown".to_owned(),
        text: None
      }
    );
  }

  #[tokio::test]
  async fn a_refused_bind_is_the_error_of_connect() {
    let (result, ..) = connect_to(|mut peer| async move {
      let bind = peer.authenticate().await;
      peer.error_for(&bind, "conflict");
      peer
    })
    .await;
    assert_eq!(failure_of(result), ConnectionError::BindFailed("conflict: on purpose".to_owned()));
  }

  #[tokio::test]
  async fn a_refused_discovery_is_the_error_of_connect() {
    let (result, ..) = connect_to(|mut peer| async move {
      let disco = peer.bind().await;
      peer.error_for(&disco, "service-unavailable");
      peer
    })
    .await;
    assert_eq!(
      failure_of(result),
      ConnectionError::DiscoveryFailed("service-unavailable: on purpose".to_owned())
    );
  }

  #[tokio::test]
  async fn a_server_that_hangs_up_during_the_setup_is_the_error_of_connect() {
    let (result, _, background) = connect_to(|mut peer| async move {
      peer.open().await;
      peer.to_client.send(Ok(Message::Close(None))).unwrap();
      peer
    })
    .await;
    assert!(matches!(failure_of(result), ConnectionError::Closed(_)));
    timeout(Duration::from_secs(5), background).await.unwrap().unwrap();
  }

  #[tokio::test]
  async fn a_server_that_says_nothing_times_out_and_the_connection_is_ended() {
    let (connection, mut peer, background) = socket(Keepalive::default());
    let result = timeout(Duration::from_secs(5), connection.connect_within(Duration::from_millis(100)))
      .await
      .expect("connect gives up");
    assert_eq!(failure_of(result), ConnectionError::Timeout);
    assert!(peer.expect("the open").await.is("open", ns::WEBSOCKET));
    // Nothing is left half set up.
    timeout(Duration::from_secs(5), background).await.unwrap().unwrap();
    assert_eq!(failure_of(connection.connect().await), ConnectionError::Timeout);
  }

  #[tokio::test]
  async fn a_setup_that_finishes_after_connect_gave_up_does_not_matter() {
    let (connection, mut peer, _background) = socket(Keepalive::default());
    // `connect` is dropped, as its caller may do; the setup goes on and finishes.
    let _ = timeout(Duration::from_millis(50), connection.connect()).await;
    let server = tokio::spawn(async move {
      peer.serve_setup().await;
      peer
    });
    // There is no connect to answer, and the connection does not end because of that.
    let _peer = server.await.unwrap();
    timeout(Duration::from_secs(5), async {
      while !matches!(connection.inner.lock().await.state, ConnectionState::Idle) {
        tokio::time::sleep(Duration::from_millis(5)).await;
      }
    })
    .await
    .expect("the setup finishes");
    assert!(connection.inner.lock().await.closed.is_none());
    assert!(!_background.is_finished());
  }

  const FAST_PINGS: Keepalive = Keepalive {
    interval: Duration::from_millis(40),
    timeout: Duration::from_millis(40),
    max_failures: 2,
  };

  #[tokio::test]
  async fn a_server_that_does_not_answer_the_pings_ends_the_connection() {
    let (connection, mut peer, background) = socket(FAST_PINGS);
    let closed = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(async move {
      peer.serve_setup().await;
      peer
    });
    connection
      .add_stanza_filter(Recorder {
        closed: closed.clone(),
        ..Default::default()
      })
      .await;
    connection.connect().await.unwrap();
    let mut peer = server.await.unwrap();

    // The server reads the pings and says nothing; the connection does not survive that for long.
    let mut pings = 0;
    timeout(Duration::from_secs(5), async {
      while let Some(Message::Text(xml)) = peer.from_client.recv().await {
        let ping: Element = xml.as_str().parse().unwrap();
        pings += usize::from(ping.has_child("ping", "urn:xmpp:ping"));
      }
    })
    .await
    .expect("the connection ends");
    assert_eq!(pings, 2);
    timeout(Duration::from_secs(5), background).await.unwrap().unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1, "the filters are told");
    assert_eq!(
      connection
        .request(Iq::from_get("a", xmpp_parsers::ping::Ping), Duration::from_secs(1))
        .await
        .unwrap_err(),
      RequestError::ConnectionClosed
    );
  }

  #[tokio::test]
  async fn a_server_that_answers_the_pings_keeps_the_connection() {
    let (connection, mut peer, background) = socket(FAST_PINGS);
    let server = tokio::spawn(async move {
      peer.serve_setup().await;
      peer
    });
    connection.connect().await.unwrap();
    let mut peer = server.await.unwrap();

    let mut answered = 0;
    timeout(Duration::from_secs(5), async {
      while answered < 4 {
        let ping = peer.expect("a ping").await;
        assert!(ping.has_child("ping", "urn:xmpp:ping"), "{ping:?}");
        peer.answer(&ping, "");
        answered += 1;
      }
    })
    .await
    .unwrap();
    assert!(!background.is_finished());
  }
}
