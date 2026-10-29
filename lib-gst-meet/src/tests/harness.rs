use std::{
  sync::{Arc, Mutex},
  time::Duration,
};

use minidom::Element;
use tokio::{sync::mpsc, time::timeout};
use xmpp_parsers::ns;

use super::{fake_media::FakeMedia, xml::*};
use crate::{
  conference::{JitsiConference, JitsiConferenceConfig},
  xmpp::connection::Connection,
};

const WAIT: Duration = Duration::from_secs(5);

pub(super) fn config() -> JitsiConferenceConfig {
  JitsiConferenceConfig::builder()
    .muc(ROOM.parse().unwrap())
    .focus(FOCUS_ACCOUNT.parse().unwrap())
    .nick("Rec".to_owned())
    .receive_mode(crate::ReceiveMode::Encoded)
    .join_timeout(Duration::from_secs(10))
    .media_session_timeout(Duration::from_secs(1))
    .build()
}

/// The other end of the XMPP connection: Prosody and Jicofo.
pub(super) struct Server {
  pub(super) connection: Connection,
  sent: mpsc::Receiver<Element>,
}

impl Server {
  pub(super) fn new() -> Self {
    let (connection, mut raw) = Connection::detached(MY_JID.parse().unwrap(), "meet.example");
    let (tx, sent) = mpsc::channel(64);
    // The server answers the question for STUN and TURN servers at once, whenever it is asked.
    tokio::spawn({
      let connection = connection.clone();
      async move {
        while let Some(stanza) = raw.recv().await {
          if stanza.get_child("services", "urn:xmpp:extdisco:2").is_some() {
            connection
              .receive(parse(&format!(
                "<iq xmlns='jabber:client' type='result' id='{}' from='meet.example' to='{MY_JID}'>\
                   <services xmlns='urn:xmpp:extdisco:2'>\
                     <service type='turn' host='turn.meet.example' port='3478' transport='udp' username='u' password='p'/>\
                   </services></iq>",
                stanza.attr("id").unwrap()
              )))
              .await;
          }
          else if tx.send(stanza).await.is_err() {
            break;
          }
        }
      }
    });
    Self { connection, sent }
  }

  pub(super) async fn next(&mut self) -> Element {
    timeout(WAIT, self.sent.recv())
      .await
      .expect("timed out waiting for a stanza")
      .expect("the connection is gone")
  }

  /// The next stanza that satisfies `matches`; the ones before it are dropped.
  pub(super) async fn next_matching(&mut self, what: &str, matches: impl Fn(&Element) -> bool) -> Element {
    loop {
      let stanza = timeout(WAIT, self.sent.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .expect("the connection is gone");
      if matches(&stanza) {
        return stanza;
      }
    }
  }

  /// A Jingle IQ of the action we send to the focus.
  pub(super) async fn jingle(&mut self, action: &str) -> Element {
    let what = format!("a Jingle {action}");
    self
      .next_matching(&what, |stanza| {
        stanza.attr("type") == Some("set")
          && stanza
            .get_child("jingle", ns::JINGLE)
            .is_some_and(|jingle| jingle.attr("action") == Some(action))
      })
      .await
  }

  pub(super) async fn presence(&mut self) -> Element {
    self.next_matching("a presence", |stanza| stanza.is("presence", ns::DEFAULT_NS)).await
  }

  pub(super) async fn result_for(&mut self, id: &str) -> Element {
    let what = format!("the answer to {id}");
    self
      .next_matching(&what, |stanza| {
        stanza.is("iq", ns::DEFAULT_NS) && stanza.attr("id") == Some(id) && stanza.attr("type") != Some("set")
      })
      .await
  }

  pub(super) async fn assert_quiet(&mut self) {
    if let Ok(Some(stanza)) = timeout(Duration::from_millis(100), self.sent.recv()).await {
      panic!("expected nothing, got {stanza:?}");
    }
  }

  pub(super) async fn receive(&self, xml: &str) {
    self.connection.receive(parse(xml)).await;
  }

  pub(super) async fn answer(&self, request: &Element) {
    let from = request.attr("to").expect("a request has a recipient");
    self
      .receive(&format!(
        "<iq xmlns='jabber:client' type='result' id='{}' from='{from}' to='{MY_JID}'/>",
        request.attr("id").unwrap()
      ))
      .await;
  }

  pub(super) async fn answer_error(&self, request: &Element, condition: &str, text: &str) {
    let from = request.attr("to").expect("a request has a recipient");
    self
      .receive(&format!(
        "<iq xmlns='jabber:client' type='error' id='{}' from='{from}' to='{MY_JID}'>\
           <error type='cancel'><{condition} xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
             <text xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'>{text}</text></error></iq>",
        request.attr("id").unwrap()
      ))
      .await;
  }

  /// What the focus does for a client that joins: it answers the conference request, the room confirms the
  /// presence, and the focus offers a session and gets it accepted. Returns the accept.
  pub(super) async fn serve_join(&mut self, occupants: &[&str], offer: &Offer<'_>) -> Element {
    let request = self
      .next_matching("the conference request", |stanza| {
        stanza.get_child("conference", "http://jitsi.org/protocol/focus").is_some()
      })
      .await;
    self.receive(&conference_ready(request.attr("id").unwrap(), true)).await;

    let join = self.presence().await;
    assert!(join.has_child("x", ns::MUC), "the joining presence asks for the MUC extension");
    for who in occupants {
      self.receive(&occupant_presence(who)).await;
    }
    self.receive(&self_presence()).await;

    self.offer_session("initiate-1", offer).await
  }

  /// The focus offers a session, which we accept. Returns our session-accept.
  pub(super) async fn offer_session(&mut self, iq_id: &str, offer: &Offer<'_>) -> Element {
    self.receive(&session_initiate(iq_id, offer)).await;
    let ack = self.result_for(iq_id).await;
    assert_eq!(ack.attr("type"), Some("result"), "{ack:?}");
    let accept = self.jingle("session-accept").await;
    self.answer(&accept).await;
    accept
  }

  pub(super) async fn expect_ack(&mut self, id: &str) {
    let ack = self.result_for(id).await;
    assert_eq!(ack.attr("type"), Some("result"), "{ack:?}");
  }
}

/// What the callbacks saw, in order.
#[derive(Clone, Default)]
pub(super) struct Events(Arc<Mutex<Vec<String>>>);

impl Events {
  pub(super) fn push(&self, event: impl Into<String>) {
    self.0.lock().unwrap().push(event.into());
  }

  pub(super) fn all(&self) -> Vec<String> {
    self.0.lock().unwrap().clone()
  }

  pub(super) async fn wait_for(&self, event: &str) {
    let found = async {
      while !self.all().iter().any(|seen| seen == event) {
        tokio::time::sleep(Duration::from_millis(5)).await;
      }
    };
    if timeout(WAIT, found).await.is_err() {
      panic!("timed out waiting for {event:?}, saw {:#?}", self.all());
    }
  }
}

pub(super) struct Harness {
  /// The session-accept of the join.
  pub(super) accept: Element,
  pub(super) server: Server,
  pub(super) conference: JitsiConference,
  pub(super) media: Arc<FakeMedia>,
  pub(super) events: Events,
}

impl Harness {
  pub(super) async fn join() -> Self {
    Self::join_with(config(), &["bob"], &Offer::default()).await
  }

  pub(super) async fn join_with(config: JitsiConferenceConfig, occupants: &[&str], offer: &Offer<'_>) -> Self {
    let mut server = Server::new();
    let media = Arc::new(Mutex::new(None));
    let join = JitsiConference::join_with(server.connection.clone(), config, {
      let media = media.clone();
      move |_, _, hooks| {
        let fake = FakeMedia::new(hooks);
        *media.lock().unwrap() = Some(fake.clone());
        Ok(fake)
      }
    });
    let (conference, accept) = tokio::join!(join, server.serve_join(occupants, offer));
    let media = media.lock().unwrap().take().unwrap();
    Self {
      accept,
      server,
      conference: conference.expect("the join succeeds"),
      media,
      events: Events::default(),
    }
  }

  pub(super) async fn record_events(&self) {
    let conference = &self.conference;
    let events = self.events.clone();
    conference
      .on_participant({
        let events = events.clone();
        move |participant| {
          let events = events.clone();
          Box::pin(async move {
            events.push(format!("participant {}", participant.muc_jid.resource()));
            Ok(())
          })
        }
      })
      .await;
    conference
      .on_participant_left({
        let events = events.clone();
        move |participant| {
          let events = events.clone();
          Box::pin(async move {
            events.push(format!("participant left {}", participant.muc_jid.resource()));
            Ok(())
          })
        }
      })
      .await;
    conference
      .on_source({
        let events = events.clone();
        move |event| {
          let events = events.clone();
          Box::pin(async move {
            events.push(match event {
              crate::SourceEvent::Added(source) => format!("source added {}", source.ssrc),
              crate::SourceEvent::Removed(source) => format!("source removed {}", source.ssrc),
            });
            Ok(())
          })
        }
      })
      .await;
    conference
      .on_media_session({
        let events = events.clone();
        move |event| {
          let events = events.clone();
          Box::pin(async move {
            events.push(match event {
              crate::MediaSessionEvent::Started { bridge_session_id } => {
                format!("session started {}", bridge_session_id.unwrap_or_default())
              },
              crate::MediaSessionEvent::Ended { restarting, .. } => format!("session ended restarting={restarting}"),
              crate::MediaSessionEvent::ConnectionStateChanged(state) => format!("session {state:?}"),
            });
            Ok(())
          })
        }
      })
      .await;
    conference
      .on_conference_left({
        let events = events.clone();
        move |reason| {
          let events = events.clone();
          Box::pin(async move {
            events.push(format!("left {reason:?}"));
            Ok(())
          })
        }
      })
      .await;
  }
}

/// A join that the test scripts itself: `script` plays the server and gives it back.
pub(super) async fn try_join<F, Fut>(
  config: JitsiConferenceConfig,
  script: F,
) -> (anyhow::Result<JitsiConference>, Server, Arc<FakeMedia>)
where
  F: FnOnce(Server) -> Fut,
  Fut: std::future::Future<Output = Server>,
{
  try_join_failing_to_start(config, None, script).await
}

/// `try_join`, with the media layer failing to start the first session with `start_error`.
pub(super) async fn try_join_failing_to_start<F, Fut>(
  config: JitsiConferenceConfig,
  start_error: Option<&str>,
  script: F,
) -> (anyhow::Result<JitsiConference>, Server, Arc<FakeMedia>)
where
  F: FnOnce(Server) -> Fut,
  Fut: std::future::Future<Output = Server>,
{
  let server = Server::new();
  let media = Arc::new(Mutex::new(None));
  let join = JitsiConference::join_with(server.connection.clone(), config, {
    let media = media.clone();
    let start_error = start_error.map(ToOwned::to_owned);
    move |_, _, hooks| {
      let fake = FakeMedia::new(hooks);
      if let Some(message) = &start_error {
        fake.fail_next_start(message);
      }
      *media.lock().unwrap() = Some(fake.clone());
      Ok(fake)
    }
  });
  let (conference, server) = tokio::join!(join, script(server));
  let media = media.lock().unwrap().take().unwrap();
  (conference, server, media)
}
