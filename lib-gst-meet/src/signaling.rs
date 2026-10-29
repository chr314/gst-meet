use std::{collections::BTreeMap, convert::TryFrom, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use jid::Jid;
use jitsi_xmpp_parsers::jingle::{Action, Jingle};
use minidom::Element;
use tracing::{debug, warn};
use xmpp_parsers::{
  disco::DiscoInfoQuery,
  jingle::{Reason, ReasonElement},
  ns,
  stanza_error::{DefinedCondition, ErrorType, StanzaError},
};

use crate::{
  disco,
  error::{ConferenceLeftReason, JoinError},
  jingle::expand_json_sources,
  jingle_actor::Command,
  muc::{self, presence_nick, MucUser},
  shared::{Phase, Shared},
  stanza_filter::StanzaFilter,
  util::generate_id,
  xmpp::iq::{error_iq, request_header, result_iq, stanza_error},
};

const JINGLE_ERRORS: &str = "urn:xmpp:jingle:errors:1";

pub(crate) struct Signaling(pub(crate) Arc<Shared>);

#[async_trait]
impl StanzaFilter for Signaling {
  fn filter(&self, element: &Element) -> bool {
    self.0.wants(element)
  }

  async fn take(&self, element: Element) -> Result<()> {
    self.0.handle_stanza(element).await
  }

  fn connection_closed(&self) {
    self.0.conference_left(ConferenceLeftReason::ConnectionLost);
  }
}

impl Shared {
  fn wants(&self, element: &Element) -> bool {
    let Some(from) = element.attr("from").and_then(|from| from.parse::<Jid>().ok()) else {
      return false;
    };
    let is_iq = element.is("iq", ns::DEFAULT_NS);
    (is_iq && from == self.config.focus)
      || (from.to_bare() == self.muc && (is_iq || element.is("presence", ns::DEFAULT_NS)))
  }

  pub(crate) async fn handle_stanza(self: &Arc<Self>, element: Element) -> Result<()> {
    if element.is("presence", ns::DEFAULT_NS) {
      self.handle_presence(&element);
      Ok(())
    }
    else if element.is("iq", ns::DEFAULT_NS) {
      self.handle_iq(&element).await
    }
    else {
      Ok(())
    }
  }

  async fn handle_iq(self: &Arc<Self>, element: &Element) -> Result<()> {
    let Some((type_, id, from)) = request_header(element) else {
      debug!("ignoring an IQ that is no request: {:?}", element);
      return Ok(());
    };
    let Some(payload) = element.children().next() else {
      let error = stanza_error(ErrorType::Modify, DefinedCondition::BadRequest, "the request is empty");
      return self.reply_error(from, &id, error).await;
    };
    match (type_, payload) {
      ("get", payload) if payload.is("query", ns::DISCO_INFO) => self.handle_disco(from, &id, payload).await,
      ("set", payload) if payload.is("jingle", ns::JINGLE) => self.handle_jingle(from, &id, payload).await,
      _ => {
        self
          .reply_error(
            from,
            &id,
            stanza_error(
              ErrorType::Cancel,
              DefinedCondition::ServiceUnavailable,
              "this client does not handle that request",
            ),
          )
          .await
      },
    }
  }

  async fn reply_result(&self, to: Jid, id: &str) -> Result<()> {
    self.xmpp_tx.send(result_iq(to, id, &self.jid)).await?;
    Ok(())
  }

  async fn reply_error(&self, to: Jid, id: &str, error: StanzaError) -> Result<()> {
    self.xmpp_tx.send(error_iq(to, id, &self.jid, error)).await?;
    Ok(())
  }

  async fn handle_disco(&self, from: Jid, id: &str, query: &Element) -> Result<()> {
    let Ok(query) = DiscoInfoQuery::try_from(query.clone()) else {
      return self
        .reply_error(from, id, stanza_error(ErrorType::Modify, DefinedCondition::BadRequest, "invalid query"))
        .await;
    };
    debug!("Received disco info query from {} for node {:?}", from, query.node);
    match disco::info(query.node.as_deref()) {
      Some(info) => {
        let iq = xmpp_parsers::iq::Iq::from_result(id, Some(info))
          .with_from(self.jid.clone().into())
          .with_to(from);
        self.xmpp_tx.send(iq.into()).await?;
        Ok(())
      },
      None => {
        let error = stanza_error(
          ErrorType::Cancel,
          DefinedCondition::ItemNotFound,
          format!("Unknown disco#info node: {}", query.node.unwrap_or_default()),
        );
        self.reply_error(from, id, error).await
      },
    }
  }

  async fn handle_jingle(self: &Arc<Self>, from: Jid, id: &str, payload: &Element) -> Result<()> {
    let jingle = match Jingle::try_from(payload.clone()) {
      Ok(jingle) => jingle,
      Err(e) => {
        debug!("IQ did not successfully parse as Jingle: {:?}", e);
        let error = stanza_error(ErrorType::Modify, DefinedCondition::BadRequest, "invalid Jingle");
        return self.reply_error(from, id, error).await;
      },
    };

    if from != self.focus_jid {
      return self.handle_foreign_jingle(from, id, jingle).await;
    }

    match jingle.action {
      Action::SessionInitiate => {
        if self.lock().phase == Phase::Left {
          return self.reply_error(from, id, unknown_session()).await;
        }
        self.reply_result(from, id).await?;
        self.lock().known_sid = Some(jingle.sid.0.clone());
        self.command_for(Command::Initiate, jingle);
      },
      Action::SessionInfo => {
        debug!("Ignoring session-info");
        self.reply_result(from, id).await?;
      },
      Action::SourceAdd
      | Action::SourceRemove
      | Action::TransportInfo
      | Action::SessionTerminate
      | Action::TransportReplace => {
        let known = {
          let mut state = self.lock();
          let known = state.known_sid.as_deref() == Some(jingle.sid.0.as_str());
          if known && jingle.action == Action::SessionTerminate {
            state.known_sid = None;
          }
          known
        };
        if !known {
          debug!("Rejecting {:?} for the unknown session {}", jingle.action, jingle.sid.0);
          return self.reply_error(from, id, unknown_session()).await;
        }
        self.reply_result(from, id).await?;
        let command = match jingle.action {
          Action::SourceAdd => Command::SourceAdd,
          Action::SourceRemove => Command::SourceRemove,
          Action::TransportInfo => Command::TransportInfo,
          Action::SessionTerminate => Command::Terminate,
          _ => Command::TransportReplace,
        };
        self.command_for(command, jingle);
      },
      other => {
        debug!("Rejecting the unsupported Jingle action {:?}", other);
        let error = stanza_error(
          ErrorType::Cancel,
          DefinedCondition::FeatureNotImplemented,
          "unsupported Jingle action",
        );
        self.reply_error(from, id, error).await?;
      },
    }
    Ok(())
  }

  fn command_for(&self, command: fn(Jingle) -> Command, mut jingle: Jingle) {
    if matches!(
      jingle.action,
      Action::SessionInitiate | Action::SourceAdd | Action::SourceRemove | Action::TransportReplace
    ) {
      expand_json_sources(&mut jingle);
    }
    self.command(command(jingle));
  }

  /// A Jingle IQ from somebody who is not the focus: we do not do peer-to-peer.
  async fn handle_foreign_jingle(&self, from: Jid, id: &str, jingle: Jingle) -> Result<()> {
    if jingle.action != Action::SessionInitiate {
      return self.reply_error(from, id, unknown_session()).await;
    }
    debug!("Declining P2P Jingle session-initiate from {}", from);
    self.reply_result(from.clone(), id).await?;
    let terminate = Jingle::new(Action::SessionTerminate, jingle.sid).set_reason(ReasonElement {
      reason: Reason::Decline,
      texts: BTreeMap::new(),
    });
    let iq = xmpp_parsers::iq::Iq::from_set(generate_id(), terminate)
      .with_from(self.jid.clone().into())
      .with_to(from);
    self.xmpp_tx.send(iq.into()).await?;
    Ok(())
  }

  fn handle_presence(self: &Arc<Self>, presence: &Element) {
    if self.lock().phase == Phase::Left {
      return;
    }
    let Some(from) = presence.attr("from").and_then(|from| from.parse::<Jid>().ok()) else {
      return;
    };
    let resource = from.resource().map(|resource| resource.as_str());
    let own = from == self.muc_jid;
    let user = MucUser::parse(presence);

    match presence.attr("type") {
      Some("error") => {
        // The room answers our join with an error from our occupant JID, or from the room itself.
        if own || resource.is_none() {
          self.join_refused(muc::join_error(presence));
        }
        else {
          debug!("ignoring an error presence from {}", from);
        }
      },
      Some("unavailable") => {
        if own || user.as_ref().is_some_and(MucUser::is_self) {
          self.left_the_room(user.unwrap_or_default());
        }
        else if let Some(resource) = resource {
          self.participant_gone(resource);
        }
      },
      _ => {
        if own || user.as_ref().is_some_and(MucUser::is_self) {
          self.joined_the_room();
        }
        else if let Some(resource) = resource.filter(|resource| *resource != "focus") {
          let user = user.unwrap_or_default();
          self.participant_present(resource, user.jid, presence_nick(presence).or(user.nick));
        }
      },
    }
  }

  fn joined_the_room(&self) {
    let waiter = {
      let mut state = self.lock();
      if state.phase != Phase::Joining {
        return;
      }
      debug!("Joined MUC: {}", self.muc);
      state.phase = Phase::Joined;
      state.in_room = true;
      state.muc_waiter.take()
    };
    if let Some(waiter) = waiter {
      let _ = waiter.send(Ok(()));
    }
  }

  /// Anything the room says about us after the join is not about joining any more.
  fn join_refused(&self, error: JoinError) {
    let waiter = {
      let mut state = self.lock();
      if state.phase != Phase::Joining {
        debug!("ignoring an error presence about us after the join: {}", error);
        return;
      }
      state.in_room = false;
      state.muc_waiter.take()
    };
    if let Some(waiter) = waiter {
      let _ = waiter.send(Err(error));
    }
  }

  /// Our own occupant has gone away: kicked, banned, the room destroyed, or we left.
  fn left_the_room(self: &Arc<Self>, user: MucUser) {
    let reason = user.left_reason();
    {
      let mut state = self.lock();
      if state.left.is_some() {
        // Our own unavailable presence, coming back.
        return;
      }
      state.in_room = false;
    }
    warn!("we are no longer in the room: {:?}", reason);
    self.conference_left(reason);
  }
}

fn unknown_session() -> StanzaError {
  StanzaError {
    other: Some(Element::builder("unknown-session", JINGLE_ERRORS).build()),
    ..stanza_error(ErrorType::Cancel, DefinedCondition::ItemNotFound, "unknown session")
  }
}
