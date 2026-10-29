use jid::FullJid;
use minidom::Element;
use xmpp_parsers::ns;

use crate::{error::{ConferenceLeftReason, JoinError}, xmpp::ns::JITSI_JITMEET};

const MUC_USER: &str = ns::MUC_USER;
const STANZAS: &str = ns::XMPP_STANZAS;
const JITSI_VISITORS: &str = "jitsi:visitors";

/// The `<x xmlns='http://jabber.org/protocol/muc#user'/>` of a presence.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct MucUser {
  pub(crate) statuses: Vec<u16>,
  /// The real JID of the occupant, if the room reveals it to us.
  pub(crate) jid: Option<FullJid>,
  pub(crate) nick: Option<String>,
  /// The reason of a kick or a ban.
  pub(crate) reason: Option<String>,
  pub(crate) destroy: Option<Destroy>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Destroy {
  pub(crate) reason: Option<String>,
}

impl MucUser {
  /// Parsed by hand: `xmpp-parsers` rejects the `<actor/>` of a kick and the status codes it does not know.
  pub(crate) fn parse(presence: &Element) -> Option<MucUser> {
    let x = presence.get_child("x", MUC_USER)?;
    let item = x.get_child("item", MUC_USER);
    Some(MucUser {
      statuses: x
        .children()
        .filter(|child| child.is("status", MUC_USER))
        .filter_map(|status| status.attr("code")?.parse().ok())
        .collect(),
      jid: item.and_then(|item| item.attr("jid")?.parse().ok()),
      nick: item.and_then(|item| item.attr("nick")).map(ToOwned::to_owned),
      reason: item
        .and_then(|item| item.get_child("reason", MUC_USER))
        .map(Element::text)
        .filter(|reason| !reason.is_empty()),
      destroy: x.get_child("destroy", MUC_USER).map(|destroy| Destroy {
        reason: destroy
          .get_child("reason", MUC_USER)
          .map(Element::text)
          .filter(|reason| !reason.is_empty()),
      }),
    })
  }

  pub(crate) fn is_self(&self) -> bool {
    self.statuses.contains(&110)
  }

  /// Goes by the status codes of an `unavailable` presence about ourselves.
  pub(crate) fn left_reason(&self) -> ConferenceLeftReason {
    if let Some(destroy) = &self.destroy {
      return ConferenceLeftReason::RoomDestroyed {
        reason: destroy.reason.clone(),
      };
    }
    // 301 banned, 307 kicked, 321 lost the affiliation, 322 the room became members-only.
    if self.statuses.iter().any(|code| matches!(code, 301 | 307 | 321 | 322)) {
      return ConferenceLeftReason::Kicked {
        reason: self.reason.clone(),
      };
    }
    // 332: the service shuts down.
    if self.statuses.contains(&332) {
      return ConferenceLeftReason::RoomDestroyed {
        reason: self.reason.clone(),
      };
    }
    ConferenceLeftReason::Left
  }
}

/// The nickname in the `<nick xmlns='http://jabber.org/protocol/nick'/>` of a presence.
pub(crate) fn presence_nick(presence: &Element) -> Option<String> {
  presence
    .get_child("nick", ns::NICK)
    .map(Element::text)
    .filter(|nick| !nick.is_empty())
}

/// Why the room refused a join: the mapping of lib-jitsi-meet's `ChatRoom.onPresError`. `presence` is a presence of
/// type `error`.
pub(crate) fn join_error(presence: &Element) -> JoinError {
  let Some(error) = presence.children().find(|child| child.name() == "error") else {
    return JoinError::RoomJoinError("no error element".to_owned());
  };
  let type_ = error.attr("type").unwrap_or_default();
  let has = |name: &str, namespace: &str| error.has_child(name, namespace);
  let text = error
    .get_child("text", STANZAS)
    .map(Element::text)
    .filter(|text| !text.is_empty());

  match type_ {
    "auth" if has("not-authorized", STANZAS) => JoinError::PasswordRequired,
    "cancel" if has("not-allowed", STANZAS) => {
      if text.as_deref() == Some("Room creation is restricted") || has("room-does-not-exist", JITSI_JITMEET) {
        JoinError::RoomCreationRestricted
      }
      else if has("authentication-required", JITSI_JITMEET) {
        JoinError::AuthenticationRequired
      }
      else if let Some(visitors) = ["no-main-participants", "promotion-not-allowed", "no-visitors-lobby"]
        .into_iter()
        .find(|name| has(name, JITSI_VISITORS))
      {
        JoinError::NotAllowed(visitors.to_owned())
      }
      else {
        JoinError::NotAllowed(text.unwrap_or_else(|| "not-allowed".to_owned()))
      }
    },
    "cancel" if has("resource-constraint", STANZAS) => JoinError::TimeLimitReached,
    _ if has("service-unavailable", STANZAS) => JoinError::RoomFull,
    "auth" if has("registration-required", STANZAS) => JoinError::MembersOnly {
      lobby_room: error
        .children()
        .find(|child| child.name() == "lobbyroom")
        .map(Element::text)
        .filter(|lobby| !lobby.is_empty()),
      waiting_for_host: error.children().any(|child| child.name() == "waiting-for-host"),
    },
    "modify" if has("displayname-required", JITSI_JITMEET) => JoinError::DisplayNameRequired,
    _ if has("conflict", STANZAS) => JoinError::NicknameConflict,
    _ if has("forbidden", STANZAS) => JoinError::Banned,
    _ => JoinError::RoomJoinError(error_condition(error).unwrap_or_else(|| "unknown".to_owned())),
  }
}

pub(crate) fn error_condition(error: &Element) -> Option<String> {
  error
    .children()
    .find(|child| child.ns() == STANZAS && child.name() != "text")
    .map(|condition| condition.name().to_owned())
}

#[cfg(test)]
mod tests {
  use super::*;

  fn presence(error: &str) -> Element {
    format!(
      "<presence xmlns='jabber:client' type='error' from='room@conference.meet.example/me'>\
         <x xmlns='http://jabber.org/protocol/muc'/>{error}</presence>"
    )
    .parse()
    .unwrap()
  }

  #[test]
  fn maps_the_errors_of_a_refused_join() {
    let cases = [
      (
        "<error type='auth'><not-authorized xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error>",
        JoinError::PasswordRequired,
      ),
      (
        "<error type='cancel'><not-allowed xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
           <text xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'>Room creation is restricted</text></error>",
        JoinError::RoomCreationRestricted,
      ),
      (
        "<error type='cancel'><not-allowed xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
           <room-does-not-exist xmlns='http://jitsi.org/jitmeet'/></error>",
        JoinError::RoomCreationRestricted,
      ),
      (
        "<error type='cancel'><not-allowed xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
           <authentication-required xmlns='http://jitsi.org/jitmeet'/></error>",
        JoinError::AuthenticationRequired,
      ),
      (
        "<error type='cancel'><not-allowed xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
           <no-main-participants xmlns='jitsi:visitors'/></error>",
        JoinError::NotAllowed("no-main-participants".to_owned()),
      ),
      (
        "<error type='cancel'><not-allowed xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
           <text xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'>No guests today</text></error>",
        JoinError::NotAllowed("No guests today".to_owned()),
      ),
      (
        "<error type='cancel'><resource-constraint xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error>",
        JoinError::TimeLimitReached,
      ),
      (
        "<error type='wait'><service-unavailable xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error>",
        JoinError::RoomFull,
      ),
      (
        "<error type='auth'><registration-required xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
           <lobbyroom xmlns='jitsi'>lobby@lobby.meet.example</lobbyroom></error>",
        JoinError::MembersOnly {
          lobby_room: Some("lobby@lobby.meet.example".to_owned()),
          waiting_for_host: false,
        },
      ),
      (
        "<error type='auth'><registration-required xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
           <waiting-for-host xmlns='jitsi'/></error>",
        JoinError::MembersOnly {
          lobby_room: None,
          waiting_for_host: true,
        },
      ),
      (
        "<error type='modify'><displayname-required xmlns='http://jitsi.org/jitmeet' lobby='true'/></error>",
        JoinError::DisplayNameRequired,
      ),
      (
        "<error type='cancel'><conflict xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error>",
        JoinError::NicknameConflict,
      ),
      (
        "<error type='auth'><forbidden xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error>",
        JoinError::Banned,
      ),
      (
        "<error type='cancel'><item-not-found xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error>",
        JoinError::RoomJoinError("item-not-found".to_owned()),
      ),
    ];
    for (error, expected) in cases {
      assert_eq!(join_error(&presence(error)), expected, "{error}");
    }
  }

  #[test]
  fn parses_the_user_element() {
    let presence: Element = "<presence xmlns='jabber:client' type='unavailable' from='room@muc/me'>\
        <x xmlns='http://jabber.org/protocol/muc#user'>\
          <item affiliation='none' role='none' jid='me@meet.example/res'>\
            <actor nick='moderator'/><reason>Spamming</reason>\
          </item>\
          <status code='110'/><status code='307'/>\
        </x></presence>"
      .parse()
      .unwrap();
    let user = MucUser::parse(&presence).unwrap();
    assert!(user.is_self());
    assert_eq!(user.jid.as_ref().unwrap().to_string(), "me@meet.example/res");
    assert_eq!(
      user.left_reason(),
      ConferenceLeftReason::Kicked {
        reason: Some("Spamming".to_owned())
      }
    );
    assert!(MucUser::parse(&"<presence xmlns='jabber:client'/>".parse().unwrap()).is_none());
  }

  #[test]
  fn tells_why_we_left() {
    let user = |statuses: &[u16], destroy: Option<&str>| MucUser {
      statuses: statuses.to_vec(),
      destroy: destroy.map(|reason| Destroy {
        reason: Some(reason.to_owned()),
      }),
      ..Default::default()
    };
    assert_eq!(
      user(&[110, 301], None).left_reason(),
      ConferenceLeftReason::Kicked { reason: None }
    );
    assert_eq!(
      user(&[110, 332], None).left_reason(),
      ConferenceLeftReason::RoomDestroyed { reason: None }
    );
    assert_eq!(
      user(&[110], Some("Bye")).left_reason(),
      ConferenceLeftReason::RoomDestroyed {
        reason: Some("Bye".to_owned())
      }
    );
    assert_eq!(user(&[110], None).left_reason(), ConferenceLeftReason::Left);
  }

  #[test]
  fn reads_the_nick() {
    let presence: Element = "<presence xmlns='jabber:client'><nick xmlns='http://jabber.org/protocol/nick'>Alice</nick></presence>"
      .parse()
      .unwrap();
    assert_eq!(presence_nick(&presence).as_deref(), Some("Alice"));
  }
}
