use std::time::Duration;

use minidom::Element;
use xmpp_parsers::ns;

use crate::{
  error::JoinError,
  muc::error_condition,
  xmpp::{iq::IqResponse, ns::JITSI_FOCUS},
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Answer {
  /// The conference exists: join the MUC.
  Ready,
  /// Not yet: ask again after a delay.
  NotReady,
  /// The focus has a problem that may pass: ask again after a delay, a few times.
  Retry(JoinError),
  /// No point in asking again.
  Refused(JoinError),
}

/// A retry after `Answer::Retry` is the last one when the delay would have reached this.
pub(crate) const MAX_ERROR_RETRY_DELAY: Duration = Duration::from_secs(60);

pub(crate) fn interpret(response: &IqResponse) -> Answer {
  match response {
    IqResponse::Result(conference) => {
      let ready = conference
        .as_ref()
        .is_some_and(|conference| conference.is("conference", JITSI_FOCUS) && conference.attr("ready") == Some("true"));
      if ready {
        Answer::Ready
      }
      else {
        Answer::NotReady
      }
    },
    IqResponse::Error(error) => interpret_error(error),
  }
}

fn interpret_error(error: &Element) -> Answer {
  let text = error
    .get_child("text", ns::XMPP_STANZAS)
    .map(Element::text)
    .filter(|text| !text.is_empty());
  let condition = error_condition(error).unwrap_or_else(|| "undefined-condition".to_owned());
  let has = |name: &str| error.children().any(|child| child.name() == name);

  if let Some(reservation) = error.children().find(|child| child.name() == "reservation-error") {
    let code = reservation.attr("error-code");
    return Answer::Refused(JoinError::FocusError {
      condition: "reservation-error".to_owned(),
      text: match (code, text) {
        (Some(code), Some(text)) => Some(format!("error code {code}: {text}")),
        (Some(code), None) => Some(format!("error code {code}")),
        (None, text) => text,
      },
    });
  }
  // The room has used up its time. Jicofo may name the condition or only mention it.
  if has("resource-constraint") || text.as_deref().is_some_and(|text| text.contains("resource-constraint")) {
    return Answer::Refused(JoinError::TimeLimitReached);
  }
  if has("not-authorized") {
    return Answer::Refused(JoinError::AuthenticationRequired);
  }
  if has("forbidden") || has("not-allowed") {
    return Answer::Refused(JoinError::NotAuthorized(text));
  }
  let failure = JoinError::FocusError { condition, text };
  if has("session-invalid") || has("not-acceptable") {
    return Answer::Retry(failure);
  }
  Answer::Refused(failure)
}

#[cfg(test)]
mod tests {
  use super::*;

  fn answer(xml: &str) -> Answer {
    let iq: Element = xml.parse().unwrap();
    interpret(&IqResponse::parse(&iq).unwrap().2)
  }

  fn error(body: &str) -> Answer {
    answer(&format!(
      "<iq xmlns='jabber:client' type='error' id='1' from='focus@auth.meet.example/focus'><error type='cancel'>{body}</error></iq>"
    ))
  }

  fn focus_error(condition: &str, text: Option<&str>) -> JoinError {
    JoinError::FocusError {
      condition: condition.to_owned(),
      text: text.map(ToOwned::to_owned),
    }
  }

  #[test]
  fn ready_is_the_attribute_of_the_conference_element() {
    let result = |attributes: &str| {
      answer(&format!(
        "<iq xmlns='jabber:client' type='result' id='1'><conference xmlns='http://jitsi.org/protocol/focus' {attributes}/></iq>"
      ))
    };
    assert_eq!(result("ready='true'"), Answer::Ready);
    assert_eq!(result("ready='false'"), Answer::NotReady);
    assert_eq!(result(""), Answer::NotReady);
    assert_eq!(
      answer("<iq xmlns='jabber:client' type='result' id='1'/>"),
      Answer::NotReady
    );
  }

  #[test]
  fn authentication_is_not_retried() {
    assert_eq!(
      error("<not-authorized xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>"),
      Answer::Refused(JoinError::AuthenticationRequired)
    );
    assert_eq!(
      error(
        "<forbidden xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
         <text xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'>token expired</text>"
      ),
      Answer::Refused(JoinError::NotAuthorized(Some("token expired".to_owned())))
    );
  }

  #[test]
  fn an_invalid_session_is_retried() {
    assert_eq!(
      error("<session-invalid xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>"),
      Answer::Retry(focus_error("session-invalid", None))
    );
    assert!(matches!(
      error("<not-acceptable xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>"),
      Answer::Retry(JoinError::FocusError { condition, .. }) if condition == "not-acceptable"
    ));
  }

  #[test]
  fn reservations_and_the_time_limit_are_final() {
    assert_eq!(
      error(
        "<undefined-condition xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
         <reservation-error xmlns='http://jitsi.org/protocol/focus' error-code='-2'/>\
         <text xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'>Conflict</text>"
      ),
      Answer::Refused(focus_error("reservation-error", Some("error code -2: Conflict")))
    );
    assert_eq!(
      error("<resource-constraint xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>"),
      Answer::Refused(JoinError::TimeLimitReached)
    );
    assert_eq!(
      error(
        "<internal-server-error xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
         <text xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'>resource-constraint: time limit</text>"
      ),
      Answer::Refused(JoinError::TimeLimitReached)
    );
  }

  #[test]
  fn other_errors_are_final_and_keep_their_condition_and_text() {
    assert_eq!(
      error(
        "<internal-server-error xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/>\
         <text xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'>Failed to start the conference.</text>"
      ),
      Answer::Refused(focus_error(
        "internal-server-error",
        Some("Failed to start the conference.")
      ))
    );
  }
}
