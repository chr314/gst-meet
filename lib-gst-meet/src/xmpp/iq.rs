use std::fmt;

use jid::{FullJid, Jid};
use minidom::Element;
use xmpp_parsers::{
  iq::Iq,
  ns,
  stanza_error::{DefinedCondition, ErrorType, StanzaError},
};

#[derive(Debug, Clone)]
pub enum IqResponse {
  Result(Option<Element>),
  Error(Element),
}

impl IqResponse {
  /// The id, the sender and the content of an `<iq type='result'/>` or `<iq type='error'/>`.
  pub(crate) fn parse(element: &Element) -> Option<(String, Option<Jid>, IqResponse)> {
    if !element.is("iq", ns::DEFAULT_NS) {
      return None;
    }
    let id = element.attr("id")?.to_owned();
    let from = element.attr("from").and_then(|from| from.parse().ok());
    let response = match element.attr("type")? {
      "result" => IqResponse::Result(element.children().next().cloned()),
      "error" => IqResponse::Error(
        element
          .get_child("error", ns::DEFAULT_NS)
          .cloned()
          .unwrap_or_else(|| Element::builder("error", ns::DEFAULT_NS).build()),
      ),
      _ => return None,
    };
    Some((id, from, response))
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestError {
  ConnectionClosed,
  Timeout,
}

impl fmt::Display for RequestError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      RequestError::ConnectionClosed => f.write_str("the XMPP connection is closed"),
      RequestError::Timeout => f.write_str("the request timed out"),
    }
  }
}

impl std::error::Error for RequestError {}

/// The `type` and `id` of an `<iq type='get'/>` or `<iq type='set'/>` that expects an answer, and its sender.
pub(crate) fn request_header(element: &Element) -> Option<(&str, String, Jid)> {
  if !element.is("iq", ns::DEFAULT_NS) {
    return None;
  }
  let type_ = element.attr("type").filter(|type_| matches!(*type_, "get" | "set"))?;
  Some((type_, element.attr("id")?.to_owned(), element.attr("from")?.parse().ok()?))
}

pub(crate) fn result_iq(to: Jid, id: &str, from: &FullJid) -> Element {
  Iq::empty_result(to, id).with_from(from.clone().into()).into()
}

pub(crate) fn error_iq(to: Jid, id: &str, from: &FullJid, error: StanzaError) -> Element {
  Iq::from_error(id, error)
    .with_to(to)
    .with_from(from.clone().into())
    .into()
}

pub(crate) fn stanza_error(type_: ErrorType, condition: DefinedCondition, text: impl Into<String>) -> StanzaError {
  StanzaError::new(type_, condition, "en", text)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parses_results_and_errors() {
    let result: Element = "<iq xmlns='jabber:client' type='result' id='1' from='a@b/c'><conference xmlns='x' ready='true'/></iq>"
      .parse()
      .unwrap();
    let (id, from, response) = IqResponse::parse(&result).unwrap();
    assert_eq!((id.as_str(), from.unwrap().to_string()), ("1", "a@b/c".to_owned()));
    assert!(matches!(response, IqResponse::Result(Some(payload)) if payload.name() == "conference"));

    let error: Element = "<iq xmlns='jabber:client' type='error' id='2'><error type='cancel'><not-authorized xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error></iq>"
      .parse()
      .unwrap();
    let (_, from, response) = IqResponse::parse(&error).unwrap();
    assert!(from.is_none());
    assert!(matches!(response, IqResponse::Error(error) if error.has_child("not-authorized", ns::XMPP_STANZAS)));

    let request: Element = "<iq xmlns='jabber:client' type='get' id='3'><ping xmlns='urn:xmpp:ping'/></iq>"
      .parse()
      .unwrap();
    assert!(IqResponse::parse(&request).is_none());
  }

  #[test]
  fn requests_need_an_id_and_a_sender() {
    let request: Element = "<iq xmlns='jabber:client' type='set' id='3' from='room@muc/focus'/>"
      .parse()
      .unwrap();
    let (type_, id, from) = request_header(&request).unwrap();
    assert_eq!((type_, id.as_str(), from.to_string().as_str()), ("set", "3", "room@muc/focus"));
    let anonymous: Element = "<iq xmlns='jabber:client' type='set' id='3'/>".parse().unwrap();
    assert!(request_header(&anonymous).is_none());
  }
}
