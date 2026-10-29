use std::convert::TryFrom;

use anyhow::{bail, Context, Result};
use minidom::Element;
use xmpp_parsers::iq::IqGetPayload;

use crate::xmpp::ns;

#[derive(Debug)]
pub(crate) struct ServicesQuery {}

impl TryFrom<Element> for ServicesQuery {
  type Error = anyhow::Error;

  fn try_from(_elem: Element) -> Result<ServicesQuery> {
    anyhow::bail!("ServicesQuery deserialization is not supported")
  }
}

impl From<ServicesQuery> for Element {
  fn from(_services: ServicesQuery) -> Element {
    let builder = Element::builder("services", ns::EXTDISCO);
    builder.build()
  }
}

impl IqGetPayload for ServicesQuery {}

#[derive(Debug, Clone)]
pub struct Service {
  pub(crate) r#type: String,
  pub(crate) host: String,
  pub(crate) port: Option<u16>,
  pub(crate) transport: Option<String>,
  pub(crate) username: Option<String>,
  pub(crate) password: Option<String>,
}

#[derive(Debug)]
pub(crate) struct ServicesResult {
  pub(crate) services: Vec<Service>,
}

impl TryFrom<Element> for ServicesResult {
  type Error = anyhow::Error;

  fn try_from(elem: Element) -> Result<ServicesResult> {
    if !elem.is("services", ns::EXTDISCO) {
      bail!("not a services element");
    }
    Ok(ServicesResult {
      services: elem
        .children()
        .map(|child| {
          Ok(Service {
            r#type: child.attr("type").context("missing type attr")?.to_owned(),
            host: child.attr("host").context("missing host attr")?.to_owned(),
            port: child.attr("port").map(|p| p.parse()).transpose()?,
            transport: child.attr("transport").map(ToOwned::to_owned),
            username: child.attr("username").map(ToOwned::to_owned),
            password: child.attr("password").map(ToOwned::to_owned),
          })
        })
        .collect::<Result<_>>()?,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parses_the_services_of_a_deployment() {
    let elem: Element = "<services xmlns='urn:xmpp:extdisco:2'>\
        <service type='stun' host='meet-jit-si-turnrelay.jitsi.net' port='443' restricted='false'/>\
        <service type='turns' host='turn.example' port='443' transport='tcp' username='u' password='p' restricted='true' expires='2026-01-01T00:00:00Z'/>\
      </services>"
      .parse()
      .unwrap();
    let services = ServicesResult::try_from(elem).unwrap().services;
    assert_eq!(services.len(), 2);
    assert_eq!(services[0].r#type, "stun");
    assert_eq!(services[0].port, Some(443));
    assert_eq!(services[1].transport.as_deref(), Some("tcp"));
    assert_eq!(services[1].username.as_deref(), Some("u"));
  }

  #[test]
  fn rejects_a_service_without_a_host() {
    let elem: Element = "<services xmlns='urn:xmpp:extdisco:2'><service type='stun'/></services>"
      .parse()
      .unwrap();
    assert!(ServicesResult::try_from(elem).is_err());
  }
}
