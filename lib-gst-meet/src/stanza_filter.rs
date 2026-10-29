use anyhow::Result;
use async_trait::async_trait;
use minidom::Element;

/// Handles a share of the stanzas a [`Connection`](crate::Connection) receives.
#[async_trait]
pub trait StanzaFilter {
  /// Whether this filter wants `element`. The first filter that does gets it.
  fn filter(&self, element: &Element) -> bool;

  /// An error is logged; it does not end the connection.
  async fn take(&self, element: Element) -> Result<()>;

  /// The connection has ended and no stanza will arrive any more. Called once, from the read loop, so it must not
  /// block.
  fn connection_closed(&self) {}
}

/// Identifies a filter added to a [`Connection`](crate::Connection), for
/// [`remove_stanza_filter`](crate::Connection::remove_stanza_filter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StanzaFilterId(pub(crate) u64);
