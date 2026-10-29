use minidom::Element;
use xmpp_parsers::{
  jingle_rtp::PayloadType,
  jingle_rtp_hdrext::RtpHdrext,
  ns::{JINGLE_RTP, JINGLE_RTP_HDREXT, JINGLE_SSMA},
};

use crate::jingle_ssma::{Group, Source};

#[derive(Debug, Clone, PartialEq)]
pub struct RtcpMux;

impl TryFrom<Element> for RtcpMux {
  type Error = xmpp_parsers::Error;

  fn try_from(root: Element) -> Result<RtcpMux, xmpp_parsers::Error> {
    check_self!(root, "rtcp-mux", JINGLE_RTP, "RtcpMux");
    Ok(RtcpMux)
  }
}

impl From<RtcpMux> for Element {
  fn from(_: RtcpMux) -> Element {
    Element::builder("rtcp-mux", JINGLE_RTP).build()
  }
}

generate_element!(
  /// Wrapper element describing an RTP session.
  Description, "description", JINGLE_RTP,
  attributes: [
      /// Namespace of the encryption scheme used.
      media: Required<String> = "media",

      /// User-friendly name for the encryption scheme, should be `None` for OTR,
      /// legacy OpenPGP and OX.
      // XXX: is this a String or an u32?!  Refer to RFC 3550.
      ssrc: Option<String> = "ssrc",
  ],
  children: [
      /// List of encodings that can be used for this RTP stream.
      payload_types: Vec<PayloadType> = ("payload-type", JINGLE_RTP) => PayloadType,

      /// Specifies the ability to multiplex RTP Data and Control Packets on a single port as
      /// described in RFC 5761.
      rtcp_mux: Option<RtcpMux> = ("rtcp-mux", JINGLE_RTP) => RtcpMux,

      /// List of ssrc-group.
      ssrc_groups: Vec<Group> = ("ssrc-group", JINGLE_SSMA) => Group,

      /// List of ssrc.
      ssrcs: Vec<Source> = ("source", JINGLE_SSMA) => Source,

      /// List of header extensions.
      hdrexts: Vec<RtpHdrext> = ("rtp-hdrext", JINGLE_RTP_HDREXT) => RtpHdrext

      // TODO: Add support for <encryption/> and <bandwidth/>.
  ]
);

impl Description {
  /// Create a new RTP description.
  pub fn new(media: String) -> Description {
    Description {
      media,
      ssrc: None,
      payload_types: Vec::new(),
      rtcp_mux: None,
      ssrc_groups: Vec::new(),
      ssrcs: Vec::new(),
      hdrexts: Vec::new(),
    }
  }
}
