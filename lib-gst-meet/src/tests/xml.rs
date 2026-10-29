use minidom::Element;

pub(crate) const NS_FOCUS: &str = "http://jitsi.org/protocol/focus";
pub(crate) const ROOM: &str = "room@conference.meet.example";
pub(crate) const FOCUS: &str = "room@conference.meet.example/focus";
pub(crate) const FOCUS_ACCOUNT: &str = "focus@auth.meet.example/focus";
pub(crate) const MY_JID: &str = "abcd1234-0000-1111-2222-333333333333@meet.example/res";
pub(crate) const ME: &str = "abcd1234";

pub(crate) fn parse(xml: &str) -> Element {
  xml.parse().unwrap_or_else(|e| panic!("bad test XML ({e}): {xml}"))
}

pub(crate) fn fingerprint() -> String {
  vec!["AB"; 32].join(":")
}

fn candidate(id: &str, port: u16) -> String {
  format!(
    "<candidate xmlns='urn:xmpp:jingle:transports:ice-udp:1' component='1' foundation='1' generation='0' id='{id}' \
     ip='10.0.0.1' port='{port}' priority='2130706431' protocol='udp' type='host'/>"
  )
}

fn transport(ufrag: &str, web_socket: bool) -> String {
  format!(
    "<transport xmlns='urn:xmpp:jingle:transports:ice-udp:1' ufrag='{ufrag}' pwd='0123456789012345678901'>\
       <fingerprint xmlns='urn:xmpp:jingle:apps:dtls:0' hash='sha-256' setup='actpass'>{}</fingerprint>{}{}\
     </transport>",
    fingerprint(),
    candidate("c1", 10000),
    if web_socket {
      "<web-socket xmlns='http://jitsi.org/protocol/colibri' url='wss://jvb.example/colibri-ws/jvb1/abc'/>"
    }
    else {
      ""
    },
  )
}

fn source(owner: &str, ssrc: u32, name: &str, video_type: Option<&str>) -> String {
  format!(
    "<source xmlns='urn:xmpp:jingle:apps:rtp:ssma:0' ssrc='{ssrc}' name='{name}'{}>\
       <ssrc-info xmlns='http://jitsi.org/jitmeet' owner='{owner}'/></source>",
    video_type.map(|t| format!(" videoType='{t}'")).unwrap_or_default()
  )
}

/// `(owner endpoint, ssrc)`; every video source also has an RTX SSRC of `ssrc + 1`.
pub(crate) type Participant<'a> = (&'a str, u32);

fn audio_sources(sources: &[Participant]) -> String {
  sources
    .iter()
    .map(|(who, ssrc)| source(&format!("{ROOM}/{who}"), *ssrc, &format!("{who}-a0"), None))
    .collect()
}

fn video_sources(sources: &[Participant]) -> String {
  sources
    .iter()
    .map(|(who, ssrc)| {
      let owner = format!("{ROOM}/{who}");
      let name = format!("{who}-v0");
      format!(
        "{}{}<ssrc-group xmlns='urn:xmpp:jingle:apps:rtp:ssma:0' semantics='FID'>\
           <source xmlns='urn:xmpp:jingle:apps:rtp:ssma:0' ssrc='{ssrc}'/>\
           <source xmlns='urn:xmpp:jingle:apps:rtp:ssma:0' ssrc='{}'/></ssrc-group>",
        source(&owner, *ssrc, &name, Some("camera")),
        source(&owner, ssrc + 1, &name, Some("camera")),
        ssrc + 1
      )
    })
    .collect()
}

fn from_focus(id: &str, jingle: &str) -> String {
  format!(
    "<iq xmlns='jabber:client' type='set' id='{id}' from='{FOCUS}' to='{MY_JID}'>{jingle}</iq>"
  )
}

pub(crate) struct Offer<'a> {
  pub(crate) sid: &'a str,
  pub(crate) bridge_session_id: Option<&'a str>,
  pub(crate) audio: &'a [Participant<'a>],
  pub(crate) video: &'a [Participant<'a>],
  pub(crate) web_socket: bool,
}

impl Default for Offer<'_> {
  fn default() -> Self {
    Self {
      sid: "sid-1",
      bridge_session_id: Some("bridge-1"),
      audio: &[("bob", 1000)],
      video: &[("bob", 2000)],
      web_socket: false,
    }
  }
}

/// The contents of a session-initiate or a transport-replace.
fn offer_contents(offer: &Offer) -> String {
  format!(
    "<group xmlns='urn:xmpp:jingle:apps:grouping:0' semantics='BUNDLE'><content name='audio'/><content name='video'/></group>\
     <content creator='initiator' name='audio' senders='both'>\
       <description xmlns='urn:xmpp:jingle:apps:rtp:1' media='audio'>\
         <payload-type id='111' name='opus' clockrate='48000' channels='2'/>\
         <rtp-hdrext xmlns='urn:xmpp:jingle:apps:rtp:rtp-hdrext:0' id='1' uri='urn:ietf:params:rtp-hdrext:ssrc-audio-level'/>\
         <rtp-hdrext xmlns='urn:xmpp:jingle:apps:rtp:rtp-hdrext:0' id='5' uri='http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01'/>\
         <rtcp-mux/>{audio}\
       </description>{transport}\
     </content>\
     <content creator='initiator' name='video' senders='both'>\
       <description xmlns='urn:xmpp:jingle:apps:rtp:1' media='video'>\
         <payload-type id='100' name='VP8' clockrate='90000'/>\
         <payload-type id='96' name='rtx' clockrate='90000'><parameter name='apt' value='100'/></payload-type>\
         <rtp-hdrext xmlns='urn:xmpp:jingle:apps:rtp:rtp-hdrext:0' id='5' uri='http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01'/>\
         <rtcp-mux/>{video}\
       </description>{transport}\
     </content>{bridge}",
    audio = audio_sources(offer.audio),
    video = video_sources(offer.video),
    transport = transport("focusufrag", offer.web_socket),
    bridge = offer
      .bridge_session_id
      .map(|id| format!("<bridge-session xmlns='http://jitsi.org/protocol/focus' id='{id}' region='eu'/>"))
      .unwrap_or_default(),
  )
}

pub(crate) fn session_initiate(iq_id: &str, offer: &Offer) -> String {
  from_focus(
    iq_id,
    &format!(
      "<jingle xmlns='urn:xmpp:jingle:1' action='session-initiate' initiator='{FOCUS_ACCOUNT}' sid='{}'>{}</jingle>",
      offer.sid,
      offer_contents(offer)
    ),
  )
}

pub(crate) fn transport_replace(iq_id: &str, offer: &Offer) -> String {
  from_focus(
    iq_id,
    &format!(
      "<jingle xmlns='urn:xmpp:jingle:1' action='transport-replace' initiator='{FOCUS_ACCOUNT}' sid='{}'>{}</jingle>",
      offer.sid,
      offer_contents(offer)
    ),
  )
}

/// A source-add or source-remove with the sources of one participant.
pub(crate) fn source_update(iq_id: &str, action: &str, sid: &str, who: &str, audio: u32, video: Option<u32>) -> String {
  let video_content = video.map_or_else(String::new, |ssrc| {
    format!(
      "<content creator='initiator' name='video'><description xmlns='urn:xmpp:jingle:apps:rtp:1' media='video'>{}</description></content>",
      video_sources(&[(who, ssrc)])
    )
  });
  from_focus(
    iq_id,
    &format!(
      "<jingle xmlns='urn:xmpp:jingle:1' action='{action}' sid='{sid}'>\
         <content creator='initiator' name='audio'><description xmlns='urn:xmpp:jingle:apps:rtp:1' media='audio'>{}</description></content>\
         {video_content}\
       </jingle>",
      audio_sources(&[(who, audio)])
    ),
  )
}

pub(crate) fn jingle(iq_id: &str, from: &str, action: &str, sid: &str, children: &str) -> String {
  format!(
    "<iq xmlns='jabber:client' type='set' id='{iq_id}' from='{from}' to='{MY_JID}'>\
       <jingle xmlns='urn:xmpp:jingle:1' action='{action}' sid='{sid}'>{children}</jingle></iq>"
  )
}

pub(crate) fn occupant_presence(who: &str) -> String {
  format!(
    "<presence xmlns='jabber:client' from='{ROOM}/{who}' to='{MY_JID}'>\
       <x xmlns='http://jabber.org/protocol/muc#user'><item affiliation='none' role='participant'/></x>\
       <nick xmlns='http://jabber.org/protocol/nick'>{who} nick</nick>\
     </presence>"
  )
}

pub(crate) fn self_presence() -> String {
  format!(
    "<presence xmlns='jabber:client' from='{ROOM}/{ME}' to='{MY_JID}'>\
       <x xmlns='http://jabber.org/protocol/muc#user'>\
         <item affiliation='none' role='participant' jid='{MY_JID}'/><status code='110'/>\
       </x></presence>"
  )
}

pub(crate) fn conference_ready(request_id: &str, ready: bool) -> String {
  format!(
    "<iq xmlns='jabber:client' type='result' id='{request_id}' from='focus@auth.meet.example/focus' to='{MY_JID}'>\
       <conference xmlns='http://jitsi.org/protocol/focus' ready='{ready}'/></iq>"
  )
}
