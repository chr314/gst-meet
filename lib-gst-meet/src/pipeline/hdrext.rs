//! RTP header extension normalization (RFC 8285).
//!
//! Chrome switches a whole packet to the two-byte header extension form as soon as one element does not fit the
//! one-byte form (for example the AV1 dependency descriptor with its template structure on keyframes). rtpsession's
//! TWCC parsing only understands the one-byte form, so the transport-cc sequence number in these packets would be
//! lost and the feedback would report them as missing.

use gstreamer as gst;
use gstreamer::prelude::PadExtManual as _;
use tracing::warn;

const RTP_FIXED_HEADER_LEN: usize = 12;
const EXTENSION_PRESENT_BIT: u8 = 0x10;

const ONE_BYTE_PROFILE: u16 = 0xBEDE;
const TWO_BYTE_PROFILE: u16 = 0x1000;
/// The low four bits of the two-byte profile are application bits.
const TWO_BYTE_PROFILE_MASK: u16 = 0xFFF0;

/// Rewrites an RTP packet with two-byte header extensions to the one-byte form.
///
/// Returns `None` if the packet has no extension, has a one-byte (or any other) extension profile, or is malformed,
/// in which case the packet should be used as it is.
///
/// Elements the one-byte form cannot carry are dropped: ids above 14 (15 is reserved), empty elements and elements
/// longer than 16 bytes. An element running past the end of the extension ends parsing, as in libwebrtc. The CSRC
/// list, the payload and any padding are kept as they are.
pub(crate) fn normalize_two_byte(packet: &[u8]) -> Option<Vec<u8>> {
  if packet.len() < RTP_FIXED_HEADER_LEN || packet[0] & EXTENSION_PRESENT_BIT == 0 {
    return None;
  }
  let csrc_count = usize::from(packet[0] & 0x0F);
  let extension_header = RTP_FIXED_HEADER_LEN + 4 * csrc_count;
  let extension_data = extension_header + 4;
  if packet.len() < extension_data {
    return None;
  }
  let profile = u16::from_be_bytes([packet[extension_header], packet[extension_header + 1]]);
  if profile & TWO_BYTE_PROFILE_MASK != TWO_BYTE_PROFILE {
    return None;
  }
  let extension_len = 4
    * usize::from(u16::from_be_bytes([
      packet[extension_header + 2],
      packet[extension_header + 3],
    ]));
  let extension_end = extension_data + extension_len;
  if packet.len() < extension_end {
    return None;
  }

  let extension = &packet[extension_data..extension_end];
  let mut elements = Vec::with_capacity(extension.len());
  let mut pos = 0;
  while pos < extension.len() {
    let id = extension[pos];
    // Padding is a single zero byte, without a length.
    if id == 0 {
      pos += 1;
      continue;
    }
    let Some(&len) = extension.get(pos + 1) else {
      break;
    };
    let len = usize::from(len);
    let data_start = pos + 2;
    let Some(data) = extension.get(data_start..data_start + len) else {
      break;
    };
    pos = data_start + len;
    if (1..=14).contains(&id) && (1..=16).contains(&len) {
      elements.push((id, data));
    }
  }

  let body_len: usize = elements.iter().map(|(_, data)| 1 + data.len()).sum();
  let padded_body_len = body_len.next_multiple_of(4);
  let mut out = Vec::with_capacity(extension_header + 4 + padded_body_len + packet.len() - extension_end);
  out.extend_from_slice(&packet[..extension_header]);
  out.extend_from_slice(&ONE_BYTE_PROFILE.to_be_bytes());
  // Bounded by the u16 length of the packet's own extension: the one-byte form is never longer.
  out.extend_from_slice(&((padded_body_len / 4) as u16).to_be_bytes());
  for (id, data) in elements {
    out.push(id << 4 | (data.len() - 1) as u8);
    out.extend_from_slice(data);
  }
  out.resize(extension_header + 4 + padded_body_len, 0);
  out.extend_from_slice(&packet[extension_end..]);
  Some(out)
}

/// Replaces buffers that need it with their one-byte form, keeping flags, timestamps and metas.
fn normalize_buffer(buffer: &gst::Buffer) -> Option<gst::Buffer> {
  let normalized = {
    let map = buffer.map_readable().ok()?;
    normalize_two_byte(map.as_slice())?
  };
  let mut out = gst::Buffer::from_mut_slice(normalized);
  let copied = buffer.copy_into(
    out.get_mut().expect("newly created buffer is writable"),
    gst::BufferCopyFlags::FLAGS | gst::BufferCopyFlags::TIMESTAMPS | gst::BufferCopyFlags::META,
    ..,
  );
  if let Err(e) = copied {
    warn!(
      "failed to copy buffer metadata while normalizing RTP header extensions: {}",
      e
    );
    return None;
  }
  Some(out)
}

pub(crate) fn install_normalizer_probe(pad: &gst::Pad) {
  pad.add_probe(
    gst::PadProbeType::BUFFER | gst::PadProbeType::BUFFER_LIST,
    |_pad, info| {
      match &mut info.data {
        Some(gst::PadProbeData::Buffer(buffer)) => {
          if let Some(normalized) = normalize_buffer(buffer) {
            *buffer = normalized;
          }
        },
        Some(gst::PadProbeData::BufferList(list)) => {
          let buffers: Vec<gst::Buffer> = list.iter_owned().collect();
          let replacements: Vec<Option<gst::Buffer>> = buffers.iter().map(normalize_buffer).collect();
          if replacements.iter().any(Option::is_some) {
            let mut out = gst::BufferList::new_sized(buffers.len());
            {
              let out = out.get_mut().expect("newly created buffer list is writable");
              for (buffer, replacement) in buffers.into_iter().zip(replacements) {
                out.add(replacement.unwrap_or(buffer));
              }
            }
            *list = out;
          }
        },
        _ => {},
      }
      gst::PadProbeReturn::Ok
    },
  );
}

#[cfg(test)]
mod tests {
  use gstreamer::prelude::*;
  use gstreamer_rtp::RTPBuffer;

  use super::*;

  const SSRC: [u8; 4] = [0x11, 0x22, 0x33, 0x44];

  /// V=2, optionally P and CC CSRCs, X set when `extension` is given. The extension header is `profile` followed by
  /// the length in words, computed from `extension`, which must be a multiple of four bytes.
  fn packet(padding: bool, csrcs: &[u32], extension: Option<(u16, &[u8])>, payload: &[u8], padding_len: u8) -> Vec<u8> {
    let mut p = vec![
      0x80 | (u8::from(padding) << 5) | (u8::from(extension.is_some()) << 4) | csrcs.len() as u8,
      0x60 | 0x80,
      0x12,
      0x34,
      0xAA,
      0xBB,
      0xCC,
      0xDD,
    ];
    p.extend_from_slice(&SSRC);
    for csrc in csrcs {
      p.extend_from_slice(&csrc.to_be_bytes());
    }
    if let Some((profile, data)) = extension {
      assert_eq!(data.len() % 4, 0, "test extension is not word aligned");
      p.extend_from_slice(&profile.to_be_bytes());
      p.extend_from_slice(&((data.len() / 4) as u16).to_be_bytes());
      p.extend_from_slice(data);
    }
    p.extend_from_slice(payload);
    if padding {
      p.extend(std::iter::repeat_n(0, usize::from(padding_len - 1)));
      p.push(padding_len);
    }
    p
  }

  fn extension_of(packet: &[u8]) -> (u16, Vec<u8>) {
    let ext = RTP_FIXED_HEADER_LEN + 4 * usize::from(packet[0] & 0x0F);
    let len = 4 * usize::from(u16::from_be_bytes([packet[ext + 2], packet[ext + 3]]));
    (
      u16::from_be_bytes([packet[ext], packet[ext + 1]]),
      packet[ext + 4..ext + 4 + len].to_vec(),
    )
  }

  const PAYLOAD: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF, 0x01];

  #[test]
  fn rewrites_transport_cc_next_to_a_long_element() {
    // id 5 (transport-cc, 2 bytes), id 15 (dependency descriptor, 20 bytes), id 1 (audio level, 1 byte).
    let mut ext = vec![5, 2, 0x01, 0x02, 15, 20];
    ext.extend((0..20).map(|i| 0xA0 + i));
    ext.extend([1, 1, 0x7F]);
    ext.resize(ext.len().next_multiple_of(4), 0);
    let input = packet(false, &[], Some((0x1000, &ext)), PAYLOAD, 0);

    let out = normalize_two_byte(&input).unwrap();

    let (profile, data) = extension_of(&out);
    assert_eq!(profile, 0xBEDE);
    // 0x51 = id 5, len-1 = 1; 0x10 = id 1, len-1 = 0; zero padded to a word.
    assert_eq!(data, vec![0x51, 0x01, 0x02, 0x10, 0x7F, 0, 0, 0]);
    assert_eq!(&out[..12], &input[..12]);
    assert!(out.ends_with(PAYLOAD));
    assert_eq!(out.len(), 12 + 4 + 8 + PAYLOAD.len());
  }

  #[test]
  fn output_is_parsed_by_the_gstreamer_one_byte_parser() {
    let mut ext = vec![5, 2, 0x01, 0x02, 15, 20];
    ext.extend((0..20).map(|i| 0xA0 + i));
    ext.extend([3, 3, 9, 8, 7]);
    ext.resize(ext.len().next_multiple_of(4), 0);
    let input = packet(false, &[0x01020304], Some((0x1000, &ext)), PAYLOAD, 0);
    gst::init().unwrap();

    let out = normalize_two_byte(&input).unwrap();

    let buffer = gst::Buffer::from_mut_slice(out);
    let rtp = RTPBuffer::from_buffer_readable(&buffer).unwrap();
    assert_eq!(rtp.extension_onebyte_header(5, 0), Some(&[0x01, 0x02][..]));
    assert_eq!(rtp.extension_onebyte_header(3, 0), Some(&[9, 8, 7][..]));
    assert_eq!(rtp.extension_onebyte_header(14, 0), None);
    assert_eq!(rtp.csrc_count(), 1);
    assert_eq!(rtp.payload().unwrap(), PAYLOAD);
    assert_eq!(rtp.ssrc(), u32::from_be_bytes(SSRC));
  }

  #[test]
  fn keeps_csrcs_and_padding() {
    let ext = [2, 1, 0xAB, 0];
    let input = packet(true, &[1, 2, 3], Some((0x1003, &ext)), PAYLOAD, 4);

    let out = normalize_two_byte(&input).unwrap();

    assert_eq!(out[0], input[0]);
    assert_eq!(&out[12..24], &input[12..24]);
    let (profile, data) = extension_of(&out);
    assert_eq!(profile, 0xBEDE);
    assert_eq!(data, vec![0x20, 0xAB, 0, 0]);
    assert_eq!(
      &out[out.len() - PAYLOAD.len() - 4..],
      &input[input.len() - PAYLOAD.len() - 4..]
    );
    assert_eq!(*out.last().unwrap(), 4);
  }

  #[test]
  fn accepts_every_application_bits_value() {
    let ext = [1, 1, 0x42, 0];
    for bits in 0..=0x0F {
      let input = packet(false, &[], Some((0x1000 | bits, &ext)), PAYLOAD, 0);
      assert!(normalize_two_byte(&input).is_some(), "profile 0x{:04x}", 0x1000 | bits);
    }
  }

  #[test]
  fn leaves_other_packets_alone() {
    let ext = [0x10, 0x42, 0, 0];
    assert!(normalize_two_byte(&packet(false, &[], None, PAYLOAD, 0)).is_none());
    assert!(normalize_two_byte(&packet(false, &[], Some((0xBEDE, &ext)), PAYLOAD, 0)).is_none());
    assert!(normalize_two_byte(&packet(false, &[], Some((0x1010, &ext)), PAYLOAD, 0)).is_none());
    assert!(normalize_two_byte(&packet(false, &[], Some((0x2000, &ext)), PAYLOAD, 0)).is_none());
    assert!(normalize_two_byte(&[]).is_none());
    assert!(normalize_two_byte(&[0x90; 11]).is_none());
  }

  #[test]
  fn rejects_truncated_packets() {
    let ext = [1, 1, 0x42, 0];
    let input = packet(false, &[2], Some((0x1000, &ext)), &[], 0);
    // Cut inside the CSRC list, the extension header and the extension data.
    for len in [12, 14, 16, 18, 19, 22, input.len() - 1] {
      assert!(normalize_two_byte(&input[..len]).is_none(), "len {len}");
    }
    assert!(normalize_two_byte(&input).is_some());
  }

  #[test]
  fn drops_what_the_one_byte_form_cannot_carry() {
    let mut ext = vec![
      1, 0, // empty element
      15, 1, 0xEE, // id 15 is reserved in the one-byte form
      16, 1, 0x99, // id above 14
      255, 1, 0xF0, // highest id
      14, 16, // longest allowed
    ];
    ext.extend(0..16);
    ext.extend([2, 17]); // too long
    ext.extend(0..17);
    ext.extend([4, 1, 0x44]);
    ext.resize(ext.len().next_multiple_of(4), 0);
    let input = packet(false, &[], Some((0x1000, &ext)), PAYLOAD, 0);

    let out = normalize_two_byte(&input).unwrap();

    let (_, data) = extension_of(&out);
    let mut expected = vec![0xEF];
    expected.extend(0..16);
    expected.extend([0x40, 0x44]);
    expected.resize(expected.len().next_multiple_of(4), 0);
    assert_eq!(data, expected);
  }

  #[test]
  fn skips_padding_between_elements() {
    let ext = [0, 0, 7, 2, 0xAA, 0xBB, 0, 9, 1, 0xCC, 0, 0];
    let input = packet(false, &[], Some((0x1000, &ext)), PAYLOAD, 0);

    let (_, data) = extension_of(&normalize_two_byte(&input).unwrap());

    assert_eq!(data, vec![0x71, 0xAA, 0xBB, 0x90, 0xCC, 0, 0, 0]);
  }

  #[test]
  fn keeps_an_empty_extension_when_nothing_fits() {
    let mut ext = vec![20, 30];
    ext.extend(std::iter::repeat_n(0x55, 30));
    ext.resize(ext.len().next_multiple_of(4), 0);
    let input = packet(false, &[], Some((0x1000, &ext)), PAYLOAD, 0);

    let out = normalize_two_byte(&input).unwrap();

    assert_eq!(out.len(), 12 + 4 + PAYLOAD.len());
    assert_eq!(extension_of(&out), (0xBEDE, vec![]));
    assert_eq!(out[0] & EXTENSION_PRESENT_BIT, EXTENSION_PRESENT_BIT);
  }

  #[test]
  fn stops_at_a_truncated_element() {
    // The second element claims 8 bytes but only 2 remain in the extension.
    let ext = [3, 1, 0x33, 6, 8, 1, 2, 0];
    let input = packet(false, &[], Some((0x1000, &ext)), PAYLOAD, 0);

    let (_, data) = extension_of(&normalize_two_byte(&input).unwrap());

    assert_eq!(data, vec![0x30, 0x33, 0, 0]);
  }

  #[test]
  fn handles_a_dangling_id_byte() {
    let ext = [3, 1, 0x33, 6];
    let input = packet(false, &[], Some((0x1000, &ext)), PAYLOAD, 0);

    let (_, data) = extension_of(&normalize_two_byte(&input).unwrap());

    assert_eq!(data, vec![0x30, 0x33, 0, 0]);
  }

  #[test]
  fn normalizing_is_idempotent() {
    let ext = [5, 2, 1, 2];
    let input = packet(false, &[], Some((0x1000, &ext)), PAYLOAD, 0);
    let once = normalize_two_byte(&input).unwrap();
    assert!(normalize_two_byte(&once).is_none());
  }

  fn rtp_buffer(ext: &[u8], pts: u64) -> gst::Buffer {
    let mut buffer = gst::Buffer::from_mut_slice(packet(false, &[], Some((0x1000, ext)), PAYLOAD, 0));
    {
      let buffer = buffer.get_mut().unwrap();
      buffer.set_pts(gst::ClockTime::from_nseconds(pts));
      buffer.set_flags(gst::BufferFlags::DISCONT | gst::BufferFlags::MARKER);
    }
    buffer
  }

  #[test]
  fn probe_rewrites_buffers_and_lists_keeping_metadata() {
    gst::init().unwrap();
    let two_byte = [5, 2, 0xAB, 0xCD];
    let one_byte = packet(false, &[], Some((0xBEDE, &[0x51, 0xAB, 0xCD, 0])), PAYLOAD, 0);

    let sink = gst::ElementFactory::make("fakesink")
      .property("sync", false)
      .build()
      .unwrap();
    let pad = sink.static_pad("sink").unwrap();
    install_normalizer_probe(&pad);
    // The probe on a pad of a fakesink sees the buffers before the sink; observe the result with a second probe.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<gst::Buffer>::new()));
    {
      let seen = seen.clone();
      pad.add_probe(
        gst::PadProbeType::BUFFER | gst::PadProbeType::BUFFER_LIST,
        move |_, info| {
          match &info.data {
            Some(gst::PadProbeData::Buffer(buffer)) => seen.lock().unwrap().push(buffer.clone()),
            Some(gst::PadProbeData::BufferList(list)) => seen.lock().unwrap().extend(list.iter_owned()),
            _ => {},
          }
          gst::PadProbeReturn::Ok
        },
      );
    }
    let src = gst::Pad::builder(gst::PadDirection::Src).name("src").build();
    src.link(&pad).unwrap();
    src.set_active(true).unwrap();
    sink.set_state(gst::State::Playing).unwrap();
    src.push_event(gst::event::StreamStart::new("test"));
    src.push_event(gst::event::Caps::new(&gst::Caps::builder("application/x-rtp").build()));
    src.push_event(gst::event::Segment::new(&gst::FormattedSegment::<gst::ClockTime>::new()));

    src.push(rtp_buffer(&two_byte, 1)).unwrap();
    let mut list = gst::BufferList::new();
    {
      let list = list.get_mut().unwrap();
      list.add(rtp_buffer(&two_byte, 2));
      // Already one-byte: must pass untouched.
      let mut untouched = gst::Buffer::from_mut_slice(one_byte.clone());
      untouched.get_mut().unwrap().set_pts(gst::ClockTime::from_nseconds(3));
      list.add(untouched);
    }
    src.push_list(list).unwrap();
    sink.set_state(gst::State::Null).unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    for (buffer, pts) in seen.iter().zip(1..) {
      assert_eq!(buffer.pts(), Some(gst::ClockTime::from_nseconds(pts)));
      assert_eq!(buffer.map_readable().unwrap().as_slice(), one_byte.as_slice());
    }
    assert!(seen[0]
      .flags()
      .contains(gst::BufferFlags::DISCONT | gst::BufferFlags::MARKER));
    assert!(seen[1]
      .flags()
      .contains(gst::BufferFlags::DISCONT | gst::BufferFlags::MARKER));
    assert!(!seen[2].flags().contains(gst::BufferFlags::DISCONT));
  }
}
