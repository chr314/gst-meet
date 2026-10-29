//! Parser for the VP9 RTP payload descriptor (RFC 9628 section 4.2).
//!
//! ```text
//!  0 1 2 3 4 5 6 7
//! +-+-+-+-+-+-+-+-+
//! |I|P|L|F|B|E|V|Z|  (required)
//! +-+-+-+-+-+-+-+-+
//! |M| PICTURE ID  |  I: 7 bit picture id, or with M set 15 bits in two bytes
//! +-+-+-+-+-+-+-+-+
//! | TID |U| SID |D|  L: layer indices
//! +-+-+-+-+-+-+-+-+
//! |   TL0PICIDX   |  L and not F (non-flexible mode)
//! +-+-+-+-+-+-+-+-+
//! |  P_DIFF   |N|  P and F: one to three reference indices, N set if another follows
//! +-+-+-+-+-+-+-+-+
//! | N_S |Y|G|-|-|-|  V: scalability structure, then Y: (N_S + 1) x (16 bit width, 16 bit height)
//! +-+-+-+-+-+-+-+-+     and G: N_G, then N_G x (T, U, R, 2 reserved bits) each followed by R x P_DIFF
//! ```

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Vp9DescriptorError;

impl fmt::Display for Vp9DescriptorError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("truncated VP9 payload descriptor")
  }
}

impl std::error::Error for Vp9DescriptorError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LayerIndices {
  pub(crate) temporal_id: u8,
  /// Switching up point (U).
  pub(crate) switching_up: bool,
  pub(crate) spatial_id: u8,
  /// Inter-layer dependency used (D).
  pub(crate) inter_layer_dependency: bool,
  /// Only present in non-flexible mode.
  pub(crate) tl0_pic_idx: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PictureGroupEntry {
  pub(crate) temporal_id: u8,
  pub(crate) switching_up: bool,
  pub(crate) reference_diffs: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScalabilityStructure {
  /// N_S + 1.
  pub(crate) spatial_layers: u8,
  /// One entry per spatial layer when Y is set.
  pub(crate) resolutions: Vec<(u16, u16)>,
  /// Empty unless G is set.
  pub(crate) picture_group: Vec<PictureGroupEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Vp9Descriptor {
  /// Length of the descriptor, that is the offset of the VP9 payload.
  pub(crate) header_len: usize,
  /// P: the layer frame is inter-picture predicted.
  pub(crate) inter_picture_predicted: bool,
  /// F: flexible mode.
  pub(crate) flexible: bool,
  /// B: first packet of a layer frame.
  pub(crate) begin: bool,
  /// E: last packet of a layer frame.
  pub(crate) end: bool,
  /// Z: not a reference frame for upper spatial layers.
  pub(crate) not_reference_for_upper_layers: bool,
  pub(crate) picture_id: Option<u16>,
  /// The picture id uses 15 bits (M set).
  pub(crate) picture_id_15_bit: bool,
  pub(crate) layer: Option<LayerIndices>,
  /// P_DIFF chain, only in flexible mode with P set.
  pub(crate) reference_diffs: Vec<u8>,
  pub(crate) scalability_structure: Option<ScalabilityStructure>,
}

impl Vp9Descriptor {
  /// Whether this packet starts a picture: the first packet of the base layer frame, or of a layer frame that does
  /// not depend on a lower one.
  pub(crate) fn starts_picture(&self) -> bool {
    self.begin && self.layer.is_none_or(|layer| !layer.inter_layer_dependency)
  }
}

struct Reader<'a> {
  data: &'a [u8],
  pos: usize,
}

impl Reader<'_> {
  fn u8(&mut self) -> Result<u8, Vp9DescriptorError> {
    let byte = *self.data.get(self.pos).ok_or(Vp9DescriptorError)?;
    self.pos += 1;
    Ok(byte)
  }

  fn skip(&mut self, n: usize) -> Result<(), Vp9DescriptorError> {
    if self.data.len() - self.pos < n {
      return Err(Vp9DescriptorError);
    }
    self.pos += n;
    Ok(())
  }
}

/// The VP9 payload follows at `payload[descriptor.header_len..]` and may be empty.
pub(crate) fn parse(payload: &[u8]) -> Result<Vp9Descriptor, Vp9DescriptorError> {
  let mut r = Reader { data: payload, pos: 0 };
  let first = r.u8()?;
  let has_picture_id = first & 0x80 != 0;
  let inter_picture_predicted = first & 0x40 != 0;
  let has_layer_indices = first & 0x20 != 0;
  let flexible = first & 0x10 != 0;
  let begin = first & 0x08 != 0;
  let end = first & 0x04 != 0;
  let has_scalability_structure = first & 0x02 != 0;
  let not_reference_for_upper_layers = first & 0x01 != 0;

  let mut picture_id = None;
  let mut picture_id_15_bit = false;
  if has_picture_id {
    let byte = r.u8()?;
    if byte & 0x80 != 0 {
      picture_id_15_bit = true;
      picture_id = Some(u16::from(byte & 0x7F) << 8 | u16::from(r.u8()?));
    } else {
      picture_id = Some(u16::from(byte));
    }
  }

  let mut layer = None;
  if has_layer_indices {
    let byte = r.u8()?;
    layer = Some(LayerIndices {
      temporal_id: byte >> 5,
      switching_up: byte & 0x10 != 0,
      spatial_id: (byte >> 1) & 0x07,
      inter_layer_dependency: byte & 0x01 != 0,
      tl0_pic_idx: if flexible { None } else { Some(r.u8()?) },
    });
  }

  let mut reference_diffs = Vec::new();
  if inter_picture_predicted && flexible {
    // At least one, at most three.
    for _ in 0..3 {
      let byte = r.u8()?;
      reference_diffs.push(byte >> 1);
      if byte & 0x01 == 0 {
        break;
      }
    }
  }

  let mut scalability_structure = None;
  if has_scalability_structure {
    let byte = r.u8()?;
    let spatial_layers = (byte >> 5) + 1;
    let has_resolutions = byte & 0x10 != 0;
    let has_picture_group = byte & 0x08 != 0;

    let mut resolutions = Vec::new();
    if has_resolutions {
      for _ in 0..spatial_layers {
        let width = u16::from(r.u8()?) << 8 | u16::from(r.u8()?);
        let height = u16::from(r.u8()?) << 8 | u16::from(r.u8()?);
        resolutions.push((width, height));
      }
    }

    let mut picture_group = Vec::new();
    if has_picture_group {
      let entries = r.u8()?;
      for _ in 0..entries {
        let byte = r.u8()?;
        let diffs = usize::from((byte >> 2) & 0x03);
        let start = r.pos;
        r.skip(diffs)?;
        picture_group.push(PictureGroupEntry {
          temporal_id: byte >> 5,
          switching_up: byte & 0x10 != 0,
          reference_diffs: payload[start..start + diffs].to_vec(),
        });
      }
    }
    scalability_structure = Some(ScalabilityStructure {
      spatial_layers,
      resolutions,
      picture_group,
    });
  }

  Ok(Vp9Descriptor {
    header_len: r.pos,
    inter_picture_predicted,
    flexible,
    begin,
    end,
    not_reference_for_upper_layers,
    picture_id,
    picture_id_15_bit,
    layer,
    reference_diffs,
    scalability_structure,
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  /// The L byte: TID, U, SID and D.
  fn layer_byte(tid: u8, u: u8, sid: u8, d: u8) -> u8 {
    tid << 5 | u << 4 | sid << 1 | d
  }

  #[test]
  fn minimal_descriptor() {
    // B and E: a complete layer frame in one packet, nothing optional.
    let d = parse(&[0x0C, 0xAA, 0xBB]).unwrap();
    assert_eq!(d.header_len, 1);
    assert!(d.begin && d.end);
    assert!(!d.inter_picture_predicted && !d.flexible && !d.not_reference_for_upper_layers);
    assert_eq!(d.picture_id, None);
    assert_eq!(d.layer, None);
    assert!(d.reference_diffs.is_empty());
    assert_eq!(d.scalability_structure, None);
    assert!(d.starts_picture());
  }

  #[test]
  fn continuation_packet() {
    let d = parse(&[0x00, 1, 2, 3]).unwrap();
    assert_eq!(d.header_len, 1);
    assert!(!d.begin && !d.end);
    assert!(!d.starts_picture());
  }

  #[test]
  fn seven_and_fifteen_bit_picture_ids() {
    let d = parse(&[0x88, 0x55, 9]).unwrap();
    assert_eq!(
      (d.header_len, d.picture_id, d.picture_id_15_bit),
      (2, Some(0x55), false)
    );

    let d = parse(&[0x88, 0x81, 0x23, 9]).unwrap();
    assert_eq!(
      (d.header_len, d.picture_id, d.picture_id_15_bit),
      (3, Some(0x0123), true)
    );

    let d = parse(&[0x88, 0xFF, 0xFF, 9]).unwrap();
    assert_eq!(d.picture_id, Some(0x7FFF));
  }

  #[test]
  fn layer_indices_in_non_flexible_mode_have_tl0picidx() {
    // I, L, B: picture id 0x12; TID 2, U 1, SID 1, D 1; TL0PICIDX 0x77.
    let d = parse(&[0xA8, 0x12, layer_byte(2, 1, 1, 1), 0x77, 0xEE]).unwrap();
    assert_eq!(d.header_len, 4);
    assert_eq!(
      d.layer,
      Some(LayerIndices {
        temporal_id: 2,
        switching_up: true,
        spatial_id: 1,
        inter_layer_dependency: true,
        tl0_pic_idx: Some(0x77),
      })
    );
    assert!(
      !d.starts_picture(),
      "B with an inter-layer dependency continues the picture"
    );
  }

  #[test]
  fn layer_frame_without_inter_layer_dependency_starts_a_picture() {
    // L, B; TID 0, SID 2, D 0.
    let d = parse(&[0x28, layer_byte(0, 0, 2, 0), 0x01, 0xEE]).unwrap();
    assert_eq!(d.layer.unwrap().spatial_id, 2);
    assert!(d.starts_picture());
  }

  #[test]
  fn flexible_mode_reference_chain() {
    // I, P, L, F, B, E: no TL0PICIDX; P_DIFF 5 (N), 7 (N), 2 (end).
    let d = parse(&[0xFC, 0x10, layer_byte(1, 0, 0, 0), 5 << 1 | 1, 7 << 1 | 1, 2 << 1, 0xEE]).unwrap();
    assert_eq!(d.header_len, 6);
    assert!(d.flexible && d.inter_picture_predicted);
    assert_eq!(d.layer.unwrap().tl0_pic_idx, None);
    assert_eq!(d.reference_diffs, vec![5, 7, 2]);
  }

  #[test]
  fn flexible_mode_reference_chain_stops_after_three() {
    // The third entry still has N set, but the descriptor allows three at most.
    let d = parse(&[0x5C, 1 << 1 | 1, 2 << 1 | 1, 3 << 1 | 1, 0xEE, 0xEE]).unwrap();
    assert_eq!(d.header_len, 4);
    assert_eq!(d.reference_diffs, vec![1, 2, 3]);
  }

  #[test]
  fn flexible_mode_single_reference() {
    let d = parse(&[0x5C, 4 << 1, 0xEE]).unwrap();
    assert_eq!(d.header_len, 2);
    assert_eq!(d.reference_diffs, vec![4]);
  }

  #[test]
  fn reference_diffs_are_only_read_in_flexible_inter_pictures() {
    // P without F: the byte after the descriptor is payload.
    let d = parse(&[0x48, 0xFF, 0xEE]).unwrap();
    assert_eq!(d.header_len, 1);
    assert!(d.reference_diffs.is_empty());
    // F without P likewise.
    let d = parse(&[0x18, 0xFF, 0xEE]).unwrap();
    assert_eq!(d.header_len, 1);
    assert!(d.reference_diffs.is_empty());
  }

  #[test]
  fn scalability_structure_with_resolutions_and_picture_group() {
    // V, B. N_S = 2 (three spatial layers), Y, G.
    let mut p = vec![0x0A, 2 << 5 | 0x10 | 0x08];
    for (w, h) in [(320u16, 180u16), (640, 360), (1280, 720)] {
      p.extend(w.to_be_bytes());
      p.extend(h.to_be_bytes());
    }
    p.push(2); // N_G
    p.push(0); // T 0, U 0, R 0
    p.extend([1 << 5 | 1 << 4 | 2 << 2, 1, 2]); // T 1, U 1, R 2: P_DIFF 1, 2
    let header_len = p.len();
    p.extend([0xEE; 4]);

    let d = parse(&p).unwrap();

    assert_eq!(d.header_len, header_len);
    assert_eq!(header_len, 1 + 1 + 12 + 1 + 1 + 3);
    let ss = d.scalability_structure.unwrap();
    assert_eq!(ss.spatial_layers, 3);
    assert_eq!(ss.resolutions, vec![(320, 180), (640, 360), (1280, 720)]);
    assert_eq!(
      ss.picture_group,
      vec![
        PictureGroupEntry {
          temporal_id: 0,
          switching_up: false,
          reference_diffs: vec![]
        },
        PictureGroupEntry {
          temporal_id: 1,
          switching_up: true,
          reference_diffs: vec![1, 2]
        },
      ]
    );
  }

  #[test]
  fn scalability_structure_without_optional_parts() {
    // N_S = 0, neither Y nor G.
    let d = parse(&[0x0A, 0x00, 0xEE]).unwrap();
    assert_eq!(d.header_len, 2);
    let ss = d.scalability_structure.unwrap();
    assert_eq!(ss.spatial_layers, 1);
    assert!(ss.resolutions.is_empty() && ss.picture_group.is_empty());
  }

  #[test]
  fn everything_at_once() {
    // I (15 bit), P, L, F, B, E, V, Z: the longest header order is I, L, P_DIFF, SS.
    let mut p = vec![0xFF, 0x80 | 0x01, 0x02, layer_byte(0, 0, 1, 1), 3 << 1 | 1, 6 << 1];
    p.extend([0x10, 0x02, 0x80, 0x01, 0x68]); // N_S 0, Y: 640x360
    let header_len = p.len();
    p.push(0xEE);
    let d = parse(&p).unwrap();
    assert_eq!(d.header_len, header_len);
    assert_eq!(d.picture_id, Some(0x0102));
    assert!(d.not_reference_for_upper_layers);
    assert_eq!(d.reference_diffs, vec![3, 6]);
    assert_eq!(d.scalability_structure.unwrap().resolutions, vec![(640, 360)]);
  }

  #[test]
  fn truncated_descriptors_are_errors() {
    assert_eq!(parse(&[]), Err(Vp9DescriptorError));
    // I without picture id, 15 bit picture id cut short, L without layer byte, non-flexible without TL0PICIDX.
    assert!(parse(&[0x80]).is_err());
    assert!(parse(&[0x80, 0x80]).is_err());
    assert!(parse(&[0x20]).is_err());
    assert!(parse(&[0x20, 0x00]).is_err());
    // P_DIFF chain cut short, with N set on the last byte present.
    assert!(parse(&[0x50]).is_err());
    assert!(parse(&[0x50, 1 << 1 | 1]).is_err());
    // SS: missing header, resolutions, N_G, group entries and their P_DIFFs.
    assert!(parse(&[0x02]).is_err());
    assert!(parse(&[0x02, 1 << 5 | 0x10, 0, 1, 0, 1]).is_err());
    assert!(parse(&[0x02, 0x08]).is_err());
    assert!(parse(&[0x02, 0x08, 2, 0]).is_err());
    assert!(parse(&[0x02, 0x08, 1, 2 << 2, 1]).is_err());
  }

  #[test]
  fn descriptor_may_end_exactly_at_the_end_of_the_input() {
    // The caller decides whether an empty payload is acceptable.
    let d = parse(&[0x88, 0x12]).unwrap();
    assert_eq!(d.header_len, 2);
  }
}
