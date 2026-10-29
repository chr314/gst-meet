use std::fmt;

use xmpp_parsers::Error;

/// Codec for colon-separated bytes of uppercase hexadecimal.
pub struct ColonSeparatedHex;

#[derive(Debug)]
struct InvalidColonSeparatedHex(String);

impl fmt::Display for InvalidColonSeparatedHex {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "invalid colon-separated hex: {}", self.0)
  }
}

impl std::error::Error for InvalidColonSeparatedHex {}

impl ColonSeparatedHex {
  pub fn decode(s: &str) -> Result<Vec<u8>, Error> {
    if s.is_empty() {
      return Ok(vec![]);
    }
    s.split(':')
      .map(|part| {
        // from_str_radix would also accept a leading "+" and one-digit groups.
        if part.len() != 2 || !part.bytes().all(|b| b.is_ascii_hexdigit()) {
          return Err(Error::text_parse_error(InvalidColonSeparatedHex(format!(
            "expected two hex digits, got {:?}",
            part
          ))));
        }
        u8::from_str_radix(part, 16).map_err(Error::text_parse_error)
      })
      .collect()
  }

  pub fn encode(b: &[u8]) -> Option<String> {
    let mut bytes = vec![];
    for byte in b {
      bytes.push(format!("{:02X}", byte));
    }
    Some(bytes.join(":"))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn decodes_valid_fingerprints() {
    assert_eq!(ColonSeparatedHex::decode("").unwrap(), Vec::<u8>::new());
    assert_eq!(ColonSeparatedHex::decode("0A").unwrap(), vec![0x0a]);
    assert_eq!(
      ColonSeparatedHex::decode("00:ff:Ab:7F").unwrap(),
      vec![0x00, 0xff, 0xab, 0x7f]
    );
  }

  #[test]
  fn roundtrips() {
    let bytes: Vec<u8> = (0..=255).collect();
    let encoded = ColonSeparatedHex::encode(&bytes).unwrap();
    assert_eq!(ColonSeparatedHex::decode(&encoded).unwrap(), bytes);
  }

  #[test]
  fn rejects_malformed_input_without_panicking() {
    for bad in [
      "0",
      "0A:",
      ":0A",
      "0A::0B",
      "0A:B",
      "0A:0B:0",
      "0A-0B",
      "0A 0B",
      "0G",
      "+F",
      "-1",
      "0A0B",
      "0A:0B:",
      " 0A",
      "é",
      "é:0A",
      "0A:é",
      "0\u{e9}",
      "0A:0\u{e9}:",
      "日本",
      "\u{0}",
      "0A:\u{1F600}",
    ] {
      assert!(ColonSeparatedHex::decode(bad).is_err(), "{bad:?} should be rejected");
    }
  }
}
