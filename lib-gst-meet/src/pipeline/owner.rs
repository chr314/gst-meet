//! Who is responsible for a pipeline element, recorded on the element itself.
//!
//! The pipeline has one bus. An error message names the element it came from; walking up that element's parents
//! to the first one carrying an [`Owner`] tells whether the error belongs to one stream, one session or nothing
//! in particular (in which case it is fatal).

use gstreamer::prelude::*;

use super::types::StreamId;

const OWNER_KEY: &str = "gst-meet-owner";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Owner {
  Session {
    session: u64,
  },
  Stream {
    session: u64,
    stream_id: StreamId,
    part: StreamPart,
  },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamPart {
  /// The depayloader chain the library built. A stream gets a new chain when its payload type changes; the
  /// generation tells which one the element belongs to, as errors of one that was replaced are not the stream's.
  Chain(u64),
  /// The per-stream consumer element the sink factory returned.
  Consumer,
}

/// Mark `object` before it joins the pipeline, so that errors posted while it is being set up are attributed.
pub(crate) fn mark(object: &impl IsA<glib::Object>, owner: Owner) {
  // SAFETY: the key is only ever used with the `Owner` type, here and in `of`.
  unsafe { object.set_data(OWNER_KEY, owner) };
}

pub(crate) fn of(object: &impl IsA<glib::Object>) -> Option<Owner> {
  // SAFETY: see `mark`.
  unsafe { object.data::<Owner>(OWNER_KEY).map(|owner| *owner.as_ref()) }
}

/// The owner of `source` or its closest marked ancestor.
pub(crate) fn classify(source: &gstreamer::Object) -> Option<Owner> {
  let mut current = Some(source.clone());
  while let Some(object) = current {
    if let Some(owner) = of(&object) {
      return Some(owner);
    }
    current = object.parent();
  }
  None
}
