use std::sync::OnceLock;

use anyhow::{anyhow, Result};

mod av1tu;
mod barrier;
#[cfg(test)]
pub(crate) mod test_util;
mod vp9sf;

pub(crate) use av1tu::make_av1_tu_aggregator;
pub(crate) use barrier::StreamBarrier;
pub(crate) use vp9sf::{make_vp9_superframe_builder, Vp9FrameSizes};

pub(crate) fn register() -> Result<()> {
  static REGISTERED: OnceLock<Result<(), String>> = OnceLock::new();
  REGISTERED
    .get_or_init(|| {
      gstreamer::init().map_err(|e| e.to_string())?;
      barrier::register().map_err(|e| e.to_string())?;
      av1tu::register().map_err(|e| e.to_string())?;
      vp9sf::register().map_err(|e| e.to_string())?;
      Ok(())
    })
    .clone()
    .map_err(|e| anyhow!("failed to register the internal elements: {}", e))
}
