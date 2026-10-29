//! What we ask the bridge to send us. The library keeps the state, so that it can be sent again whenever the bridge
//! channel is (re)connected.

use std::collections::HashMap;

use colibri::{Constraints, ReceiverVideoConstraints};

use crate::source::{MediaType, RemoteSource};

/// How much video to receive from whom, worked out from the sources that exist. The policy is applied again
/// whenever sources are added or removed.
///
/// While a policy is set it decides `last_n`, `default_constraints` and `constraints`, as well as
/// `on_stage_sources` if `screenshare_on_stage` is set and `assumed_bandwidth_bps` if the policy has one; what the
/// other setters put in those places is overruled. `selected_sources` stays under the control of the application.
#[derive(Debug, Clone, PartialEq)]
pub struct VideoReceivePolicy {
  /// The number of video sources to receive; `-1` for all.
  pub last_n: i32,
  /// For the sources that are cameras, and for the ones the bridge announces later.
  pub camera: Constraints,
  pub screenshare: Constraints,
  /// Put the screen shares on stage, so that the bridge prefers them.
  pub screenshare_on_stage: bool,
  pub assumed_bandwidth_bps: Option<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct ReceiverState {
  /// What the application has set.
  manual: ReceiverVideoConstraints,
  policy: Option<VideoReceivePolicy>,
}

impl ReceiverState {
  /// Forgets everything that was set and takes `constraints` instead.
  pub(crate) fn replace(&mut self, constraints: ReceiverVideoConstraints) {
    self.manual = constraints;
  }

  pub(crate) fn merge(&mut self, update: ReceiverVideoConstraints) {
    self.manual.merge(update);
  }

  pub(crate) fn set_policy(&mut self, policy: Option<VideoReceivePolicy>) {
    self.policy = policy;
  }

  /// `None` takes the assumption back.
  pub(crate) fn set_assumed_bandwidth(&mut self, bps: Option<u64>) {
    self.manual.assumed_bandwidth_bps = bps.map(|bps| i64::try_from(bps).unwrap_or(i64::MAX));
  }

  /// What is to be sent: the constraints that were set, with the policy applied to `sources`.
  pub(crate) fn effective(&self, sources: &[RemoteSource]) -> ReceiverVideoConstraints {
    let mut constraints = self.manual.clone();
    let Some(policy) = &self.policy else {
      return constraints;
    };

    constraints.last_n = Some(policy.last_n);
    constraints.default_constraints = Some(policy.camera);
    let mut per_source = HashMap::new();
    let mut on_stage = vec![];
    for source in sources.iter().filter(|source| source.media_type.is_video() && source.participant_id.is_some()) {
      let Some(name) = &source.source_name else {
        continue;
      };
      let screenshare = source.media_type == MediaType::VideoScreenshare;
      per_source.insert(
        name.clone(),
        if screenshare { policy.screenshare } else { policy.camera },
      );
      if screenshare {
        on_stage.push(name.clone());
      }
    }
    constraints.constraints = Some(per_source);
    if policy.screenshare_on_stage {
      on_stage.sort();
      constraints.on_stage_sources = Some(on_stage);
    }
    if let Some(bps) = policy.assumed_bandwidth_bps {
      constraints.assumed_bandwidth_bps = Some(i64::try_from(bps).unwrap_or(i64::MAX));
    }
    constraints
  }
}

/// The values that give a field of the bridge's allocation settings back to the bridge. A message that leaves a field
/// out does not do that: the bridge keeps what it has (`AllocationSettings.kt`, `setBandwidthAllocationSettings`, only
/// applies what is not null), and the endpoint outlives the WebSocket.
mod neutral {
  use colibri::Constraints;

  /// `videobridge.cc.initial-last-n`: no limit.
  pub(super) const LAST_N: i32 = -1;
  /// `videobridge.cc.default-max-height-px`, which the bridge gives an endpoint whose first constraints message has
  /// no `defaultConstraints`. There is no value that means "not set" for these: `-1` would be "unlimited".
  pub(super) const DEFAULT_CONSTRAINTS: Constraints = Constraints::height(180);
  /// A negative value is ignored by the bridge, as before the first one.
  pub(super) const ASSUMED_BANDWIDTH_BPS: i64 = -1;
}

/// What the bridge holds of the receiver constraints, going by what we have sent it, so that a field the application
/// has taken back is reset rather than left out of the message.
#[derive(Debug)]
pub(crate) struct BridgeConstraints {
  /// `None`: not known, because a message may or may not have arrived; any field may be set.
  held: Option<ReceiverVideoConstraints>,
}

impl Default for BridgeConstraints {
  fn default() -> Self {
    Self {
      held: Some(ReceiverVideoConstraints::default()),
    }
  }
}

impl BridgeConstraints {
  /// The bridge has an endpoint of ours that is new: it holds nothing.
  pub(crate) fn clear(&mut self) {
    *self = Self::default();
  }

  /// A message may have been lost on the way.
  pub(crate) fn lose_track(&mut self) {
    self.held = None;
  }

  /// The message that makes the bridge hold `wanted`, and assumes it is sent. It has `wanted`, and the neutral value
  /// of every field that the bridge may hold and `wanted` does not set. `only_changes`: nothing if the bridge holds
  /// `wanted` already.
  pub(crate) fn update(
    &mut self,
    wanted: ReceiverVideoConstraints,
    only_changes: bool,
  ) -> Option<ReceiverVideoConstraints> {
    if only_changes && self.held.as_ref() == Some(&wanted) {
      return None;
    }
    let message = match &self.held {
      Some(held) => with_resets(&wanted, held),
      None => with_resets(&wanted, &everything()),
    };
    self.held = Some(wanted);
    (!message.is_empty()).then_some(message)
  }
}

/// Every field set to something other than its neutral value.
fn everything() -> ReceiverVideoConstraints {
  ReceiverVideoConstraints {
    last_n: Some(neutral::LAST_N + 1),
    selected_sources: Some(vec![String::new()]),
    on_stage_sources: Some(vec![String::new()]),
    default_constraints: Some(Constraints::height(neutral::DEFAULT_CONSTRAINTS.max_height + 1)),
    constraints: Some(HashMap::from([(String::new(), Constraints::UNLIMITED)])),
    assumed_bandwidth_bps: Some(neutral::ASSUMED_BANDWIDTH_BPS + 1),
  }
}

fn with_resets(wanted: &ReceiverVideoConstraints, held: &ReceiverVideoConstraints) -> ReceiverVideoConstraints {
  /// `wanted`, or the neutral value if the bridge holds something else.
  fn field<T: PartialEq + Clone>(wanted: &Option<T>, held: &Option<T>, neutral: T) -> Option<T> {
    wanted
      .clone()
      .or_else(|| held.as_ref().filter(|held| **held != neutral).map(|_| neutral))
  }

  ReceiverVideoConstraints {
    last_n: field(&wanted.last_n, &held.last_n, neutral::LAST_N),
    selected_sources: field(&wanted.selected_sources, &held.selected_sources, vec![]),
    on_stage_sources: field(&wanted.on_stage_sources, &held.on_stage_sources, vec![]),
    default_constraints: field(
      &wanted.default_constraints,
      &held.default_constraints,
      neutral::DEFAULT_CONSTRAINTS,
    ),
    constraints: field(&wanted.constraints, &held.constraints, HashMap::new()),
    assumed_bandwidth_bps: field(
      &wanted.assumed_bandwidth_bps,
      &held.assumed_bandwidth_bps,
      neutral::ASSUMED_BANDWIDTH_BPS,
    ),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn source(participant: Option<&str>, ssrc: u32, media_type: MediaType, name: Option<&str>) -> RemoteSource {
    RemoteSource {
      participant_id: participant.map(ToOwned::to_owned),
      ssrc,
      media_type,
      source_name: name.map(ToOwned::to_owned),
    }
  }

  fn policy() -> VideoReceivePolicy {
    VideoReceivePolicy {
      last_n: 4,
      camera: Constraints::height(360),
      screenshare: Constraints {
        max_height: 1080,
        max_frame_rate: Some(5.0),
      },
      screenshare_on_stage: true,
      assumed_bandwidth_bps: Some(3_000_000),
    }
  }

  #[test]
  fn without_a_policy_the_constraints_are_what_was_set() {
    let mut state = ReceiverState::default();
    assert!(state.effective(&[]).is_empty());
    state.merge(ReceiverVideoConstraints {
      last_n: Some(3),
      ..Default::default()
    });
    state.merge(ReceiverVideoConstraints {
      selected_sources: Some(vec!["a-v0".into()]),
      ..Default::default()
    });
    assert_eq!(
      state.effective(&[]),
      ReceiverVideoConstraints {
        last_n: Some(3),
        selected_sources: Some(vec!["a-v0".into()]),
        ..Default::default()
      }
    );
    state.replace(ReceiverVideoConstraints {
      last_n: Some(1),
      ..Default::default()
    });
    assert_eq!(state.effective(&[]).selected_sources, None);
    assert_eq!(state.effective(&[]).last_n, Some(1));
  }

  #[test]
  fn the_policy_constrains_each_source_by_its_type() {
    let mut state = ReceiverState::default();
    state.merge(ReceiverVideoConstraints {
      selected_sources: Some(vec!["a-v0".into()]),
      ..Default::default()
    });
    state.set_policy(Some(policy()));
    let sources = [
      source(Some("a"), 1, MediaType::Audio, Some("a-a0")),
      source(Some("a"), 2, MediaType::Video, Some("a-v0")),
      source(Some("b"), 3, MediaType::VideoScreenshare, Some("b-v1")),
      source(Some("c"), 4, MediaType::Video, None),
      source(None, 5, MediaType::Video, Some("jvb-v0")),
    ];
    let constraints = state.effective(&sources);
    assert_eq!(constraints.last_n, Some(4));
    assert_eq!(constraints.default_constraints, Some(Constraints::height(360)));
    assert_eq!(
      constraints.constraints,
      Some(HashMap::from([
        ("a-v0".to_owned(), Constraints::height(360)),
        (
          "b-v1".to_owned(),
          Constraints {
            max_height: 1080,
            max_frame_rate: Some(5.0)
          }
        ),
      ]))
    );
    assert_eq!(constraints.on_stage_sources, Some(vec!["b-v1".to_owned()]));
    assert_eq!(constraints.assumed_bandwidth_bps, Some(3_000_000));
    // The selection is the application's.
    assert_eq!(constraints.selected_sources, Some(vec!["a-v0".to_owned()]));
  }

  #[test]
  fn the_policy_follows_the_sources() {
    let mut state = ReceiverState::default();
    state.set_policy(Some(policy()));
    let camera = source(Some("a"), 2, MediaType::Video, Some("a-v0"));
    let screenshare = source(Some("a"), 3, MediaType::VideoScreenshare, Some("a-v1"));

    assert_eq!(
      state.effective(&[camera.clone(), screenshare]).on_stage_sources,
      Some(vec!["a-v1".to_owned()])
    );
    // The screen share stops: the stage is cleared, not left as it was.
    let constraints = state.effective(&[camera]);
    assert_eq!(constraints.on_stage_sources, Some(vec![]));
    assert_eq!(constraints.constraints.unwrap().len(), 1);
  }

  #[test]
  fn the_policy_leaves_the_stage_alone_unless_asked() {
    let mut state = ReceiverState::default();
    state.merge(ReceiverVideoConstraints {
      on_stage_sources: Some(vec!["a-v0".into()]),
      assumed_bandwidth_bps: Some(500_000),
      ..Default::default()
    });
    state.set_policy(Some(VideoReceivePolicy {
      screenshare_on_stage: false,
      assumed_bandwidth_bps: None,
      ..policy()
    }));
    let constraints = state.effective(&[source(Some("b"), 3, MediaType::VideoScreenshare, Some("b-v1"))]);
    assert_eq!(constraints.on_stage_sources, Some(vec!["a-v0".to_owned()]));
    assert_eq!(constraints.assumed_bandwidth_bps, Some(500_000));

    state.set_policy(None);
    assert_eq!(state.effective(&[]).constraints, None);
  }

  fn everything_set() -> ReceiverVideoConstraints {
    ReceiverVideoConstraints {
      last_n: Some(3),
      selected_sources: Some(vec!["a-v0".into()]),
      on_stage_sources: Some(vec!["b-v1".into()]),
      default_constraints: Some(Constraints::height(360)),
      constraints: Some(HashMap::from([("a-v0".to_owned(), Constraints::height(720))])),
      assumed_bandwidth_bps: Some(2_000_000),
    }
  }

  #[test]
  fn a_field_that_is_taken_back_is_reset_on_the_bridge() {
    let mut bridge = BridgeConstraints::default();
    assert_eq!(bridge.update(everything_set(), true), Some(everything_set()));
    // Nothing changed.
    assert_eq!(bridge.update(everything_set(), true), None);
    // Without `only_changes` the state is sent again.
    assert_eq!(bridge.update(everything_set(), false), Some(everything_set()));

    // Replacing it by a state with a field of its own resets all the others, to the values the bridge starts with.
    let replacement = ReceiverVideoConstraints {
      last_n: Some(1),
      ..Default::default()
    };
    assert_eq!(
      bridge.update(replacement.clone(), true),
      Some(ReceiverVideoConstraints {
        last_n: Some(1),
        selected_sources: Some(vec![]),
        on_stage_sources: Some(vec![]),
        default_constraints: Some(Constraints::height(180)),
        constraints: Some(HashMap::new()),
        assumed_bandwidth_bps: Some(-1),
      })
    );
    // The resets are done: they are not repeated, and what is only repeated on request.
    assert_eq!(bridge.update(replacement.clone(), true), None);
    assert_eq!(
      bridge.update(replacement, false),
      Some(ReceiverVideoConstraints {
        last_n: Some(1),
        ..Default::default()
      })
    );
    // Taking back the last field resets it, too.
    assert_eq!(
      bridge.update(ReceiverVideoConstraints::default(), true),
      Some(ReceiverVideoConstraints {
        last_n: Some(-1),
        ..Default::default()
      })
    );
    assert_eq!(bridge.update(ReceiverVideoConstraints::default(), false), None);
  }

  #[test]
  fn a_field_the_bridge_holds_at_its_neutral_value_is_not_reset() {
    let mut bridge = BridgeConstraints::default();
    bridge.update(
      ReceiverVideoConstraints {
        last_n: Some(-1),
        selected_sources: Some(vec![]),
        default_constraints: Some(Constraints::height(180)),
        ..Default::default()
      },
      false,
    );
    assert_eq!(bridge.update(ReceiverVideoConstraints::default(), true), None);
  }

  #[test]
  fn a_new_endpoint_holds_nothing_and_a_lost_message_may_have_set_anything() {
    let mut bridge = BridgeConstraints::default();
    bridge.update(everything_set(), true);
    bridge.clear();
    assert_eq!(bridge.update(ReceiverVideoConstraints::default(), true), None);

    bridge.lose_track();
    let message = bridge.update(
      ReceiverVideoConstraints {
        selected_sources: Some(vec!["c-v0".into()]),
        ..Default::default()
      },
      true,
    );
    assert_eq!(
      message,
      Some(ReceiverVideoConstraints {
        last_n: Some(-1),
        selected_sources: Some(vec!["c-v0".into()]),
        on_stage_sources: Some(vec![]),
        default_constraints: Some(Constraints::height(180)),
        constraints: Some(HashMap::new()),
        assumed_bandwidth_bps: Some(-1),
      })
    );
  }

  #[test]
  fn the_assumed_bandwidth_can_be_taken_back() {
    let mut state = ReceiverState::default();
    state.set_assumed_bandwidth(Some(2_000_000));
    assert_eq!(state.effective(&[]).assumed_bandwidth_bps, Some(2_000_000));
    state.set_assumed_bandwidth(None);
    assert!(state.effective(&[]).is_empty());
    state.set_assumed_bandwidth(Some(u64::MAX));
    assert_eq!(state.effective(&[]).assumed_bandwidth_bps, Some(i64::MAX));
  }
}
