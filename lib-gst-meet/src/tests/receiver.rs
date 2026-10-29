use std::{collections::HashMap, time::Duration};

use colibri::{ColibriMessage, Constraints, ReceiverVideoConstraints, VideoType};
use tokio::{sync::mpsc, time::timeout};

use super::{harness::Harness, xml::*};
use crate::{colibri::ColibriSender, VideoReceivePolicy};

async fn harness_with_channel() -> (Harness, mpsc::Receiver<ColibriMessage>) {
  let harness = Harness::join().await;
  let (sender, messages) = ColibriSender::detached();
  harness.conference.shared.set_colibri_sender(Some(sender));
  (harness, messages)
}

async fn constraints(messages: &mut mpsc::Receiver<ColibriMessage>) -> ReceiverVideoConstraints {
  match timeout(Duration::from_secs(5), messages.recv()).await.unwrap().unwrap() {
    ColibriMessage::ReceiverVideoConstraints(constraints) => constraints,
    other => panic!("expected the receiver video constraints, got {other:?}"),
  }
}

async fn nothing(messages: &mut mpsc::Receiver<ColibriMessage>) {
  if let Ok(Some(message)) = timeout(Duration::from_millis(100), messages.recv()).await {
    panic!("expected nothing, got {message:?}");
  }
}

fn policy() -> VideoReceivePolicy {
  VideoReceivePolicy {
    last_n: 3,
    camera: Constraints::height(360),
    screenshare: Constraints::height(1080),
    screenshare_on_stage: true,
    assumed_bandwidth_bps: None,
  }
}

#[tokio::test]
async fn each_setter_changes_its_part_and_the_whole_state_is_sent() {
  let (harness, mut messages) = harness_with_channel().await;
  let conference = &harness.conference;

  conference.set_last_n(5).await.unwrap();
  assert_eq!(
    constraints(&mut messages).await,
    ReceiverVideoConstraints {
      last_n: Some(5),
      ..Default::default()
    }
  );

  conference.set_selected_sources(vec!["bob-v0".to_owned()]).await.unwrap();
  constraints(&mut messages).await;
  conference.set_on_stage_sources(vec!["bob-v1".to_owned()]).await.unwrap();
  constraints(&mut messages).await;
  conference.set_max_receive_height(360).await.unwrap();
  constraints(&mut messages).await;
  conference.set_assumed_bandwidth(Some(2_000_000)).await.unwrap();
  assert_eq!(
    constraints(&mut messages).await,
    ReceiverVideoConstraints {
      last_n: Some(5),
      selected_sources: Some(vec!["bob-v0".to_owned()]),
      on_stage_sources: Some(vec!["bob-v1".to_owned()]),
      default_constraints: Some(Constraints::height(360)),
      constraints: None,
      assumed_bandwidth_bps: Some(2_000_000),
    }
  );

  // Setting it all replaces it all: the bridge keeps what a message leaves out, so what the new state does not set is
  // given back to it, with the values it starts with.
  conference
    .set_receiver_video_constraints(ReceiverVideoConstraints {
      last_n: Some(1),
      ..Default::default()
    })
    .await
    .unwrap();
  assert_eq!(
    constraints(&mut messages).await,
    ReceiverVideoConstraints {
      last_n: Some(1),
      selected_sources: Some(vec![]),
      on_stage_sources: Some(vec![]),
      default_constraints: Some(Constraints::height(180)),
      constraints: None,
      assumed_bandwidth_bps: Some(-1),
    }
  );
  // They are given back once.
  conference.set_last_n(2).await.unwrap();
  assert_eq!(
    constraints(&mut messages).await,
    ReceiverVideoConstraints {
      last_n: Some(2),
      ..Default::default()
    }
  );
}

#[tokio::test]
async fn what_the_application_takes_back_is_reset_on_the_bridge() {
  let (harness, mut messages) = harness_with_channel().await;
  let conference = &harness.conference;

  conference.set_assumed_bandwidth(Some(2_000_000)).await.unwrap();
  assert_eq!(constraints(&mut messages).await.assumed_bandwidth_bps, Some(2_000_000));
  conference.set_selected_sources(vec!["bob-v0".to_owned()]).await.unwrap();
  constraints(&mut messages).await;

  // The assumption is taken back: the bridge is told, it does not keep the old one. The selection stays.
  conference.set_assumed_bandwidth(None).await.unwrap();
  assert_eq!(
    constraints(&mut messages).await,
    ReceiverVideoConstraints {
      selected_sources: Some(vec!["bob-v0".to_owned()]),
      assumed_bandwidth_bps: Some(-1),
      ..Default::default()
    }
  );

  // The bridge ends up with nothing selected when the state that selected something is replaced by an empty one.
  conference
    .set_receiver_video_constraints(ReceiverVideoConstraints::default())
    .await
    .unwrap();
  assert_eq!(
    constraints(&mut messages).await,
    ReceiverVideoConstraints {
      selected_sources: Some(vec![]),
      ..Default::default()
    }
  );
  conference.set_receiver_video_constraints(ReceiverVideoConstraints::default()).await.unwrap();
  nothing(&mut messages).await;
}

async fn last_n_taken_back_while_the_channel_is_down() -> Harness {
  let (harness, mut messages) = harness_with_channel().await;
  harness.conference.set_last_n(4).await.unwrap();
  constraints(&mut messages).await;
  harness.conference.shared.set_colibri_sender(None);
  harness
    .conference
    .set_receiver_video_constraints(ReceiverVideoConstraints::default())
    .await
    .unwrap();
  harness
}

#[tokio::test]
async fn a_reconnect_gives_back_what_was_taken_back_in_the_meantime() {
  let harness = last_n_taken_back_while_the_channel_is_down().await;
  // The same endpoint on the bridge still has the number of videos: it is reset.
  assert_eq!(
    harness.conference.shared.colibri_greeting()[0],
    ColibriMessage::ReceiverVideoConstraints(ReceiverVideoConstraints {
      last_n: Some(-1),
      ..Default::default()
    })
  );
}

#[tokio::test]
async fn a_new_media_session_starts_without_resets() {
  let harness = last_n_taken_back_while_the_channel_is_down().await;
  // A new endpoint on the bridge holds nothing of ours, so there is nothing to give back.
  harness.conference.shared.bridge_endpoint_created();
  assert_eq!(harness.conference.shared.colibri_greeting().len(), 1);
}

#[tokio::test]
async fn the_state_is_sent_to_every_new_channel() {
  let (harness, mut messages) = harness_with_channel().await;
  let shared = &harness.conference.shared;
  let source_name = "abcd1234-v0".to_owned();

  // Nothing to say about the receiver, but the bridge wants to know what our video is.
  assert_eq!(
    shared.colibri_greeting(),
    [ColibriMessage::SourceVideoTypeMessage {
      source_name: source_name.clone(),
      video_type: VideoType::Camera
    }]
  );

  harness.conference.set_last_n(2).await.unwrap();
  constraints(&mut messages).await;
  harness.conference.set_video_type(VideoType::Desktop).await.unwrap();
  assert_eq!(
    timeout(Duration::from_secs(5), messages.recv()).await.unwrap().unwrap(),
    ColibriMessage::SourceVideoTypeMessage {
      source_name: source_name.clone(),
      video_type: VideoType::Desktop
    }
  );
  assert_eq!(
    shared.colibri_greeting(),
    [
      ColibriMessage::ReceiverVideoConstraints(ReceiverVideoConstraints {
        last_n: Some(2),
        ..Default::default()
      }),
      ColibriMessage::SourceVideoTypeMessage {
        source_name,
        video_type: VideoType::Desktop
      },
    ]
  );
}

#[tokio::test]
async fn setting_the_state_without_a_channel_is_not_an_error() {
  let harness = Harness::join().await;
  assert!(harness.conference.shared.colibri_sender().is_none());
  harness.conference.set_last_n(4).await.unwrap();
  assert!(harness.conference.send_colibri_message(ColibriMessage::LastNChangedEvent { last_n: 1 }).await.is_err());
  // It is there for the channel that comes.
  assert!(matches!(
    &harness.conference.shared.colibri_greeting()[0],
    ColibriMessage::ReceiverVideoConstraints(constraints) if constraints.last_n == Some(4)
  ));
}

#[tokio::test]
async fn raw_messages_are_sent_and_not_kept() {
  let (harness, mut messages) = harness_with_channel().await;
  harness
    .conference
    .send_colibri_message(ColibriMessage::EndpointMessage {
      from: None,
      to: Some("bob".to_owned()),
      msg_payload: serde_json::json!({ "hello": 1 }),
    })
    .await
    .unwrap();
  assert!(matches!(messages.recv().await.unwrap(), ColibriMessage::EndpointMessage { .. }));
  assert_eq!(harness.conference.shared.colibri_greeting().len(), 1);
}

#[tokio::test]
async fn a_policy_works_out_the_constraints_of_the_sources_there_are() {
  let (mut harness, mut messages) = harness_with_channel().await;
  harness.conference.set_selected_sources(vec!["bob-v0".to_owned()]).await.unwrap();
  constraints(&mut messages).await;

  harness.conference.set_video_receive_policy(Some(policy())).await.unwrap();
  let sent = constraints(&mut messages).await;
  assert_eq!(sent.last_n, Some(3));
  assert_eq!(sent.default_constraints, Some(Constraints::height(360)));
  assert_eq!(
    sent.constraints,
    Some(HashMap::from([("bob-v0".to_owned(), Constraints::height(360))]))
  );
  assert_eq!(sent.on_stage_sources, Some(vec![]));
  assert_eq!(sent.selected_sources, Some(vec!["bob-v0".to_owned()]));

  // Carol shares her screen: the new source is constrained and put on stage.
  harness
    .server
    .receive(&jingle(
      "add-1",
      FOCUS,
      "source-add",
      "sid-1",
      "<json-message xmlns='http://jitsi.org/jitmeet'>{\"sources\":{\"carol\":[[{\"s\":3100,\"n\":\"carol-v1\",\"v\":\"d\"}],[],[],[]]}}</json-message>",
    ))
    .await;
  harness.server.expect_ack("add-1").await;
  let sent = constraints(&mut messages).await;
  assert_eq!(
    sent.constraints,
    Some(HashMap::from([
      ("bob-v0".to_owned(), Constraints::height(360)),
      ("carol-v1".to_owned(), Constraints::height(1080)),
    ]))
  );
  assert_eq!(sent.on_stage_sources, Some(vec!["carol-v1".to_owned()]));

  // A source that changes nothing about the constraints sends nothing.
  harness.server.receive(&source_update("add-2", "source-add", "sid-1", "dave", 4000, None)).await;
  harness.server.expect_ack("add-2").await;
  nothing(&mut messages).await;

  harness
    .server
    .receive(&jingle(
      "remove-1",
      FOCUS,
      "source-remove",
      "sid-1",
      "<json-message xmlns='http://jitsi.org/jitmeet'>{\"sources\":{\"carol\":[[{\"s\":3100,\"n\":\"carol-v1\",\"v\":\"d\"}],[],[],[]]}}</json-message>",
    ))
    .await;
  harness.server.expect_ack("remove-1").await;
  let sent = constraints(&mut messages).await;
  assert_eq!(sent.on_stage_sources, Some(vec![]));
  assert_eq!(sent.constraints.unwrap().len(), 1);

  // Without the policy the explicit constraints are what is left, and what the policy had set is given back.
  harness.conference.set_video_receive_policy(None).await.unwrap();
  let sent = constraints(&mut messages).await;
  assert_eq!(sent.last_n, Some(-1));
  assert_eq!(sent.default_constraints, Some(Constraints::height(180)));
  assert_eq!(sent.constraints, Some(HashMap::new()));
  assert_eq!(sent.on_stage_sources, None, "the stage was empty");
  assert_eq!(sent.selected_sources, Some(vec!["bob-v0".to_owned()]));
}
