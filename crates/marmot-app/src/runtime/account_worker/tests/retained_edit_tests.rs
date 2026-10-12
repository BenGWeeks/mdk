//! Retained edit dependency and per-group scheduling regressions in production policy mode.

use super::*;
use crate::tests::ScriptedPushRelayClient;
use marmot_account::AccountHome;

#[tokio::test]
async fn engine_owned_failed_original_defers_retained_edit_without_finishing_it() {
    use cgka_traits::storage::{OutboundIntentStorage, QueuedOutboundIntent};
    let dir = tempfile::tempdir().unwrap();
    let account = AccountHome::open(dir.path())
        .create_account("alice")
        .unwrap();
    let app = MarmotApp::with_relay(dir.path(), "wss://relay.example")
        .with_test_relay_client(Arc::new(ScriptedPushRelayClient::default()));
    let mut client = app.client("alice").await.unwrap();
    let group = client.create_group("retained edit", &[]).await.unwrap();
    let sibling = client.create_group("sibling", &[]).await.unwrap();
    let group_hex = hex::encode(group.as_slice());
    let original = app
        .admit_local_message_at(
            "alice",
            &group,
            "original".into(),
            crate::local_submissions::LocalMessageRequest {
                content: "original text".into(),
                reply_to: None,
                attachments: vec![],
            },
            None,
            42,
        )
        .unwrap()
        .0;
    app.admit_local_message_with_edit_at(
        "alice",
        &group,
        "edit".into(),
        crate::local_submissions::LocalMessageRequest {
            content: "updated text".into(),
            reply_to: None,
            attachments: vec![],
        },
        None,
        Some("original".into()),
        crate::unix_now_seconds(),
    )
    .unwrap();
    let storage = app.account_storage("alice").unwrap();
    let queued = storage
        .local_submission(&group_hex, "original")
        .unwrap()
        .unwrap();
    storage
        .put_queued_outbound_intent(&QueuedOutboundIntent {
            id: cgka_traits::MessageId::new(vec![5; 32]),
            group_id: group.clone(),
            intent: cgka_traits::SendIntent::AppMessage {
                group_id: group.clone(),
                payload: queued.payload.unwrap(),
                expected_epoch: None,
            },
            created_at_ms: 1,
            reissue_attempts: 0,
        })
        .unwrap();
    assert_eq!(
        storage
            .local_submission(&group_hex, "original")
            .unwrap()
            .unwrap()
            .state,
        1
    );
    app.invalidate_timeline_app_event(
        "alice",
        &group_hex,
        &original.message_id_hex,
        crate::LOCAL_PUBLISH_FAILED_REASON,
    )
    .unwrap();
    let runtime = crate::MarmotAppRuntime::new(app.clone());
    let edit = storage
        .local_submission(&group_hex, "edit")
        .unwrap()
        .unwrap();
    assert!(client.retained_edit_waits_for_original(&edit).unwrap());
    let retry = publish_next_local_submission(
        &mut client,
        &app,
        &runtime.shared_services(),
        &runtime.events,
        &account.account_id_hex,
        "alice",
    )
    .await;
    assert_eq!(retry, Some(Duration::from_secs(1)));
    let retained = storage
        .local_submission(&group_hex, "edit")
        .unwrap()
        .unwrap();
    assert_eq!(retained.state, 0);
    assert!(retained.request_json.is_some() && retained.payload.is_some());
    assert!(retained.outcome_json.is_none());
    for (group, token) in [(&group, "later-same-group"), (&sibling, "sibling")] {
        app.admit_local_message(
            "alice",
            group,
            token.into(),
            crate::local_submissions::LocalMessageRequest {
                content: token.into(),
                reply_to: None,
                attachments: vec![],
            },
            None,
        )
        .unwrap();
    }
    publish_next_local_submission(
        &mut client,
        &app,
        &runtime.shared_services(),
        &runtime.events,
        &account.account_id_hex,
        "alice",
    )
    .await;
    assert!(
        storage
            .local_submission(&hex::encode(sibling.as_slice()), "sibling")
            .unwrap()
            .unwrap()
            .outcome_json
            .is_some()
    );
    assert!(
        storage
            .local_submission(&group_hex, "later-same-group")
            .unwrap()
            .unwrap()
            .outcome_json
            .is_none()
    );
    assert!(
        storage
            .local_submission(&group_hex, "edit")
            .unwrap()
            .unwrap()
            .outcome_json
            .is_none()
    );
    app.clear_timeline_local_publish_failure("alice", &group_hex, &original.message_id_hex)
        .unwrap();
    assert!(!client.retained_edit_waits_for_original(&retained).unwrap());
    publish_next_local_submission(
        &mut client,
        &app,
        &runtime.shared_services(),
        &runtime.events,
        &account.account_id_hex,
        "alice",
    )
    .await;
    let resumed = storage
        .local_submission(&group_hex, "edit")
        .unwrap()
        .unwrap();
    assert_eq!(resumed.state, 1);
    assert!(resumed.outcome_json.is_some());
    app.invalidate_timeline_app_event(
        "alice",
        &group_hex,
        &original.message_id_hex,
        "convergence_invalidated",
    )
    .unwrap();
    assert!(!client.retained_edit_waits_for_original(&retained).unwrap());
}
