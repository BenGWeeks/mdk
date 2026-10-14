//! Real account-runtime membership changes through the app projection boundary.

use super::*;
use crate::conversation_presentation::ConversationAuthority;
use cgka_traits::engine::{GroupEvent, SendIntent};

fn app_at(dir: &tempfile::TempDir, relay: Arc<ScriptedPushRelayClient>) -> MarmotApp {
    MarmotApp::with_relay_and_config(
        dir.path(),
        "wss://relay.example",
        MarmotAppConfig {
            key_package_client_name: Some("Test Client".into()),
            ..MarmotAppConfig::default()
        }
        .with_dev_settlement_quiescence_ms(0)
        .with_dev_scheduled_convergence_delay_ms(0),
    )
    .with_test_relay_client(relay)
}

/// Deliver actual signed publications through the app's normal inbound seam.
async fn deliver_new(client: &mut AppClient, relay: &ScriptedPushRelayClient, cursor: &mut usize) {
    let events = relay.published_events.lock().unwrap().clone();
    let account = client
        .app
        .account_home()
        .account(&client.state.label)
        .unwrap();
    for event in &events[*cursor..] {
        let plane = match event.kind {
            1059 if event.tags.iter().any(|tag| {
                tag.first().is_some_and(|v| v == "p") && tag.get(1) == Some(&account.account_id_hex)
            }) =>
            {
                cgka_traits::TransportDeliveryPlane::AccountInbox
            }
            transport_nostr_peeler::KIND_MARMOT_GROUP_MESSAGE => {
                cgka_traits::TransportDeliveryPlane::Group
            }
            _ => continue,
        };
        client
            .ingest_received_delivery(cgka_traits::TransportDelivery {
                account_id: MemberId::new(hex::decode(&account.account_id_hex).unwrap()),
                group_id_hint: None,
                message: event.to_transport_message().unwrap(),
                received_at: cgka_traits::Timestamp(unix_now_seconds()),
                source: cgka_traits::TransportDeliverySource {
                    transport: cgka_traits::transport::TransportSource("nostr".into()),
                    plane,
                    endpoint: None,
                    subscription_id: None,
                    wire: None,
                },
            })
            .await
            .unwrap();
    }
    *cursor = events.len();
}

/// Delay only app projection, retaining every native effect from the owning runtime.
fn collect_effects(
    batch: &mut marmot_account::AccountDeviceEffects,
    mut effects: marmot_account::AccountDeviceEffects,
) {
    macro_rules! append {
        ($($field:ident),+ $(,)?) => {
            $(batch.$field.append(&mut effects.$field);)+
        };
    }
    append!(
        events,
        queued,
        pending_convergence,
        reports,
        fanout,
        failures,
        unresolved_publishes,
        unresolved_app_messages,
        failed_app_messages,
        published_app_messages,
        welcome_failures,
        superseded_intents,
        pending,
    );
    if effects.maintenance_disposition
        == cgka_traits::SendMaintenanceDisposition::PostJoinRotationPendingRetryable
    {
        batch.maintenance_disposition = effects.maintenance_disposition;
    }
}

/// Feed signed relay deliveries to this client's own runtime, delaying app projection.
async fn collect_new_deliveries(
    client: &mut AppClient,
    relay: &ScriptedPushRelayClient,
    cursor: &mut usize,
    batch: &mut marmot_account::AccountDeviceEffects,
) {
    let events = relay.published_events.lock().unwrap().clone();
    let account = client
        .app
        .account_home()
        .account(&client.state.label)
        .unwrap();
    for event in &events[*cursor..] {
        let plane = match event.kind {
            1059 if event.tags.iter().any(|tag| {
                tag.first().is_some_and(|v| v == "p") && tag.get(1) == Some(&account.account_id_hex)
            }) =>
            {
                cgka_traits::TransportDeliveryPlane::AccountInbox
            }
            transport_nostr_peeler::KIND_MARMOT_GROUP_MESSAGE => {
                cgka_traits::TransportDeliveryPlane::Group
            }
            _ => continue,
        };
        let ingested = client
            .runtime
            .ingest_delivery(cgka_traits::TransportDelivery {
                account_id: MemberId::new(hex::decode(&account.account_id_hex).unwrap()),
                group_id_hint: None,
                message: event.to_transport_message().unwrap(),
                received_at: cgka_traits::Timestamp(unix_now_seconds()),
                source: cgka_traits::TransportDeliverySource {
                    transport: cgka_traits::transport::TransportSource("nostr".into()),
                    plane,
                    endpoint: None,
                    subscription_id: None,
                    wire: None,
                },
            })
            .await
            .unwrap();
        collect_effects(batch, ingested.effects);
    }
    *cursor = events.len();
}

/// Real departure and disband settlement can outpace projection in a single
/// collected batch. Neither history projection nor cleanup may require live MLS.
#[tokio::test]
#[cfg(feature = "test-policy-overrides")]
async fn real_departure_then_disband_projects_after_mls_deletion() {
    use cgka_traits::engine::GroupStateChange;

    let dir = tempfile::tempdir().unwrap();
    let home = AccountHome::open(dir.path());
    home.create_account("alice").unwrap();
    let bob_account = home.create_account("bob").unwrap();
    let carol_account = home.create_account("carol").unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let app = app_at(&dir, relay.clone());
    for peer in [&bob_account, &carol_account] {
        remember_test_member_inbox(&app, &peer.account_id_hex, "wss://relay.example");
    }
    let mut bob = app.client("bob").await.unwrap();
    let mut carol = app.client("carol").await.unwrap();
    bob.publish_key_package().await.unwrap();
    carol.publish_key_package().await.unwrap();
    let mut alice = app.client("alice").await.unwrap();
    let group = alice
        .create_group(
            "collected closure",
            &[&bob_account.account_id_hex, &carol_account.account_id_hex],
        )
        .await
        .unwrap();
    let group_hex = hex::encode(group.as_slice());
    let mut alice_cursor = 0;
    let mut bob_cursor = 0;
    let mut carol_cursor = 0;
    deliver_new(&mut bob, &relay, &mut bob_cursor).await;
    deliver_new(&mut carol, &relay, &mut carol_cursor).await;
    bob.sync().await.unwrap();
    carol.sync().await.unwrap();
    bob.accept_group_invite(&group).unwrap();
    carol.accept_group_invite(&group).unwrap();
    assert!(installed_group_routes(&carol, &group) > 0);

    // Share genuine signed token events while both members still belong.
    for label in ["alice", "bob", "carol"] {
        app.set_native_push_enabled(label, true).unwrap();
    }
    let server = nostr::prelude::Keys::generate().public_key().to_hex();
    alice
        .upsert_and_share_push_registration(PushPlatform::Fcm, "alice-token", &server, None)
        .await
        .unwrap();
    bob.upsert_and_share_push_registration(PushPlatform::Fcm, "bob-token", &server, None)
        .await
        .unwrap();
    carol
        .upsert_and_share_push_registration(PushPlatform::Fcm, "carol-token", &server, None)
        .await
        .unwrap();
    deliver_new(&mut carol, &relay, &mut carol_cursor).await;
    carol.sync().await.unwrap();
    let tokens = app.group_push_tokens("carol", &group_hex).unwrap();
    assert!(
        tokens
            .iter()
            .any(|token| token.member_id_hex == bob_account.account_id_hex)
    );
    assert!(
        tokens
            .iter()
            .any(|token| token.member_id_hex != bob_account.account_id_hex)
    );

    bob.leave_group(&group).await.unwrap();
    deliver_new(&mut alice, &relay, &mut alice_cursor).await;
    alice.sync().await.unwrap();
    let delay = alice
        .runtime
        .scheduled_self_remove_auto_commit_delay_ms(&group)
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(delay + 1)).await;
    alice.retry_group_convergence(&group).await.unwrap();

    let mut batch = marmot_account::AccountDeviceEffects::default();
    collect_new_deliveries(&mut carol, &relay, &mut carol_cursor, &mut batch).await;
    collect_effects(
        &mut batch,
        carol.runtime.advance_convergence(&group).await.unwrap(),
    );
    let bob_member = MemberId::new(hex::decode(&bob_account.account_id_hex).unwrap());
    let departure = batch.events.iter().position(|event| matches!(event,
        GroupEvent::GroupStateChanged { change: GroupStateChange::MemberLeft { member }, .. }
            if member == &bob_member
    )).expect("actual retained departure must be applied before disband");
    assert!(batch.events.iter().any(|event| matches!(event,
        GroupEvent::GroupMemberLeavesRemoved { departed_members, .. }
            if departed_members.contains(&bob_member)
    )));

    alice.disband_group(&group).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            alice.retry_group_convergence(&group).await.unwrap();
            collect_new_deliveries(&mut carol, &relay, &mut carol_cursor, &mut batch).await;
            collect_effects(
                &mut batch,
                carol.runtime.advance_convergence(&group).await.unwrap(),
            );
            if carol
                .runtime
                .group_record(&group)
                .unwrap()
                .disbanded
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real disband must settle");
    let disband = batch
        .events
        .iter()
        .position(|event| {
            matches!(
                event,
                GroupEvent::GroupStateChanged {
                    change: GroupStateChange::GroupDisbanded,
                    ..
                }
            )
        })
        .expect("actual settlement must emit disband");
    assert!(departure < disband);
    assert!(
        carol
            .runtime
            .session()
            .canonical_group_membership(&group)
            .is_err(),
        "settlement must actually delete live MLS before app projection"
    );

    carol.observe_drained_session_events(&batch).await.unwrap();
    carol.refresh_group_routes().unwrap();
    carol
        .save_state_with_pending_local_group_deletion_frontier_clears()
        .unwrap();
    let assert_projection = |app: &MarmotApp, client: &AppClient| {
        let roster = client.group_roster_session(&group).unwrap();
        assert!(!roster.complete);
        assert!(!roster.members.is_empty());
        assert!(roster.members.iter().all(|member| member.devices.is_none()));
        let rows = app
            .timeline_messages_with_query(
                "carol",
                TimelineMessageQuery {
                    group_id_hex: Some(group_hex.clone()),
                    ..Default::default()
                },
            )
            .unwrap()
            .messages;
        assert_eq!(
            rows.iter()
                .filter(|row| row
                    .group_system
                    .as_ref()
                    .is_some_and(|event| event.system_type == "member_left"))
                .count(),
            1
        );
        assert_eq!(
            rows.iter()
                .filter(|row| row
                    .group_system
                    .as_ref()
                    .is_some_and(|event| event.system_type == "group_disbanded"))
                .count(),
            1
        );
        assert!(
            app.group_push_tokens("carol", &group_hex)
                .unwrap()
                .is_empty()
        );
        assert!(
            app.pending_push_registration_removals("carol")
                .unwrap()
                .iter()
                .any(|(group, _)| group == &group_hex)
        );
        assert_eq!(installed_group_routes(client, &group), 0);
        assert!(app.group("carol", &group_hex).unwrap().unwrap().disbanded);
    };
    assert_projection(&app, &carol);
    // Replaying the same genuine batch cannot duplicate history or reinstall routes.
    carol.observe_drained_session_events(&batch).await.unwrap();
    carol.refresh_group_routes().unwrap();
    assert_projection(&app, &carol);
    drop(carol);
    let reopened = app.client("carol").await.unwrap();
    assert_projection(&app, &reopened);
}

/// Document the real runtime's terminal boundary: late competing commits cannot
/// restore a removed copy through normal delivery or convergence retries. The
/// engine's retained-history rollback test is intentionally a lower-level proof.
#[tokio::test]
async fn late_competing_commit_cannot_restore_terminal_app_copy() {
    let dir = tempfile::tempdir().unwrap();
    let home = AccountHome::open(dir.path());
    let alice_account = home.create_account("alice").unwrap();
    let bob_account = home.create_account("bob").unwrap();
    let carol_account = home.create_account("carol").unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let app = app_at(&dir, relay.clone());
    for peer in [&bob_account, &carol_account] {
        remember_test_member_inbox(&app, &peer.account_id_hex, "wss://relay.example");
    }
    let mut bob = app.client("bob").await.unwrap();
    let mut carol = app.client("carol").await.unwrap();
    bob.publish_key_package().await.unwrap();
    carol.publish_key_package().await.unwrap();
    let mut alice = app.client("alice").await.unwrap();
    let group = alice
        .create_group(
            "terminal fork",
            &[&bob_account.account_id_hex, &carol_account.account_id_hex],
        )
        .await
        .unwrap();
    let group_hex = hex::encode(group.as_slice());
    let mut bob_cursor = 0;
    let mut carol_cursor = 0;
    deliver_new(&mut bob, &relay, &mut bob_cursor).await;
    deliver_new(&mut carol, &relay, &mut carol_cursor).await;
    bob.sync().await.unwrap();
    carol.sync().await.unwrap();
    bob.accept_group_invite(&group).unwrap();
    carol.accept_group_invite(&group).unwrap();
    alice
        .promote_admin(&group, &bob_account.account_id_hex)
        .await
        .unwrap();
    deliver_new(&mut bob, &relay, &mut bob_cursor).await;
    deliver_new(&mut carol, &relay, &mut carol_cursor).await;
    bob.retry_group_convergence(&group).await.unwrap();
    carol.retry_group_convergence(&group).await.unwrap();
    assert_eq!(
        alice.runtime.group_record(&group).unwrap().epoch,
        bob.runtime.group_record(&group).unwrap().epoch
    );
    assert_eq!(
        alice.runtime.group_record(&group).unwrap().epoch,
        carol.runtime.group_record(&group).unwrap().epoch
    );

    // Both branches are privileged. The lexicographically smaller admin would
    // win canonical selection if the late rename were admitted to convergence.
    let (mut renamer, mut remover) = if alice_account.account_id_hex < bob_account.account_id_hex {
        (alice, bob)
    } else {
        (bob, alice)
    };
    renamer.send(&group, b"read baseline").await.unwrap();
    deliver_new(&mut carol, &relay, &mut carol_cursor).await;
    carol.sync().await.unwrap();
    app.initialize_chat_read_state("carol", &group_hex).unwrap();
    // Read watermarks use whole seconds; advance past the baseline timestamp.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    renamer
        .send(&group, b"unread before removal")
        .await
        .unwrap();
    deliver_new(&mut carol, &relay, &mut carol_cursor).await;
    carol.sync().await.unwrap();
    let unread = || {
        app.account_unread_summary()
            .unwrap()
            .into_iter()
            .find(|summary| summary.account_id_hex == carol_account.account_id_hex)
            .unwrap()
            .unread_count
    };
    assert!(unread() > 0);
    assert!(can_send(&carol, &group));
    remover
        .remove_members(&group, &[&carol_account.account_id_hex])
        .await
        .unwrap();
    deliver_new(&mut carol, &relay, &mut carol_cursor).await;
    carol.retry_group_convergence(&group).await.unwrap();
    let assert_terminal = |client: &AppClient| {
        assert!(client.runtime.group_record(&group).unwrap().removed);
        assert_eq!(
            app.stored_group_self_membership("carol", &group_hex)
                .unwrap(),
            Some(SelfMembership::Removed)
        );
        assert!(!can_send(client, &group));
        assert!(!client.group_roster_session(&group).unwrap().complete);
        assert_eq!(unread(), 0);
        assert_eq!(installed_group_routes(client, &group), 0);
    };
    assert_terminal(&carol);
    let removed_epoch = carol.runtime.group_record(&group).unwrap().epoch;

    let rename = renamer
        .update_group_profile(&group, Some("late winning rename"), None)
        .await
        .unwrap();
    assert_eq!(
        rename.accept_disposition,
        cgka_traits::SendAcceptDisposition::Published
    );
    assert_eq!(
        renamer.runtime.group_record(&group).unwrap().epoch,
        removed_epoch,
        "the signed rename must compete at the same source epoch as removal"
    );
    assert_eq!(
        app.group(&renamer.state.label, &group_hex)
            .unwrap()
            .unwrap()
            .profile
            .name,
        "late winning rename",
        "the competing author really confirms its branch"
    );
    let mut late = marmot_account::AccountDeviceEffects::default();
    collect_new_deliveries(&mut carol, &relay, &mut carol_cursor, &mut late).await;
    collect_effects(
        &mut late,
        carol.runtime.advance_convergence(&group).await.unwrap(),
    );
    assert!(
        !late
            .events
            .iter()
            .any(|event| matches!(event, GroupEvent::LocalGroupCopyRestored { .. }))
    );
    carol.observe_drained_session_events(&late).await.unwrap();
    carol.retry_group_convergence(&group).await.unwrap();
    assert_terminal(&carol);
    assert_eq!(
        carol.runtime.group_record(&group).unwrap().epoch,
        removed_epoch
    );
    assert_ne!(
        app.chat_list_row("carol", &group_hex)
            .unwrap()
            .unwrap()
            .title,
        "late winning rename"
    );
    drop(carol);
    let reopened = app.client("carol").await.unwrap();
    assert_terminal(&reopened);
}

/// Attach the real published KeyPackage event needed by the Welcome wrapper.
async fn published_key_package(
    client: &mut AppClient,
    relay: &ScriptedPushRelayClient,
) -> cgka_traits::KeyPackage {
    let mut kp = client.publish_key_package().await.unwrap();
    let account = client
        .app
        .account_home()
        .account(&client.state.label)
        .unwrap();
    let event = relay
        .published_events
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|event| event.kind == 30443 && event.pubkey == account.account_id_hex)
        .unwrap()
        .clone();
    kp.source = Some(cgka_traits::engine::KeyPackageSource {
        event_id: MessageId::new(hex::decode(event.id).unwrap()),
    });
    kp
}

/// Read the same stored membership and live authority inputs used by conversation capture.
fn can_send(client: &AppClient, group: &GroupId) -> bool {
    let authority = client.runtime.session().group_authority(group).unwrap();
    let facts = authority.facts;
    ConversationAuthority {
        is_member: facts.is_member,
        self_membership: client
            .app
            .stored_group_self_membership(&client.state.label, &hex::encode(group.as_slice()))
            .unwrap()
            .unwrap(),
        is_admin: facts.is_admin,
        admin_count: facts.admin_count,
        pending_confirmation: false,
        leave_request_pending: client.runtime.session().leave_in_progress(group).unwrap(),
        lifecycle: authority.lifecycle.into(),
        unrecoverable: facts.unrecoverable,
        disbanding: authority.disbanding,
        disbanding_enabled: facts.disbanding_enabled,
        has_disbanding_blockers: facts.has_disbanding_blockers,
    }
    .capabilities()
    .can_send
}

/// A real sibling SelfRemove must not terminate the surviving device's projection.
#[tokio::test]
async fn sibling_selfremove_preserves_surviving_app_device_after_reopen() {
    sibling_selfremove_through_author_command(false).await;
}

/// A group command can confirm a retained sibling departure before its own
/// profile update; the command must observe those native effects as well.
#[tokio::test]
async fn own_group_command_observes_retained_sibling_departure() {
    sibling_selfremove_through_author_command(true).await;
}

async fn sibling_selfremove_through_author_command(profile_command: bool) {
    let dir = tempfile::tempdir().unwrap();
    let sibling_dir = tempfile::tempdir().unwrap();
    let home = AccountHome::open(dir.path());
    home.create_account("alice").unwrap();
    let bob = home.create_account("bob").unwrap();
    let secret = home
        .load_signing_keys("bob")
        .unwrap()
        .secret_key()
        .to_secret_hex();
    AccountHome::open(sibling_dir.path())
        .import_account("bob", &secret)
        .unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let app = app_at(&dir, relay.clone());
    let sibling_app = app_at(&sibling_dir, relay.clone());
    remember_test_member_inbox(&app, &bob.account_id_hex, "wss://relay.example");
    let mut bob_client = app.client("bob").await.unwrap();
    bob_client.publish_key_package().await.unwrap();
    bob_client.sync().await.unwrap();
    let mut alice = app.client("alice").await.unwrap();
    let mut alice_cursor = 0;
    let mut bob_cursor = 0;
    let group = alice
        .create_group("sibling departure", &[&bob.account_id_hex])
        .await
        .unwrap();
    deliver_new(&mut bob_client, &relay, &mut bob_cursor).await;
    bob_client.sync().await.unwrap();
    let pending_roster = bob_client.group_roster_session(&group).unwrap();
    assert!(pending_roster.group_record.pending_confirmation);
    assert!(
        pending_roster.complete,
        "UI acceptance is separate from MLS admission"
    );
    bob_client.accept_group_invite(&group).unwrap();
    let mut sibling = sibling_app.client("bob").await.unwrap();
    sibling.sync().await.unwrap();
    let mut sibling_cursor = relay.published_events.lock().unwrap().len();
    let kp = published_key_package(&mut sibling, &relay).await;
    let effects = alice
        .runtime
        .send(SendIntent::Invite {
            group_id: group.clone(),
            key_packages: vec![kp],
            initial_admins: vec![],
        })
        .await
        .unwrap();
    alice
        .observe_drained_session_events(&effects)
        .await
        .unwrap();
    deliver_new(&mut sibling, &relay, &mut sibling_cursor).await;
    sibling.sync().await.unwrap();
    sibling.accept_group_invite(&group).unwrap();
    deliver_new(&mut bob_client, &relay, &mut bob_cursor).await;
    bob_client.sync().await.unwrap();
    bob_client.retry_group_convergence(&group).await.unwrap();
    let group_hex = hex::encode(group.as_slice());
    let surviving_leaf = bob_client.runtime.own_leaf_index(&group).unwrap();
    let departing_leaf = sibling.runtime.own_leaf_index(&group).unwrap();
    assert_ne!(surviving_leaf, departing_leaf);
    for leaf in [surviving_leaf, departing_leaf] {
        let mut token = drained_seam_push_token(&group_hex, &bob.account_id_hex, leaf);
        token.token_fingerprint = "same-device-token".into();
        app.upsert_group_push_token("bob", &token).unwrap();
        app.upsert_group_push_token("alice", &token).unwrap();
    }
    relay.fail_publishes_as_unavailable();
    bob_client
        .send(&group, b"retained sibling send")
        .await
        .unwrap();
    let mut fanout = bob_client
        .runtime
        .session()
        .outbound_fanouts_for_group(&group)
        .unwrap()
        .remove(0);
    for index in fanout.outstanding_target_indexes() {
        let failure = fanout.target_failure(index).unwrap().clone();
        for _ in 0..6 {
            fanout
                .mark_attempt_started_at(index, notifications::unix_now_ms().try_into().unwrap())
                .unwrap();
            fanout
                .record_target_failure(index, failure.clone())
                .unwrap();
        }
    }
    bob_client
        .runtime
        .session()
        .put_outbound_fanout(&fanout)
        .unwrap();
    relay.allow_publishes();
    sibling
        .app
        .upsert_push_registration(
            "bob",
            PushPlatform::Fcm,
            "departing-device-token",
            &nostr::prelude::Keys::generate().public_key().to_hex(),
            None,
        )
        .unwrap();
    let peer_token = drained_seam_push_token(
        &group_hex,
        &home.account("alice").unwrap().account_id_hex,
        alice.runtime.own_leaf_index(&group).unwrap(),
    );
    sibling_app
        .upsert_group_push_token("bob", &peer_token)
        .unwrap();
    sibling.leave_group(&group).await.unwrap();
    deliver_new(&mut alice, &relay, &mut alice_cursor).await;
    alice.sync().await.unwrap();
    let delay = alice
        .runtime
        .scheduled_self_remove_auto_commit_delay_ms(&group)
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(delay + 1)).await;
    if profile_command {
        alice
            .update_group_profile(&group, Some("after departure"), None)
            .await
            .unwrap();
    } else {
        alice.retry_group_convergence(&group).await.unwrap();
    }
    let author_tokens = app.group_push_tokens("alice", &group_hex).unwrap();
    assert_eq!(
        author_tokens.len(),
        1,
        "the public command must observe the author's leaf cleanup"
    );
    assert_eq!(author_tokens[0].leaf_index, surviving_leaf);
    deliver_new(&mut bob_client, &relay, &mut bob_cursor).await;
    bob_client.sync().await.unwrap();
    bob_client.retry_group_convergence(&group).await.unwrap();
    assert_eq!(
        app.stored_group_self_membership("bob", &group_hex).unwrap(),
        Some(SelfMembership::Member)
    );
    assert!(can_send(&bob_client, &group));
    let tokens = app.group_push_tokens("bob", &group_hex).unwrap();
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].leaf_index, surviving_leaf);
    let assert_held = |app: &MarmotApp| {
        let rows = app
            .timeline_messages_with_query(
                "bob",
                TimelineMessageQuery {
                    group_id_hex: Some(group_hex.clone()),
                    ..Default::default()
                },
            )
            .unwrap()
            .messages;
        let held = rows
            .iter()
            .find(|row| row.plaintext == "retained sibling send")
            .unwrap();
        assert_eq!(held.invalidation_status, None);
        assert!(!rows.iter().any(|row| {
            row.group_system
                .as_ref()
                .is_some_and(|event| event.system_type == "member_left")
        }));
    };
    deliver_new(&mut sibling, &relay, &mut sibling_cursor).await;
    sibling.sync().await.unwrap();
    sibling.retry_group_convergence(&group).await.unwrap();
    assert!(sibling.runtime.group_record(&group).unwrap().removed);
    assert!(!can_send(&sibling, &group));
    assert_eq!(
        sibling
            .app
            .stored_group_self_membership("bob", &group_hex)
            .unwrap(),
        Some(SelfMembership::Left)
    );
    assert!(!sibling.runtime.has_queued_outbound_intents(&group).unwrap());
    assert!(
        sibling
            .app
            .pending_push_registration_removals("bob")
            .unwrap()
            .is_empty(),
        "termination must not enqueue an unsendable removal rumor"
    );
    assert_eq!(
        sibling_app.group_push_tokens("bob", &group_hex).unwrap(),
        vec![peer_token]
    );
    if !profile_command {
        // The durable terminal marker survives an app-projection write failure.
        // Eager open stays usable and retains the repair for its bounded retry.
        drop(sibling);
        let path = sibling_app.account_storage_path("bob");
        let keys = sibling_app.account_home().load_signing_keys("bob").unwrap();
        let key = sibling_app
            .sqlcipher_key("bob", &keys, &path, SqlcipherDatabaseKind::Session)
            .unwrap();
        let connection = rusqlite::Connection::open(path).unwrap();
        storage_sqlite::open_hardened_sqlcipher(
            &connection,
            &key,
            storage_sqlite::SqlCipherHardening::cipher_only(),
        )
        .unwrap();
        connection.execute_batch("CREATE TRIGGER fail_terminal_repair BEFORE UPDATE OF self_membership ON account_groups
            BEGIN SELECT RAISE(FAIL, 'injected terminal repair failure'); END;").unwrap();
        let mut recovered = sibling_app
            .client("bob")
            .await
            .expect("terminal repair does not abort eager open");
        assert!(recovered.has_pending_runtime_group_subscription_refresh());
        connection
            .execute_batch("DROP TRIGGER fail_terminal_repair")
            .unwrap();
        recovered
            .retry_pending_runtime_group_subscription_refresh()
            .await
            .unwrap();
        assert!(!recovered.has_pending_runtime_group_subscription_refresh());
    }
    assert_held(&app);
    assert!(
        !bob_client
            .runtime
            .session()
            .outbound_fanouts_for_group(&group)
            .unwrap()
            .is_empty()
    );
    let registration = app
        .upsert_push_registration(
            "bob",
            PushPlatform::Fcm,
            "surviving-device-token",
            &nostr::prelude::Keys::generate().public_key().to_hex(),
            None,
        )
        .unwrap();
    assert!(
        app.pending_push_registration_shares(
            "bob",
            &registration.token_fingerprint,
            registration.updated_at_ms
        )
        .unwrap()
        .contains(&group_hex)
    );
    let stale = drained_seam_push_token(&group_hex, &bob.account_id_hex, departing_leaf);
    app.upsert_group_push_token("bob", &stale).unwrap();
    assert_eq!(app.group_push_tokens("bob", &group_hex).unwrap().len(), 2);
    drop(bob_client);
    drop(alice);
    drop(app);
    let reopened = app_at(&dir, relay.clone());
    let path = reopened.account_storage_path("bob");
    let keys = reopened.account_home().load_signing_keys("bob").unwrap();
    let key = reopened
        .sqlcipher_key("bob", &keys, &path, SqlcipherDatabaseKind::Session)
        .unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    storage_sqlite::open_hardened_sqlcipher(
        &connection,
        &key,
        storage_sqlite::SqlCipherHardening::cipher_only(),
    )
    .unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TRIGGER fail_reopen_leaf_cleanup BEFORE DELETE ON group_push_tokens
        WHEN OLD.leaf_index = {departing_leaf}
        BEGIN SELECT RAISE(FAIL, 'injected leaf cleanup failure'); END;"
        ))
        .unwrap();
    let mut client = if profile_command {
        let plane = MarmotRelayPlane::new(None, relay);
        let mut client = reopened
            .local_client_with_relay_plane_and_hydration("bob", &plane, None, true, None)
            .await
            .unwrap();
        crate::runtime::account_worker::drain_deferred_hydration(&mut client)
            .await
            .unwrap();
        client
    } else {
        reopened.client("bob").await.unwrap()
    };
    assert_eq!(
        reopened.group_push_tokens("bob", &group_hex).unwrap().len(),
        2
    );
    assert!(client.has_pending_runtime_group_subscription_refresh());
    connection
        .execute_batch("DROP TRIGGER fail_reopen_leaf_cleanup")
        .unwrap();
    client
        .retry_pending_runtime_group_subscription_refresh()
        .await
        .unwrap();
    assert!(client.pending_push_leaf_reconciliations.is_empty());
    assert!(can_send(&client, &group));
    assert!(
        !client
            .runtime
            .session()
            .outbound_fanouts_for_group(&group)
            .unwrap()
            .is_empty()
    );
    assert!(
        reopened
            .pending_push_registration_shares(
                "bob",
                &registration.token_fingerprint,
                registration.updated_at_ms
            )
            .unwrap()
            .contains(&group_hex)
    );
    let repaired = reopened.group_push_tokens("bob", &group_hex).unwrap();
    assert_eq!(repaired.len(), 1, "hydration repairs missed leaf cleanup");
    assert_eq!(repaired[0].leaf_index, surviving_leaf);
    assert_held(&reopened);
}

/// Projection contract only: native restoration re-enables the existing unread
/// aggregate without inserting an invitation or changing the read watermark.
#[tokio::test]
async fn native_restoration_restores_projection_unread_without_invitation() {
    let dir = tempfile::tempdir().unwrap();
    let account = AccountHome::open(dir.path())
        .create_account("alice")
        .unwrap();
    let app = app_at(&dir, Arc::new(ScriptedPushRelayClient::default()));
    let mut client = app.client("alice").await.unwrap();
    let group = client.create_group("unread contract", &[]).await.unwrap();
    let group_hex = hex::encode(group.as_slice());
    app.initialize_chat_read_state("alice", &group_hex).unwrap();
    // An explicit projection fixture; authenticity/reorg reachability belongs
    // to separate engine and real-runtime tests, not this synthetic event seam.
    app.record_account_app_event(
        "alice",
        &AppMessageProjection {
            authority: None,
            message_id_hex: "unread-projection-fixture".into(),
            source_message_id_hex: None,
            direction: "received".into(),
            group_id_hex: group_hex.clone(),
            sender: nostr::prelude::Keys::generate().public_key().to_hex(),
            sender_device: Default::default(),
            plaintext: "already retained unread".into(),
            kind: MARMOT_APP_EVENT_KIND_CHAT,
            tags: Vec::new(),
            source_epoch: Some(1),
            retention: None,
            recorded_at: Some(unix_now_seconds() + 60),
            origin_commit_id: None,
            moderation_grant: false,
        },
    )
    .unwrap();
    let unread = || {
        app.account_unread_summary()
            .unwrap()
            .into_iter()
            .find(|summary| summary.account_id_hex == account.account_id_hex)
            .unwrap()
            .unread_count
    };
    assert_eq!(unread(), 1);
    let termination = marmot_account::AccountDeviceEffects {
        events: vec![GroupEvent::LocalGroupCopyTerminated {
            group_id: group.clone(),
            voluntary: false,
        }],
        ..Default::default()
    };
    client
        .observe_drained_session_events(&termination)
        .await
        .unwrap();
    assert_eq!(unread(), 0);
    let restoration = marmot_account::AccountDeviceEffects {
        events: vec![GroupEvent::LocalGroupCopyRestored { group_id: group }],
        ..Default::default()
    };
    client
        .observe_drained_session_events(&restoration)
        .await
        .unwrap();
    assert_eq!(unread(), 1);
    client
        .observe_drained_session_events(&restoration)
        .await
        .unwrap();
    assert_eq!(unread(), 1);
    let rows = app
        .timeline_messages_with_query(
            "alice",
            TimelineMessageQuery {
                group_id_hex: Some(group_hex),
                ..Default::default()
            },
        )
        .unwrap()
        .messages;
    assert_eq!(rows.len(), 1);
    assert!(rows[0].group_system.is_none());
    drop(client);
    let _reopened = app.client("alice").await.unwrap();
    assert_eq!(unread(), 1);
}

/// Explicit projection-seam regression: a termination consumed after the engine
/// already restored its tree must still withdraw purged sends. This is not a
/// scheduler-driven rollback test; the engine retained-history test owns that proof.
#[tokio::test]
async fn termination_then_restoration_preserves_failed_sends_and_archive() {
    #[derive(Clone, Copy)]
    enum Observer {
        Drained,
        Retry,
        Command,
        Maintenance,
    }
    for (archived, seam, failed) in [
        (false, Observer::Drained, false),
        (true, Observer::Drained, false),
        (false, Observer::Retry, false),
        (true, Observer::Retry, false),
        (false, Observer::Command, false),
        (true, Observer::Command, false),
        (false, Observer::Maintenance, false),
        (true, Observer::Maintenance, false),
        (true, Observer::Drained, true),
        (true, Observer::Retry, true),
        (true, Observer::Command, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let home = AccountHome::open(dir.path());
        let account = home.create_account("alice").unwrap();
        let app = app_at(&dir, Arc::new(ScriptedPushRelayClient::default()));
        let mut client = app.client("alice").await.unwrap();
        let group = client.create_group("effect batch", &[]).await.unwrap();
        let group_hex = hex::encode(group.as_slice());
        client.set_group_archived(&group, archived).unwrap();
        let peer = nostr::prelude::Keys::generate().public_key().to_hex();
        let peer_token = drained_seam_push_token(&group_hex, &peer, 7);
        app.upsert_group_push_token("alice", &peer_token).unwrap();
        let storage = app.account_storage("alice").unwrap();
        let mut withdrawn = peer_token.clone();
        withdrawn.leaf_index = 8;
        storage
            .apply_group_push_token_tombstone(
                &group_hex,
                &peer,
                8,
                withdrawn.platform.platform_byte(),
                &withdrawn.server_pubkey_hex,
                withdrawn.owner_ts + 1,
                "peer-withdrawal",
                500,
            )
            .unwrap();

        app.record_account_app_event(
            "alice",
            &AppMessageProjection {
                authority: None,
                message_id_hex: "held".into(),
                source_message_id_hex: None,
                direction: "sent".into(),
                group_id_hex: group_hex.clone(),
                sender: account.account_id_hex.clone(),
                sender_device: Default::default(),
                plaintext: "held send".into(),
                kind: MARMOT_APP_EVENT_KIND_CHAT,
                tags: Vec::new(),
                source_epoch: Some(1),
                retention: None,
                recorded_at: Some(11),
                origin_commit_id: None,
                moderation_grant: false,
            },
        )
        .unwrap();
        let mut effects = marmot_account::AccountDeviceEffects {
            events: vec![
                GroupEvent::LocalGroupCopyTerminated {
                    group_id: group.clone(),
                    voluntary: false,
                },
                GroupEvent::LocalGroupCopyRestored {
                    group_id: group.clone(),
                },
            ],
            ..Default::default()
        };
        if failed {
            effects.failures.push(marmot_account::PublishFailure {
                message_id: MessageId::new(vec![0xef; 32]),
                reason: "unrelated publication failed".into(),
            });
        }
        let result = match seam {
            Observer::Drained => client
                .observe_drained_session_events(&effects)
                .await
                .map(|_| ()),
            Observer::Retry => client
                .observe_convergence_retry_effects(&group, &effects)
                .await
                .map(|_| ()),
            Observer::Command => {
                client
                    .observe_recovery_evidence_then_fail_if_publish_failed(&effects)
                    .await
            }
            Observer::Maintenance => client
                .finish_maintenance_effects(&effects)
                .await
                .map(|_| ()),
        };
        assert_eq!(result.is_err(), failed);
        assert_eq!(
            app.group_push_tokens("alice", &group_hex).unwrap(),
            vec![peer_token]
        );
        assert!(
            !storage
                .apply_group_push_token(&account_group_push_token_from_app(&withdrawn))
                .unwrap(),
            "termination/restoration must preserve peers' anti-resurrection evidence"
        );

        assert_eq!(
            app.stored_group_self_membership("alice", &group_hex)
                .unwrap(),
            Some(SelfMembership::Member)
        );
        assert_eq!(
            app.group("alice", &group_hex).unwrap().unwrap().archived,
            archived
        );
        let row = app
            .timeline_messages_with_query(
                "alice",
                TimelineMessageQuery {
                    group_id_hex: Some(group_hex.clone()),
                    ..Default::default()
                },
            )
            .unwrap()
            .messages
            .into_iter()
            .find(|row| row.message_id_hex == "held")
            .unwrap();
        assert_eq!(
            row.invalidation_status.as_deref(),
            Some("local_publish_failed")
        );
        // Replay stays absolute and must not create any kind-1210 invitation.
        // The unrelated publication is not retried by this native-event replay.
        effects.failures.clear();
        client
            .observe_drained_session_events(&effects)
            .await
            .unwrap();
        assert!(
            app.timeline_messages_with_query(
                "alice",
                TimelineMessageQuery {
                    group_id_hex: Some(group_hex),
                    ..Default::default()
                }
            )
            .unwrap()
            .messages
            .iter()
            .all(|row| row.group_system.is_none())
        );
    }
}

/// The maintenance boundary observes each native recovery transition once,
/// even though summary construction and membership projection are separate.
#[cfg(feature = "product-analytics-export")]
#[tokio::test]
async fn maintenance_counts_one_recovery_transition() {
    let dir = tempfile::tempdir().unwrap();
    AccountHome::open(dir.path())
        .create_account("alice")
        .unwrap();
    let mut app = app_at(&dir, Arc::new(ScriptedPushRelayClient::default()));
    app.product_analytics = crate::product_analytics::test_product_collector();
    let runtime = app.runtime();
    runtime.set_usage_diagnostics_consent(true).unwrap();
    let mut client = app.client("alice").await.unwrap();
    let group = client
        .create_group("maintenance observation", &[])
        .await
        .unwrap();
    let effects = marmot_account::AccountDeviceEffects {
        events: vec![GroupEvent::PendingCommitRecovered {
            group_id: group,
            recovered_epoch: cgka_traits::EpochId(1),
        }],
        ..Default::default()
    };
    client.finish_maintenance_effects(&effects).await.unwrap();
    let payloads = app.product_analytics.test_payloads();
    let transitions: Vec<_> = payloads
        .iter()
        .filter(|event| {
            event["eventName"] == "mdk_recovery_summary"
                && event["props"]["operation"] == "pending_commit"
        })
        .collect();
    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0]["props"]["count_bucket"], "1");
}

/// A storage failure after the first native event retains the whole drained
/// batch. Retrying projects the tail without changing the primary publish result.
#[tokio::test]
async fn partial_native_projection_retries_without_republishing() {
    for (seam, failed_publish) in [
        (0, false),
        (0, true),
        (1, false),
        (1, true),
        (2, false),
        (2, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let account = AccountHome::open(dir.path())
            .create_account("alice")
            .unwrap();
        let relay = Arc::new(ScriptedPushRelayClient::default());
        let app = app_at(&dir, relay.clone());
        let mut client = app.client("alice").await.unwrap();
        client.prepare_transport().await.unwrap();
        let group = client.create_group("projection retry", &[]).await.unwrap();
        let group_hex = hex::encode(group.as_slice());
        client.take_pending_applied_sync_summary();
        app.record_account_app_event(
            "alice",
            &AppMessageProjection {
                authority: None,
                message_id_hex: "held-prefix".into(),
                source_message_id_hex: None,
                direction: "sent".into(),
                group_id_hex: group_hex.clone(),
                sender: account.account_id_hex.clone(),
                sender_device: Default::default(),
                plaintext: "pending before storage fault".into(),
                kind: MARMOT_APP_EVENT_KIND_CHAT,
                tags: Vec::new(),
                source_epoch: Some(1),
                retention: None,
                recorded_at: Some(11),
                origin_commit_id: None,
                moderation_grant: false,
            },
        )
        .unwrap();

        let path = app.account_storage_path("alice");
        let keys = app.account_home().load_signing_keys("alice").unwrap();
        let key = app
            .sqlcipher_key("alice", &keys, &path, SqlcipherDatabaseKind::Session)
            .unwrap();
        let connection = rusqlite::Connection::open(path).unwrap();
        storage_sqlite::open_hardened_sqlcipher(
            &connection,
            &key,
            storage_sqlite::SqlCipherHardening::cipher_only(),
        )
        .unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_restoration BEFORE UPDATE OF self_membership ON account_groups
             WHEN OLD.self_membership = 'removed' AND NEW.self_membership = 'member'
             BEGIN SELECT RAISE(FAIL, 'injected restoration failure'); END;",
            )
            .unwrap();
        let mut effects = marmot_account::AccountDeviceEffects {
            events: vec![
                GroupEvent::LocalGroupCopyTerminated {
                    group_id: group.clone(),
                    voluntary: false,
                },
                GroupEvent::LocalGroupCopyRestored {
                    group_id: group.clone(),
                },
                GroupEvent::GroupStateChanged {
                    group_id: group.clone(),
                    epoch: cgka_traits::EpochId(1),
                    actor: Some(MemberId::new(hex::decode(&account.account_id_hex).unwrap())),
                    change: cgka_traits::GroupStateChange::GroupRenamed {
                        name: "committed tail".into(),
                        previous_name: None,
                    },
                    origin_commit_id: Some(MessageId::new(vec![0xe3; 32])),
                },
            ],
            ..Default::default()
        };
        // Canonical app payloads model already authenticated native deliveries.
        // Push protocol traffic must stay hidden even when the next event fails.
        let message_events = [MARMOT_APP_EVENT_KIND_CHAT, 447, 448, 449]
            .into_iter()
            .map(|kind| GroupEvent::MessageReceived {
                authority: None,
                group_id: group.clone(),
                message_id: MessageId::new(vec![kind as u8; 32]),
                sender: MemberId::new(hex::decode(&account.account_id_hex).unwrap()),
                sender_device: Default::default(),
                epoch: client.runtime.group_record(&group).unwrap().epoch,
                payload: cgka_traits::app_event::MarmotAppEvent::new(
                    &account.account_id_hex,
                    12,
                    kind,
                    Vec::new(),
                    "projection prefix",
                )
                .encode()
                .unwrap(),
                retention: None,
                encrypted_media_secret: None,
            })
            .collect::<Vec<_>>();
        effects.events.splice(0..0, message_events);
        if failed_publish {
            effects.failures.push(marmot_account::PublishFailure {
                message_id: MessageId::new(vec![0xe4; 32]),
                reason: "primary failure".into(),
            });
        }
        let publications = relay.published_events.lock().unwrap().len();
        let result = match seam {
            0 => {
                client
                    .observe_recovery_evidence_then_fail_if_publish_failed(&effects)
                    .await
            }
            1 => client
                .observe_drained_session_events(&effects)
                .await
                .map(|_| ()),
            _ => client
                .observe_convergence_retry_effects(&group, &effects)
                .await
                .map(|_| ()),
        };
        if failed_publish {
            assert!(
                matches!(result, Err(AppError::Publish(reason)) if reason == "primary failure")
            );
        } else if seam == 0 {
            result.unwrap();
        } else {
            assert!(
                result.is_err(),
                "ordinary observers still report their storage failure"
            );
        }
        assert_eq!(
            app.stored_group_self_membership("alice", &group_hex)
                .unwrap(),
            Some(SelfMembership::Removed)
        );
        assert!(client.has_pending_runtime_group_subscription_refresh());
        assert_eq!(client.pending_applied_effects.len(), 1);
        let native_only = seam == 0 && !failed_publish;
        let prefix = client.take_pending_applied_sync_summary();
        assert!(
            prefix
                .messages
                .iter()
                .all(|message| !notifications::is_push_gossip_kind(message.kind))
        );
        assert!(
            prefix.messages.is_empty(),
            "outbox notifications wait for the failed batch's acknowledgement checkpoint"
        );
        let cleanup_count = |summary: &crate::SyncSummary| {
            summary
                .projection_updates
                .iter()
                .flat_map(|update| &update.timeline_messages)
                .filter(|row| {
                    row.message_id_hex == "held-prefix"
                        && row.invalidation_status.as_deref() == Some("local_publish_failed")
                })
                .count()
        };
        assert_eq!(
            cleanup_count(&prefix),
            usize::from(native_only),
            "native-only prefixes are immediately visible; outbox batches wait for acknowledgement"
        );
        assert!(
            client
                .retry_pending_runtime_group_subscription_refresh()
                .await
                .is_err()
        );
        assert_eq!(client.pending_applied_effects.len(), 1);
        assert!(
            client
                .take_pending_applied_sync_summary()
                .messages
                .iter()
                .all(|message| !notifications::is_push_gossip_kind(message.kind))
        );
        connection
            .execute_batch("DROP TRIGGER fail_restoration")
            .unwrap();
        client
            .retry_pending_runtime_group_subscription_refresh()
            .await
            .unwrap();
        assert_eq!(
            app.stored_group_self_membership("alice", &group_hex)
                .unwrap(),
            Some(SelfMembership::Member)
        );
        assert!(!client.has_pending_runtime_group_subscription_refresh());
        assert!(client.pending_applied_effects.is_empty());
        let summary = client.take_pending_applied_sync_summary();
        assert!(
            summary
                .messages
                .iter()
                .all(|message| !notifications::is_push_gossip_kind(message.kind))
        );
        assert_eq!(
            summary
                .messages
                .iter()
                .filter(|message| message.kind == MARMOT_APP_EVENT_KIND_CHAT)
                .count(),
            usize::from(!native_only),
            "repair emits the deferred chat once when the seam observes messages"
        );
        assert_eq!(
            cleanup_count(&prefix) + cleanup_count(&summary),
            1,
            "the committed invalidation survives the failed tail and is delivered once"
        );
        if failed_publish {
            assert_eq!(
                summary
                    .projection_updates
                    .iter()
                    .flat_map(|u| &u.timeline_messages)
                    .filter(|row| row.group_system.is_some())
                    .count(),
                1
            );
        }
        assert_eq!(
            relay.published_events.lock().unwrap().len(),
            publications,
            "projection retry must not republish"
        );
        client
            .retry_pending_runtime_group_subscription_refresh()
            .await
            .unwrap();
        assert!(
            client
                .take_pending_applied_sync_summary()
                .projection_updates
                .is_empty()
        );
    }
}

/// Failure in the row-synthesis tail must retain the earlier successful write
/// and retry the remaining row with its original observation timestamp.
#[tokio::test]
async fn committed_activity_tail_retries_after_storage_failure() {
    let dir = tempfile::tempdir().unwrap();
    let account = AccountHome::open(dir.path())
        .create_account("alice")
        .unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let app = app_at(&dir, relay.clone());
    let mut client = app.client("alice").await.unwrap();
    client.prepare_transport().await.unwrap();
    let group = client.create_group("row retry", &[]).await.unwrap();
    client.take_pending_applied_sync_summary();
    let path = app.account_storage_path("alice");
    let keys = app.account_home().load_signing_keys("alice").unwrap();
    let key = app
        .sqlcipher_key("alice", &keys, &path, SqlcipherDatabaseKind::Session)
        .unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    storage_sqlite::open_hardened_sqlcipher(
        &connection,
        &key,
        storage_sqlite::SqlCipherHardening::cipher_only(),
    )
    .unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_activity_tail BEFORE INSERT ON app_events
         WHEN NEW.kind = 1210 AND (SELECT COUNT(*) FROM app_events WHERE kind = 1210) > 0
         BEGIN SELECT RAISE(FAIL, 'injected activity tail failure'); END;",
        )
        .unwrap();
    let effects = marmot_account::AccountDeviceEffects {
        events: (2..=3)
            .map(|epoch| GroupEvent::GroupStateChanged {
                group_id: group.clone(),
                epoch: cgka_traits::EpochId(epoch),
                actor: Some(MemberId::new(hex::decode(&account.account_id_hex).unwrap())),
                change: cgka_traits::GroupStateChange::GroupRenamed {
                    name: format!("rename {epoch}"),
                    previous_name: None,
                },
                origin_commit_id: Some(MessageId::new(vec![epoch as u8; 32])),
            })
            .collect(),
        ..Default::default()
    };
    let publications = relay.published_events.lock().unwrap().len();
    client
        .observe_send_applied_effects(&effects)
        .await
        .expect_err("the injected activity tail is retained for retry");
    assert!(client.has_pending_runtime_group_subscription_refresh());
    assert_eq!(client.pending_applied_effects.len(), 1);
    let count = || {
        connection
            .query_row(
                "SELECT COUNT(*) FROM app_events WHERE kind = 1210",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    assert_eq!(count(), 1, "first row committed before failure");
    connection
        .execute_batch("DROP TRIGGER fail_activity_tail")
        .unwrap();
    client
        .retry_pending_runtime_group_subscription_refresh()
        .await
        .unwrap();
    assert_eq!(count(), 2);
    assert!(client.pending_applied_effects.is_empty());
    let summary = client.take_pending_applied_sync_summary();
    let ids = summary
        .projection_updates
        .iter()
        .flat_map(|u| &u.timeline_messages)
        .filter(|row| row.group_system.is_some())
        .map(|row| &row.message_id_hex)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids.len(), 2, "subscribers receive both committed rows");
    assert_eq!(relay.published_events.lock().unwrap().len(), publications);
    client.observe_send_applied_effects(&effects).await.unwrap();
    assert!(
        !client.has_pending_runtime_group_subscription_refresh(),
        "successful ordinary activity must not arm an unnecessary route rebuild"
    );
}

/// Independent hydration repairs must not serialize unrelated groups behind a
/// persistent storage fault or prevent the account's route refresh from finishing.
#[tokio::test]
async fn failed_push_sweep_does_not_block_other_groups_or_routes() {
    let dir = tempfile::tempdir().unwrap();
    let account = AccountHome::open(dir.path())
        .create_account("alice")
        .unwrap();
    let app = app_at(&dir, Arc::new(ScriptedPushRelayClient::default()));
    let mut client = app.client("alice").await.unwrap();
    client.prepare_transport().await.unwrap();
    let first = client.create_group("first repair", &[]).await.unwrap();
    let second = client.create_group("second repair", &[]).await.unwrap();
    for group in [&first, &second] {
        app.upsert_group_push_token(
            "alice",
            &drained_seam_push_token(&hex::encode(group.as_slice()), &account.account_id_hex, 777),
        )
        .unwrap();
        client
            .pending_push_leaf_reconciliations
            .insert(group.clone());
    }
    // Fail the first iteration deterministically, independently of HashSet order.
    let blocked = client
        .pending_push_leaf_reconciliations
        .iter()
        .next()
        .unwrap()
        .clone();
    let healthy = if blocked == first { second } else { first };
    let blocked_hex = hex::encode(blocked.as_slice());
    let healthy_hex = hex::encode(healthy.as_slice());
    let path = app.account_storage_path("alice");
    let keys = app.account_home().load_signing_keys("alice").unwrap();
    let key = app
        .sqlcipher_key("alice", &keys, &path, SqlcipherDatabaseKind::Session)
        .unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    storage_sqlite::open_hardened_sqlcipher(
        &connection,
        &key,
        storage_sqlite::SqlCipherHardening::cipher_only(),
    )
    .unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TRIGGER fail_one_leaf_sweep BEFORE DELETE ON group_push_tokens
        WHEN OLD.group_id_hex = '{blocked_hex}'
        BEGIN SELECT RAISE(FAIL, 'injected group-specific cleanup failure'); END;"
        ))
        .unwrap();
    assert!(
        client
            .retry_pending_runtime_group_subscription_refresh()
            .await
            .is_err()
    );
    assert_eq!(
        app.group_push_tokens("alice", &blocked_hex).unwrap().len(),
        1
    );
    assert!(
        app.group_push_tokens("alice", &healthy_hex)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        client.pending_push_leaf_reconciliations,
        HashSet::from([blocked])
    );
    assert!(
        !client.pending_runtime_group_subscription_refresh,
        "the independent route refresh must finish despite the token cleanup error"
    );
    assert!(client.has_pending_runtime_group_subscription_refresh());
    connection
        .execute_batch("DROP TRIGGER fail_one_leaf_sweep")
        .unwrap();
    assert!(
        !client
            .retry_pending_runtime_group_subscription_refresh()
            .await
            .unwrap()
    );
    assert!(
        app.group_push_tokens("alice", &blocked_hex)
            .unwrap()
            .is_empty()
    );
}

/// A blocked one-shot activity row survives the managed worker's live receive
/// boundary and is delivered without another engine event or client reopen.
#[tokio::test]
#[cfg(feature = "test-policy-overrides")]
async fn managed_worker_keeps_activity_retry_while_live_delivery_waits() {
    assert_managed_worker_retains_activity(false).await;
}

#[tokio::test]
async fn managed_worker_keeps_activity_after_late_checkpoint_failure() {
    assert_managed_worker_retains_activity(true).await;
}

async fn assert_managed_worker_retains_activity(late_checkpoint: bool) {
    let dir = tempfile::tempdir().unwrap();
    let home = AccountHome::open(dir.path());
    let alice_account = home.create_account("alice").unwrap();
    let bob_account = home.create_account("bob").unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let app = app_at(&dir, relay.clone());
    remember_test_member_inbox(&app, &alice_account.account_id_hex, "wss://relay.example");
    remember_test_member_inbox(&app, &bob_account.account_id_hex, "wss://relay.example");
    let mut alice = app.client("alice").await.unwrap();
    alice.publish_key_package().await.unwrap();
    let mut bob = app.client("bob").await.unwrap();
    let group = bob
        .create_group("worker retry", &[&alice_account.account_id_hex])
        .await
        .unwrap();
    let mut cursor = 0;
    deliver_new(&mut alice, &relay, &mut cursor).await;
    alice.sync().await.unwrap();
    alice.accept_group_invite(&group).unwrap();
    let path = app.account_storage_path("alice");
    let keys = home.load_signing_keys("alice").unwrap();
    let key = app
        .sqlcipher_key("alice", &keys, &path, crate::SqlcipherDatabaseKind::Session)
        .unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    storage_sqlite::open_hardened_sqlcipher(
        &connection,
        &key,
        storage_sqlite::SqlCipherHardening::cipher_only(),
    )
    .unwrap();
    drop(alice);
    let _pump = scripted_eose_pump(app.relay_plane.clone(), relay.clone(), every_subscription);
    let runtime = app.runtime();
    let mut notifications = runtime.subscribe();
    let mut timeline = runtime
        .subscribe_timeline_messages(
            "alice",
            TimelineMessageQuery {
                group_id_hex: Some(hex::encode(group.as_slice())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    runtime
        .retry_group_convergence("alice", &group)
        .await
        .unwrap();
    let baseline = timeline.take_snapshot().messages.len();
    let fault = if late_checkpoint {
        "CREATE TRIGGER fail_live_activity BEFORE INSERT ON account_groups WHEN NEW.profile_name = 'surviving worker rename' BEGIN SELECT RAISE(FAIL, 'injected late checkpoint'); END;"
    } else {
        "CREATE TRIGGER fail_live_activity BEFORE INSERT ON app_events WHEN NEW.kind = 1210 BEGIN SELECT RAISE(FAIL, 'injected live activity'); END;"
    };
    connection.execute_batch(fault).unwrap();
    let cursor = relay.published_events.lock().unwrap().len();
    bob.update_group_profile(&group, Some("surviving worker rename"), None)
        .await
        .unwrap();
    let signed = relay.published_events.lock().unwrap()[cursor..]
        .iter()
        .find(|event| event.kind == transport_nostr_peeler::KIND_MARMOT_GROUP_MESSAGE)
        .unwrap()
        .clone();
    let delivery = cgka_traits::TransportDelivery {
        account_id: MemberId::new(hex::decode(&alice_account.account_id_hex).unwrap()),
        group_id_hint: Some(group.clone()),
        message: signed.to_transport_message().unwrap(),
        received_at: cgka_traits::Timestamp(unix_now_seconds()),
        source: cgka_traits::TransportDeliverySource {
            transport: cgka_traits::transport::TransportSource("nostr".into()),
            plane: cgka_traits::TransportDeliveryPlane::Group,
            endpoint: None,
            subscription_id: None,
            wire: None,
        },
    };
    app.relay_plane
        .route_account_delivery_for_test(delivery.clone());
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let crate::MarmotAppEvent::AccountError(error) = notifications.recv().await.unwrap()
                && error.account_label == "alice"
            {
                break;
            }
        }
    })
    .await
    .expect("worker must reach the injected projection failure");
    // Another live delivery used to provoke reopening the client, losing its
    // one-shot retry. Keep the fault through several bounded retry turns.
    let cursor = relay.published_events.lock().unwrap().len();
    bob.send(&group, b"chat waiting behind activity")
        .await
        .unwrap();
    let next_signed = relay.published_events.lock().unwrap()[cursor..]
        .iter()
        .find(|event| event.kind == transport_nostr_peeler::KIND_MARMOT_GROUP_MESSAGE)
        .unwrap()
        .clone();
    let mut later_delivery = delivery;
    later_delivery.message = next_signed.to_transport_message().unwrap();
    app.relay_plane
        .route_account_delivery_for_test(later_delivery);
    tokio::time::sleep(Duration::from_millis(300)).await;
    connection
        .execute_batch("DROP TRIGGER fail_live_activity")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            timeline.recv().await.unwrap();
            let page = timeline.take_snapshot();
            if page.messages.iter().any(|message| {
                message.kind == 1210 && message.plaintext.contains("surviving worker rename")
            }) && page
                .messages
                .iter()
                .any(|message| message.plaintext == "chat waiting behind activity")
            {
                break;
            }
        }
    })
    .await
    .expect("quiet worker retry must deliver the one-shot activity");
    assert_eq!(timeline.take_snapshot().messages.len(), baseline + 2);
    runtime.shutdown().await;
}

/// Canonical devices remain separate from the account roster and leaf positions.
#[tokio::test]
async fn member_devices_group_accounts_and_survive_self_update_reopen_and_leaf_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let sibling_dir = tempfile::tempdir().unwrap();
    let home = AccountHome::open(dir.path());
    home.create_account("alice").unwrap();
    let bob = home.create_account("bob").unwrap();
    let secret = home
        .load_signing_keys("bob")
        .unwrap()
        .secret_key()
        .to_secret_hex();
    AccountHome::open(sibling_dir.path())
        .import_account("bob", &secret)
        .unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let app = app_at(&dir, relay.clone());
    let sibling_app = app_at(&sibling_dir, relay.clone());
    remember_test_member_inbox(&app, &bob.account_id_hex, "wss://relay.example");
    let mut bob_client = app.client("bob").await.unwrap();
    bob_client.publish_key_package().await.unwrap();
    bob_client.sync().await.unwrap();
    let mut alice = app.client("alice").await.unwrap();
    let mut alice_cursor = 0;
    let mut bob_cursor = 0;
    let group = alice
        .create_group("sibling departure", &[&bob.account_id_hex])
        .await
        .unwrap();
    deliver_new(&mut bob_client, &relay, &mut bob_cursor).await;
    bob_client.sync().await.unwrap();
    bob_client.accept_group_invite(&group).unwrap();
    let mut sibling = sibling_app.client("bob").await.unwrap();
    sibling.sync().await.unwrap();
    let mut sibling_cursor = relay.published_events.lock().unwrap().len();
    let kp = published_key_package(&mut sibling, &relay).await;
    let effects = alice
        .runtime
        .send(SendIntent::Invite {
            group_id: group.clone(),
            key_packages: vec![kp],
            initial_admins: vec![],
        })
        .await
        .unwrap();
    alice
        .observe_drained_session_events(&effects)
        .await
        .unwrap();
    deliver_new(&mut sibling, &relay, &mut sibling_cursor).await;
    sibling.sync().await.unwrap();
    sibling.accept_group_invite(&group).unwrap();
    deliver_new(&mut bob_client, &relay, &mut bob_cursor).await;
    bob_client.sync().await.unwrap();
    bob_client.retry_group_convergence(&group).await.unwrap();
    let surviving_leaf = bob_client.runtime.own_leaf_index(&group).unwrap();
    let departing_leaf = sibling.runtime.own_leaf_index(&group).unwrap();
    assert_ne!(surviving_leaf, departing_leaf);

    let admitted = bob_client
        .runtime
        .session()
        .canonical_group_membership(&group)
        .unwrap();
    let admitted_accounts = admitted
        .devices
        .iter()
        .map(|leaf| hex::encode(leaf.member.as_slice()))
        .collect::<std::collections::BTreeSet<_>>();
    let roster = bob_client.group_roster_session(&group).unwrap();
    assert!(roster.complete);
    let rows = bob_client.members(&group).unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.member_id_hex.clone())
            .collect::<std::collections::BTreeSet<_>>(),
        admitted_accounts
    );
    assert_eq!(
        rows.len(),
        2,
        "one person row per account, including sibling leaves"
    );
    let devices = rows
        .iter()
        .find(|m| m.member_id_hex == bob.account_id_hex)
        .unwrap()
        .devices
        .clone()
        .unwrap();
    assert_eq!(devices.len(), 2);
    assert_eq!(devices[0].leaf_index, surviving_leaf);
    assert!(devices[0].is_this_installation);
    assert!(!devices[1].is_this_installation);
    assert_ne!(devices[0].device_id, devices[1].device_id);
    assert!(devices.iter().all(|d| d.joined_at.is_none()));
    assert!(devices.iter().all(|d| {
        d.device_id.len() == 64
            && d.device_id
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    }));
    assert_eq!(bob_client.members(&group).unwrap(), rows);
    assert_eq!(
        bob_client.group_roster_session(&group).unwrap().members,
        rows
    );
    assert!(rows.iter().all(|row| {
        row.devices
            .as_ref()
            .is_some_and(|devices| !devices.is_empty())
    }));
    let sibling_devices = sibling
        .members(&group)
        .unwrap()
        .into_iter()
        .find(|member| member.member_id_hex == bob.account_id_hex)
        .unwrap()
        .devices
        .unwrap();
    assert_eq!(sibling_devices[0].leaf_index, departing_leaf);
    assert!(sibling_devices[0].is_this_installation);
    assert!(!sibling_devices[1].is_this_installation);
    assert_eq!(sibling_devices[1].leaf_index, surviving_leaf);
    // The retained signed publication enriches our own leaf even when the
    // account's addressable cache slot has been replaced by a sibling.
    assert_eq!(devices[0].client_label.as_deref(), Some("Test Client"));
    let selected = alice
        .members(&group)
        .unwrap()
        .into_iter()
        .find(|m| m.member_id_hex == bob.account_id_hex)
        .unwrap()
        .devices
        .unwrap();
    assert_eq!(
        selected
            .iter()
            .find(|d| d.leaf_index == surviving_leaf)
            .unwrap()
            .client_label
            .as_deref(),
        Some("Test Client")
    );
    assert!(
        selected
            .iter()
            .find(|d| d.leaf_index == departing_leaf)
            .unwrap()
            .client_label
            .is_none()
    );
    assert!(selected.iter().all(|d| !d.is_this_installation));
    // Both authenticated sibling leaves of one account retain their own device ID.
    bob_client
        .send(&group, b"sender provenance original")
        .await
        .unwrap();
    sibling
        .send(&group, b"sender provenance sibling")
        .await
        .unwrap();
    deliver_new(&mut alice, &relay, &mut alice_cursor).await;
    alice.sync().await.unwrap();
    deliver_new(&mut bob_client, &relay, &mut bob_cursor).await;
    bob_client.sync().await.unwrap();
    let group_hex = hex::encode(group.as_slice());
    for (content, index) in [
        ("sender provenance original", surviving_leaf),
        ("sender provenance sibling", departing_leaf),
    ] {
        let expected_id = &selected
            .iter()
            .find(|d| d.leaf_index == index)
            .unwrap()
            .device_id;
        let raw = app
            .messages("alice")
            .unwrap()
            .into_iter()
            .find(|m| m.plaintext == content)
            .unwrap();
        let timeline = app
            .timeline_message("alice", &group_hex, &raw.message_id_hex)
            .unwrap()
            .unwrap();
        assert_eq!(raw.sender_device, timeline.sender_device);
        let crate::SenderDevice::Exact { client_label, .. } = &raw.sender_device else {
            panic!("fresh sibling messages must retain exact device evidence");
        };
        assert_eq!(
            client_label,
            &selected
                .iter()
                .find(|d| d.leaf_index == index)
                .unwrap()
                .client_label
        );
        assert!(
            matches!(&raw.sender_device, crate::SenderDevice::Exact { device_id, leaf_index, is_this_installation: false, .. } if device_id == expected_id && *leaf_index == index)
        );
        let local = app
            .messages("bob")
            .unwrap()
            .into_iter()
            .find(|m| m.plaintext == content)
            .unwrap();
        assert!(
            matches!(&local.sender_device, crate::SenderDevice::Exact { device_id, is_this_installation, .. } if device_id == expected_id && *is_this_installation == (index == surviving_leaf))
        );
    }
    let reopened_projection = app_at(&dir, relay.clone());
    let reopened_messages = reopened_projection.messages("alice").unwrap();
    for raw in app
        .messages("alice")
        .unwrap()
        .into_iter()
        .filter(|m| m.plaintext.starts_with("sender provenance"))
    {
        assert_eq!(
            reopened_messages
                .iter()
                .find(|m| m.message_id_hex == raw.message_id_hex)
                .unwrap()
                .sender_device,
            raw.sender_device
        );
    }
    // Mirror-only staged accounts stay visible without inventing admitted leaves.
    let staged_account = home.create_account("staged").unwrap();
    let storage = app.account_storage("alice").unwrap();
    let original = storage.get_group(&group).unwrap();
    let mut staged = original.clone();
    let mut member = staged.members[0].clone();
    member.id = MemberId::new(hex::decode(&staged_account.account_id_hex).unwrap());
    staged.members.push(member);
    storage.put_group(&staged).unwrap();
    assert!(!alice.group_roster_session(&group).unwrap().complete);
    assert!(
        !alice
            .group_read_snapshot()
            .unwrap()
            .group_roster(&group)
            .unwrap()
            .complete
    );
    let staged_rows = alice.members(&group).unwrap();
    assert_eq!(staged_rows.len(), 3);
    assert!(
        staged_rows
            .iter()
            .find(|member| member.member_id_hex == staged_account.account_id_hex)
            .unwrap()
            .devices
            .is_none()
    );
    assert_eq!(
        alice.group_roster_session(&group).unwrap().members,
        staged_rows
    );
    // A staged removal can hide an account while both its leaves remain admitted.
    let mut omitted = original.clone();
    omitted
        .members
        .retain(|member| hex::encode(member.id.as_slice()) != bob.account_id_hex);
    storage.put_group(&omitted).unwrap();
    let omitted_roster = alice.group_roster_session(&group).unwrap();
    assert_eq!(omitted_roster.members.len(), 1);
    assert!(omitted_roster.members.iter().all(|row| {
        row.devices
            .as_ref()
            .is_some_and(|devices| !devices.is_empty())
    }));
    assert!(
        !omitted_roster.complete,
        "known devices on every returned row do not prove completeness"
    );
    assert!(
        !alice
            .group_read_snapshot()
            .unwrap()
            .group_roster(&group)
            .unwrap()
            .complete
    );
    storage.put_group(&original).unwrap();
    assert!(alice.group_roster_session(&group).unwrap().complete);
    assert_eq!(
        rows.iter()
            .find(|m| m.member_id_hex != bob.account_id_hex)
            .unwrap()
            .devices
            .as_ref()
            .unwrap()[0]
            .client_label
            .as_deref(),
        Some("Test Client")
    );

    let effects = bob_client
        .runtime
        .send(SendIntent::SelfUpdate {
            group_id: group.clone(),
        })
        .await
        .unwrap();
    bob_client
        .observe_drained_session_events(&effects)
        .await
        .unwrap();
    assert_eq!(bob_client.members(&group).unwrap(), rows);
    deliver_new(&mut alice, &relay, &mut alice_cursor).await;
    alice.sync().await.unwrap();
    alice.retry_group_convergence(&group).await.unwrap();
    drop(bob_client);
    let bob_client = app.client("bob").await.unwrap();
    assert_eq!(bob_client.members(&group).unwrap(), rows);

    let old = alice
        .members(&group)
        .unwrap()
        .into_iter()
        .find(|m| m.member_id_hex == bob.account_id_hex)
        .unwrap()
        .devices
        .unwrap();
    let effects = alice
        .runtime
        .send(SendIntent::RemoveMembers {
            group_id: group.clone(),
            members: vec![MemberId::new(hex::decode(&bob.account_id_hex).unwrap())],
        })
        .await
        .unwrap();
    alice
        .observe_drained_session_events(&effects)
        .await
        .unwrap();
    assert!(
        alice
            .members(&group)
            .unwrap()
            .iter()
            .all(|m| m.member_id_hex != bob.account_id_hex)
    );
    let replacement_dir = tempfile::tempdir().unwrap();
    AccountHome::open(replacement_dir.path())
        .import_account("bob", &secret)
        .unwrap();
    let replacement_app = app_at(&replacement_dir, relay.clone());
    let mut replacement = replacement_app.client("bob").await.unwrap();
    let kp = published_key_package(&mut replacement, &relay).await;
    let effects = alice
        .runtime
        .send(SendIntent::Invite {
            group_id: group.clone(),
            key_packages: vec![kp],
            initial_admins: vec![],
        })
        .await
        .unwrap();
    alice
        .observe_drained_session_events(&effects)
        .await
        .unwrap();
    let new = alice
        .members(&group)
        .unwrap()
        .into_iter()
        .find(|m| m.member_id_hex == bob.account_id_hex)
        .unwrap()
        .devices
        .unwrap();
    assert_eq!(new.len(), 1);
    let reused = old
        .iter()
        .find(|d| d.leaf_index == new[0].leaf_index)
        .expect("OpenMLS reuses a removed slot");
    assert_ne!(reused.device_id, new[0].device_id);
    assert!(
        new[0].client_label.is_none(),
        "a cached sibling label must not follow an account to another signature key"
    );
    assert!(!new[0].is_this_installation);
}

async fn member_device_label_fixture() -> (tempfile::TempDir, MarmotApp, AppClient, GroupId, String)
{
    let dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    AccountHome::open(dir.path())
        .create_account("alice")
        .unwrap();
    let bob = AccountHome::open(remote_dir.path())
        .create_account("bob")
        .unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let app = app_at(&dir, relay.clone());
    let remote = app_at(&remote_dir, relay);
    let mut bob_client = remote.client("bob").await.unwrap();
    bob_client.publish_key_package().await.unwrap();
    remember_test_member_inbox(&app, &bob.account_id_hex, "wss://relay.example");
    let mut alice = app.client("alice").await.unwrap();
    let group = alice
        .create_group("label regression", &[&bob.account_id_hex])
        .await
        .unwrap();
    (dir, app, alice, group, bob.account_id_hex)
}

#[tokio::test]
async fn member_device_labels_unreadable_advisory_row_preserves_roster_and_snapshot() {
    let (_dir, app, mut alice, group, bob_id) = member_device_label_fixture().await;
    let second = alice.create_group("second roster", &[]).await.unwrap();
    let second_members = alice.members(&second).unwrap();
    let before = alice.members(&group).unwrap();
    let shared = app.shared_storage().unwrap();
    let mut record = shared.public_directory_user(&bob_id).unwrap().unwrap();
    record.key_package_json = Some("unreadable json".into());
    shared.put_public_directory_user(&record).unwrap();
    let mut expected = before;
    for member in &mut expected {
        if member.member_id_hex == bob_id {
            for device in member.devices.as_mut().unwrap() {
                device.client_label = None;
            }
        }
    }
    assert_eq!(alice.members(&group).unwrap(), expected);
    assert_eq!(
        alice.group_roster_session(&group).unwrap().members,
        expected
    );
    assert_eq!(
        alice
            .group_read_snapshot()
            .unwrap()
            .members(&group)
            .unwrap(),
        expected
    );
    assert_eq!(
        alice
            .group_read_snapshot()
            .unwrap()
            .members(&second)
            .unwrap(),
        second_members
    );
    // A failed advisory store read must also be harmless.
    let connection = rusqlite::Connection::open(app.shared_storage_path()).unwrap();
    connection
        .execute(
            "UPDATE directory_users SET key_package_json = X'FF' WHERE account_id_hex = ?1",
            [&bob_id],
        )
        .unwrap();
    assert_eq!(
        alice
            .members(&group)
            .unwrap()
            .iter()
            .map(|m| m.devices.as_ref().unwrap().len())
            .sum::<usize>(),
        2
    );
    // Authoritative failures still fail roster capture.
    let path = app.account_storage_path("alice");
    let keys = app.account_home().load_signing_keys("alice").unwrap();
    let key = app
        .sqlcipher_key("alice", &keys, &path, SqlcipherDatabaseKind::Session)
        .unwrap();
    let authoritative = rusqlite::Connection::open(path).unwrap();
    storage_sqlite::open_hardened_sqlcipher(
        &authoritative,
        &key,
        storage_sqlite::SqlCipherHardening::cipher_only(),
    )
    .unwrap();
    authoritative
        .execute(
            "UPDATE cgka_groups SET record = 'unreadable json' WHERE id = ?1",
            [group.as_slice()],
        )
        .unwrap();
    assert!(alice.members(&group).is_err());
    assert!(alice.group_roster_session(&group).is_err());
    assert!(alice.group_read_snapshot().is_err());
}

#[tokio::test]
async fn member_device_labels_capture_ignores_follow_edges_and_relay_metadata() {
    let (_dir, app, alice, group, bob_id) = member_device_label_fixture().await;
    let expected = alice.members(&group).unwrap();
    let path = app.directory_cache_path("alice");
    let keys = app.account_home().load_signing_keys("alice").unwrap();
    let key = app
        .sqlcipher_key(
            "alice",
            &keys,
            &path,
            crate::sqlcipher::SqlcipherDatabaseKind::DirectoryCache,
        )
        .unwrap();
    let private = rusqlite::Connection::open(path).unwrap();
    storage_sqlite::open_hardened_sqlcipher(
        &private,
        &key,
        storage_sqlite::SqlCipherHardening::live_cache(),
    )
    .unwrap();
    private.execute_batch("DROP TABLE directory_user_follows; UPDATE directory_users SET relay_lists_json = 'unreadable json'").unwrap();
    let connection = rusqlite::Connection::open(app.shared_storage_path()).unwrap();
    connection.execute("INSERT INTO directory_user_follows (account_id_hex, follow_account_id_hex, position) VALUES (?1, X'FF', 0)", [&bob_id]).unwrap();
    connection.execute("UPDATE directory_users SET relay_lists_json = 'unreadable json' WHERE account_id_hex = ?1", [&bob_id]).unwrap();
    assert_eq!(
        alice
            .group_read_snapshot()
            .unwrap()
            .members(&group)
            .unwrap(),
        expected
    );
}

#[tokio::test]
async fn member_device_labels_reuse_validation_until_package_changes() {
    let (_dir, app, alice, group, bob_id) = member_device_label_fixture().await;
    let expected = alice.members(&group).unwrap();
    let warmed = alice
        .member_device_validation_count
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        warmed, 2,
        "each package, including the local publication, validates once"
    );
    assert_eq!(
        alice
            .group_read_snapshot()
            .unwrap()
            .members(&group)
            .unwrap(),
        expected
    );
    assert_eq!(alice.members(&group).unwrap(), expected);
    assert_eq!(
        alice
            .member_device_validation_count
            .load(std::sync::atomic::Ordering::Relaxed),
        warmed,
        "unchanged packages must not be revalidated"
    );
    let shared = app.shared_storage().unwrap();
    let mut record = shared.public_directory_user(&bob_id).unwrap().unwrap();
    let mut package: DirectoryKeyPackage =
        serde_json::from_str(record.key_package_json.as_ref().unwrap()).unwrap();
    package.created_at += 1;
    package.key_package_hex = "00".into();
    record.key_package_json = Some(serde_json::to_string(&package).unwrap());
    shared.put_public_directory_user(&record).unwrap();
    let members = alice.members(&group).unwrap();
    assert!(
        members
            .iter()
            .find(|m| m.member_id_hex == bob_id)
            .unwrap()
            .devices
            .as_ref()
            .unwrap()[0]
            .client_label
            .is_none()
    );
    assert_eq!(
        alice
            .member_device_validation_count
            .load(std::sync::atomic::Ordering::Relaxed),
        warmed + 1
    );
    alice.members(&group).unwrap();
    assert_eq!(
        alice
            .member_device_validation_count
            .load(std::sync::atomic::Ordering::Relaxed),
        warmed + 1,
        "unchanged rejected packages must not be revalidated"
    );
}

/// A receiver learns Alice's leaf through Welcome, without observing her publication.
#[tokio::test]
async fn welcome_device_labels_enrich_member_and_existing_exact_sender() {
    welcome_device_labels_read_time_fixture(false).await;
}

#[tokio::test]
async fn device_labels_cache_success_survives_rejected_history_writes() {
    welcome_device_labels_read_time_fixture(true).await;
}

async fn welcome_device_labels_read_time_fixture(reject_history: bool) {
    let inviter_dir = tempfile::tempdir().unwrap();
    let receiver_dir = tempfile::tempdir().unwrap();
    let home = AccountHome::open(inviter_dir.path());
    let alice_account = home.create_account("alice").unwrap();
    let bob_account = home.create_account("bob").unwrap();
    let secret = home
        .load_signing_keys("bob")
        .unwrap()
        .secret_key()
        .to_secret_hex();
    AccountHome::open(receiver_dir.path())
        .import_account("bob", &secret)
        .unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let mut inviter = MarmotApp::with_relay_and_config(
        inviter_dir.path(),
        "wss://relay.example",
        MarmotAppConfig {
            key_package_client_name: Some("Lightning Piggy".into()),
            ..MarmotAppConfig::default()
        }
        .with_dev_settlement_quiescence_ms(0),
    )
    .with_test_relay_client(relay.clone());
    let inviter_fetcher = Arc::new(MemberResolutionDirectoryFetcher::default());
    inviter.relay_plane = MarmotRelayPlane::new_with_directory_fetcher_for_test(
        Some(Duration::from_secs(120)),
        relay.clone(),
        inviter_fetcher.clone(),
        false,
    );
    let fetcher = Arc::new(MemberResolutionDirectoryFetcher::default());
    let mut receiver = app_at(&receiver_dir, relay.clone());
    receiver.relay_plane = MarmotRelayPlane::new_with_directory_fetcher_for_test(
        Some(Duration::from_secs(120)),
        relay.clone(),
        fetcher.clone(),
        false,
    );
    remember_test_member_inbox(&inviter, &bob_account.account_id_hex, "wss://relay.example");
    let mut bob = receiver.client("bob").await.unwrap();
    bob.publish_key_package().await.unwrap();
    bob.sync().await.unwrap();
    let mut alice = inviter.client("alice").await.unwrap();
    alice.publish_key_package().await.unwrap();
    let alice_event = relay
        .published_events
        .lock()
        .unwrap()
        .iter()
        .find(|event| {
            event.kind == KIND_MARMOT_KEY_PACKAGE && event.pubkey == alice_account.account_id_hex
        })
        .unwrap()
        .clone();
    *inviter_fetcher.events.lock().unwrap() = relay
        .published_events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.kind == KIND_MARMOT_KEY_PACKAGE)
        .cloned()
        .collect();
    let mut bob_routes = AccountRelayListStatus::empty();
    bob_routes.nip65.relays = vec!["wss://relay.example".into()];
    bob_routes.inbox.relays = vec!["wss://relay.example".into()];
    inviter
        .remember_directory_relay_lists(&bob_account.account_id_hex, &bob_routes)
        .unwrap();
    let group = alice
        .create_group("Welcome label", &[&bob_account.account_id_hex])
        .await
        .unwrap();
    // The inviter already selected Bob's validated package and retains its label.
    let inviter_bob = alice
        .members(&group)
        .unwrap()
        .into_iter()
        .find(|m| m.member_id_hex == bob_account.account_id_hex)
        .unwrap();
    assert_eq!(
        inviter_bob.devices.unwrap()[0].client_label.as_deref(),
        Some("Test Client")
    );
    let mut cursor = 0;
    deliver_new(&mut bob, &relay, &mut cursor).await;
    bob.sync().await.unwrap();
    bob.accept_group_invite(&group).unwrap();
    alice.send(&group, b"label arrives later").await.unwrap();
    deliver_new(&mut bob, &relay, &mut cursor).await;
    bob.sync().await.unwrap();
    let before = bob
        .members(&group)
        .unwrap()
        .into_iter()
        .find(|m| m.member_id_hex == alice_account.account_id_hex)
        .unwrap();
    assert!(before.devices.as_ref().unwrap()[0].client_label.is_none());
    let raw_before = receiver
        .messages("bob")
        .unwrap()
        .into_iter()
        .find(|m| m.plaintext == "label arrives later")
        .unwrap();
    assert!(matches!(
        raw_before.sender_device,
        crate::SenderDevice::Exact {
            client_label: None,
            ..
        }
    ));
    fetcher.requests.lock().unwrap().clear();
    let mut routes = AccountRelayListStatus::empty();
    routes.nip65.relays = vec![
        "wss://relay.nostr.band".into(),
        "wss://192.168.1.1".into(),
        "wss://relay.example".into(),
    ];
    receiver
        .remember_directory_relay_lists(&alice_account.account_id_hex, &routes)
        .unwrap();
    *fetcher.events.lock().unwrap() = vec![alice_event];
    // Exercise the actual worker: blocked optional I/O cannot block roster reads.
    drop(bob);
    fetcher.requests.lock().unwrap().clear();
    let (entered, release) = fetcher.hold_fetches();
    *fetcher.fetch_gate_author.lock().unwrap() = Some(alice_account.account_id_hex.clone());
    let runtime = crate::MarmotAppRuntime::new(receiver.clone());
    let mut notifications = runtime.subscribe();
    runtime.start().await.unwrap();
    let mut timeline_subscription = runtime
        .subscribe_timeline_messages(
            "bob",
            crate::TimelineMessageQuery {
                group_id_hex: Some(hex::encode(group.as_slice())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        timeline_subscription
            .take_snapshot()
            .messages
            .iter()
            .any(|row| row.message_id_hex == raw_before.message_id_hex
                && matches!(
                    &row.sender_device,
                    crate::SenderDevice::Exact {
                        client_label: None,
                        ..
                    }
                ))
    );
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("missing labels schedule a background fetch");
    for _ in 0..20 {
        let rows =
            tokio::time::timeout(Duration::from_secs(2), runtime.group_members("bob", &group))
                .await
                .expect("optional I/O must not block roster reads")
                .unwrap();
        let member = rows
            .iter()
            .find(|m| m.member_id_hex == alice_account.account_id_hex)
            .unwrap();
        assert!(member.devices.as_ref().unwrap()[0].client_label.is_none());
    }
    let path = receiver.account_storage_path("bob");
    let keys = receiver.account_home().load_signing_keys("bob").unwrap();
    let key = receiver
        .sqlcipher_key("bob", &keys, &path, SqlcipherDatabaseKind::Session)
        .unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    storage_sqlite::open_hardened_sqlcipher(
        &connection,
        &key,
        storage_sqlite::SqlCipherHardening::cipher_only(),
    )
    .unwrap();
    if reject_history {
        connection.execute_batch("CREATE TRIGGER reject_label_history BEFORE UPDATE OF sender_device ON app_events BEGIN SELECT RAISE(ABORT, 'reject history writes'); END;").unwrap();
    }
    release.notify_one();
    let mut notified_group = false;
    let mut notified_timeline = false;
    tokio::time::timeout(Duration::from_secs(10), async {
        // The runtime event asks the host to re-read its roster while the
        // independent bounded timeline subscription refreshes its own window.
        let roster_updates = async {
            while !notified_group {
                if let crate::MarmotAppEvent::DeviceLabelsChanged { account_id_hex } = notifications.recv().await.unwrap() {
                    notified_group |= account_id_hex.as_deref().is_none_or(|account| account == alice_account.account_id_hex);
                }
            }
        };
        let timeline_updates = async {
            while !notified_timeline {
                timeline_subscription.recv().await.expect("timeline subscription remains open");
                notified_timeline |= timeline_subscription.take_snapshot().messages.iter().any(|row|
                    row.message_id_hex == raw_before.message_id_hex && matches!(&row.sender_device,
                        crate::SenderDevice::Exact { client_label: Some(label), .. } if label == "Lightning Piggy"));
            }
        };
        tokio::join!(roster_updates, timeline_updates);
    }).await.unwrap_or_else(|_| {
        let has_label = |row: &crate::TimelineMessageRecord| matches!(&row.sender_device,
            crate::SenderDevice::Exact { client_label: Some(label), .. } if label == "Lightning Piggy");
        let reread = receiver.timeline_message("bob", &raw_before.group_id_hex, &raw_before.message_id_hex).unwrap();
        let snapshot = timeline_subscription.take_snapshot();
        let displayed = snapshot.messages.iter().find(|row| row.message_id_hex == raw_before.message_id_hex);
        panic!("roster and timeline hosts are notified: group={notified_group}, timeline={notified_timeline}, stored_present={}, stored_label={}, displayed_present={}, displayed_label={}, window_size={}",
            reread.is_some(), reread.as_ref().is_some_and(has_label), displayed.is_some(), displayed.is_some_and(has_label), snapshot.messages.len());
    });
    let after = runtime
        .group_members("bob", &group)
        .await
        .unwrap()
        .into_iter()
        .find(|m| m.member_id_hex == alice_account.account_id_hex)
        .unwrap();
    assert_eq!(
        after.devices.as_ref().unwrap()[0].client_label.as_deref(),
        Some("Lightning Piggy")
    );
    let raw_after = receiver
        .messages("bob")
        .unwrap()
        .into_iter()
        .find(|m| m.message_id_hex == raw_before.message_id_hex)
        .unwrap();
    let expected = crate::SenderDevice::Exact {
        device_id: after.devices.as_ref().unwrap()[0].device_id.clone(),
        leaf_index: after.devices.as_ref().unwrap()[0].leaf_index,
        client_label: Some("Lightning Piggy".into()),
        is_this_installation: false,
    };
    assert_eq!(raw_after.sender_device, expected);
    assert_eq!(
        receiver
            .account_storage("bob")
            .unwrap()
            .app_message(&raw_after.group_id_hex, &raw_after.message_id_hex)
            .unwrap()
            .unwrap()
            .sender_device,
        raw_before.sender_device,
        "background enrichment must never rewrite history"
    );

    let timeline = receiver
        .timeline_message(
            "bob",
            &hex::encode(group.as_slice()),
            &raw_after.message_id_hex,
        )
        .unwrap()
        .unwrap();
    assert_eq!(timeline.sender_device, expected);
    assert_eq!(
        receiver
            .message_by_id("bob", &raw_after.group_id_hex, &raw_after.message_id_hex)
            .unwrap()
            .unwrap()
            .sender_device,
        expected
    );
    for page in [
        receiver
            .timeline_messages_with_query(
                "bob",
                crate::TimelineMessageQuery {
                    group_id_hex: Some(raw_after.group_id_hex.clone()),
                    ..Default::default()
                },
            )
            .unwrap(),
        receiver
            .timeline_messages_by_wall_clock_with_query(
                "bob",
                crate::TimelineMessageQuery {
                    group_id_hex: Some(raw_after.group_id_hex.clone()),
                    ..Default::default()
                },
            )
            .unwrap(),
    ] {
        assert_eq!(
            page.messages
                .iter()
                .find(|row| row.message_id_hex == raw_after.message_id_hex)
                .unwrap()
                .sender_device,
            expected
        );
    }
    let window = runtime
        .open_conversation_window("bob", &group, Default::default())
        .await
        .unwrap();
    let snapshot = &window.snapshot;
    assert_eq!(
        snapshot
            .page
            .page()
            .messages
            .iter()
            .find(|row| row.message_id_hex == raw_after.message_id_hex)
            .unwrap()
            .sender_device,
        expected
    );
    drop(window);
    runtime.shutdown().await;
    assert_eq!(
        fetcher
            .requests
            .lock()
            .unwrap()
            .iter()
            .flat_map(|request| &request.queries)
            .filter(|query| query.kind == KIND_MARMOT_KEY_PACKAGE
                && query.authors.contains(&alice_account.account_id_hex))
            .count(),
        1
    );
}

#[tokio::test]
async fn device_label_fetch_failure_leaves_none_and_skips_retired_and_unsafe_routes() {
    let dir = tempfile::tempdir().unwrap();
    let home = AccountHome::open(dir.path());
    let account = home.create_account("alice").unwrap();
    let fetcher = Arc::new(MemberResolutionDirectoryFetcher::default());
    fetcher.fail_all_fetches();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let mut app = app_at(&dir, relay.clone());
    app.relay_plane = MarmotRelayPlane::new_with_directory_fetcher_for_test(
        Some(Duration::from_secs(120)),
        relay,
        fetcher.clone(),
        false,
    );
    let mut routes = AccountRelayListStatus::empty();
    routes.nip65.relays = vec![
        "wss://relay.nostr.band".into(),
        "wss://192.168.1.1".into(),
        "wss://healthy.example".into(),
    ];
    app.remember_directory_relay_lists(&account.account_id_hex, &routes)
        .unwrap();
    let mut client = app.client("alice").await.unwrap();
    let group = GroupId::new([1, 2, 3]);
    let device = crate::SenderDevice::Exact {
        device_id: "leaf".into(),
        leaf_index: 0,
        client_label: None,
        is_this_installation: false,
    };
    for _ in 0..20 {
        assert_eq!(
            client.enrich_sender_device(&group, &account.account_id_hex, device.clone()),
            device
        );
    }
    assert!(fetcher.requests.lock().unwrap().is_empty());
    let accounts = client.device_label_enrichment.begin();
    assert_eq!(accounts, vec![account.account_id_hex.clone()]);
    let fetched = app.clone().run_device_label_pass(accounts).await;
    client.finish_device_label_pass(fetched);
    assert_eq!(
        client.enrich_sender_device(&group, &account.account_id_hex, device.clone()),
        device
    );
    assert!(
        client.device_label_enrichment.begin().is_empty(),
        "failures receive cooldown"
    );
    let requests = fetcher.requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "coalesced demand makes one failed fetch");
    assert!(requests.iter().all(
        |request| request.endpoints == vec![TransportEndpoint("wss://healthy.example".into())]
    ));
    assert!(
        requests
            .iter()
            .flat_map(|request| &request.queries)
            .all(|query| query.limit == crate::client::device_labels::DEVICE_LABEL_EVENT_LIMIT)
    );
}

#[tokio::test]
async fn inviter_device_label_cache_retains_every_selected_leaf_after_slot_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let home = AccountHome::open(dir.path());
    let account = home.create_account("alice").unwrap();
    let app = app_at(&dir, Arc::new(ScriptedPushRelayClient::default()));
    let mut keys = Vec::new();
    for (created_at, label) in [(10, "Lightning Piggy\n"), (11, "White Noise")] {
        let package = fresh_key_package_for_account(&app, &account, false).await;
        let metadata = cgka_engine::key_package::key_package_metadata(&package).unwrap();
        keys.push(metadata.signature_key);
        let selected = FetchedKeyPackage {
            account_id_hex: account.account_id_hex.clone(),
            key_package: package,
            client_label: Some(label.into()),
            key_package_id: "same-slot".into(),
            key_package_ref_hex: metadata.key_package_ref_hex,
            key_package_event_id: "42".repeat(32),
            created_at,
            source_relays: vec!["wss://relay.example".into()],
            relay_lists: AccountRelayListStatus::empty(),
        };
        // The common validated-package acceptance seam used by create and invite.
        app.remember_device_client_label(&selected).unwrap();
        app.remember_directory_key_package(&selected).unwrap();
    }
    assert_ne!(keys[0], keys[1]);
    let client = app.client("alice").await.unwrap();
    let group = GroupId::new([1]);
    for (key, label) in keys.iter().zip(["Lightning Piggy", "White Noise"]) {
        let result = client.enrich_sender_device(
            &group,
            &account.account_id_hex,
            crate::SenderDevice::Exact {
                device_id: cgka_traits::message::member_device_id(&group, key),
                leaf_index: 0,
                client_label: None,
                is_this_installation: false,
            },
        );
        assert!(
            matches!(result, crate::SenderDevice::Exact { client_label: Some(ref actual), .. } if actual == label)
        );
    }
}

#[tokio::test]
async fn device_label_fetch_validates_signatures_freshness_and_local_event_bound() {
    use nostr_sdk::prelude::UnsignedEvent;
    let dir = tempfile::tempdir().unwrap();
    let home = AccountHome::open(dir.path());
    let account = home.create_account("alice").unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let fetcher = Arc::new(MemberResolutionDirectoryFetcher::default());
    let mut app = app_at(&dir, relay.clone());
    app.relay_plane = MarmotRelayPlane::new_with_directory_fetcher_for_test(
        Some(Duration::from_secs(120)),
        relay.clone(),
        fetcher.clone(),
        false,
    );
    let mut client = app.client("alice").await.unwrap();
    client.publish_key_package().await.unwrap();
    let event = relay
        .published_events
        .lock()
        .unwrap()
        .iter()
        .find(|e| e.kind == KIND_MARMOT_KEY_PACKAGE)
        .unwrap()
        .clone();
    let mut routes = AccountRelayListStatus::empty();
    routes.nip65.relays = vec!["wss://relay.example".into()];
    app.remember_directory_relay_lists(&account.account_id_hex, &routes)
        .unwrap();
    let mut tampered = event.clone();
    tampered
        .tags
        .push(vec!["client".into(), "Forged label".into()]);
    let mut future = event.clone();
    future.created_at += 24 * 3600;
    future.id = future.computed_id();
    let keys = home.load_signing_keys("alice").unwrap();
    let signed = transport_nostr_peeler::MarmotNostrSigner::sign_event(
        &keys,
        UnsignedEvent::from_json(serde_json::to_string(&future).unwrap()).unwrap(),
    )
    .await
    .unwrap();
    let future = NostrTransportEvent::from_nostr_event(&signed).unwrap();
    assert!(future.to_verified_nostr_event().is_ok());
    *fetcher.events.lock().unwrap() = vec![tampered, future];
    assert!(
        app.clone()
            .run_device_label_pass(vec![account.account_id_hex.clone()])
            .await
            .is_empty()
    );
    let mut events = Vec::new();
    for index in 0..20 {
        let mut slot = event.clone();
        slot.tags
            .iter_mut()
            .find(|tag| tag.first().is_some_and(|t| t == "d"))
            .unwrap()[1] = format!("label-slot-{index}");
        slot.id = slot.computed_id();
        let signed = transport_nostr_peeler::MarmotNostrSigner::sign_event(
            &keys,
            UnsignedEvent::from_json(serde_json::to_string(&slot).unwrap()).unwrap(),
        )
        .await
        .unwrap();
        events.push(NostrTransportEvent::from_nostr_event(&signed).unwrap());
    }
    *fetcher.events.lock().unwrap() = events;
    // The scripted fetcher intentionally ignores the wire limit.
    assert_eq!(
        app.run_device_label_pass(vec![account.account_id_hex])
            .await
            .len(),
        crate::client::device_labels::DEVICE_LABEL_EVENT_LIMIT
    );
}

#[tokio::test]
async fn device_labels_unrelated_passes_do_not_admit_or_notify() {
    let (_dir, app, mut client, group, account) = member_device_label_fixture().await;
    let before = app
        .shared_storage()
        .unwrap()
        .device_client_labels(std::slice::from_ref(&account))
        .unwrap()
        .len();
    let row = app
        .shared_storage()
        .unwrap()
        .device_client_labels(std::slice::from_ref(&account))
        .unwrap()
        .remove(0);
    let id = cgka_traits::message::member_device_id(&group, &row.signature_key);
    client.pending_projection_updates.clear();
    tokio::time::pause();
    for pass in 0..40u8 {
        tokio::time::advance(crate::client::device_labels::DEVICE_LABEL_REFETCH_INTERVAL).await;
        client
            .device_label_enrichment
            .schedule(&account, &group, &id);
        let _ = client.device_label_enrichment.begin();
        client.finish_device_label_pass(vec![crate::client::device_labels::FetchedLabel {
            account: account.clone(),
            key: vec![pass; 32],
            label: "Unrelated".into(),
            created_at: u64::from(pass),
        }]);
        assert!(client.pending_projection_updates.is_empty());
    }
    assert_eq!(
        app.shared_storage()
            .unwrap()
            .device_client_labels(&[account])
            .unwrap()
            .len(),
        before
    );
}

#[tokio::test]
async fn device_labels_unchanged_demand_produces_zero_notifications() {
    let (_dir, app, mut client, group, account) = member_device_label_fixture().await;
    let runtime = MarmotAppRuntime::new(app.clone());
    let mut events = runtime.subscribe_events();
    let row = app
        .shared_storage()
        .unwrap()
        .device_client_labels(std::slice::from_ref(&account))
        .unwrap()
        .remove(0);
    let id = cgka_traits::message::member_device_id(&group, &row.signature_key);
    client
        .device_label_enrichment
        .schedule(&account, &group, &id);
    client.pending_projection_updates.clear();
    assert!(client.device_label_enrichment.begin().contains(&account));
    client.finish_device_label_pass(vec![crate::client::device_labels::FetchedLabel {
        account,
        key: row.signature_key,
        label: row.client_label,
        created_at: row.created_at + 1,
    }]);
    assert!(client.pending_projection_updates.is_empty());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), events.recv())
            .await
            .is_err(),
        "recency-only admission emits no runtime label event"
    );
}

#[tokio::test]
async fn device_labels_read_time_overrides_legacy_values_and_ignores_cache_errors() {
    let (_dir, app, _client, group, account) = member_device_label_fixture().await;
    let cached = app
        .shared_storage()
        .unwrap()
        .device_client_labels(std::slice::from_ref(&account))
        .unwrap()
        .remove(0);
    let device = crate::SenderDevice::Exact {
        device_id: cgka_traits::message::member_device_id(&group, &cached.signature_key),
        leaf_index: 5,
        client_label: Some("Stale persisted label".into()),
        is_this_installation: false,
    };
    let group_hex = hex::encode(group.as_slice());
    let projection = AppMessageProjection {
        authority: None,
        message_id_hex: "legacy-label".into(),
        source_message_id_hex: None,
        direction: "received".into(),
        group_id_hex: group_hex.clone(),
        sender: account,
        sender_device: device.clone(),
        plaintext: "legacy label fixture".into(),
        kind: MARMOT_APP_EVENT_KIND_CHAT,
        tags: Vec::new(),
        source_epoch: Some(1),
        retention: None,
        recorded_at: Some(unix_now_seconds()),
        origin_commit_id: None,
        moderation_grant: false,
    };
    let mut legacy =
        crate::conversions::stored_app_event_from_projection(&projection, unix_now_seconds());
    legacy.sender_device = device.clone();
    app.account_storage("alice")
        .unwrap()
        .record_app_event(&legacy)
        .unwrap();
    let mut expected = device;
    if let crate::SenderDevice::Exact { client_label, .. } = &mut expected {
        *client_label = Some(cached.client_label);
    }
    assert_eq!(
        app.message_by_id("alice", &group_hex, "legacy-label")
            .unwrap()
            .unwrap()
            .sender_device,
        expected
    );
    assert_eq!(
        app.timeline_message("alice", &group_hex, "legacy-label")
            .unwrap()
            .unwrap()
            .sender_device,
        expected
    );
    // Cache resolution also applies to both copies carried in live updates.
    crate::client::device_labels::LABEL_QUERY_COUNT.with(|c| c.set(0));
    let update = app
        .record_account_app_event(
            "alice",
            &AppMessageProjection {
                message_id_hex: "new-label".into(),
                ..projection
            },
        )
        .unwrap();
    assert_eq!(
        crate::client::device_labels::LABEL_QUERY_COUNT.with(|c| c.get()),
        1,
        "message and change copies share candidate queries"
    );
    assert_eq!(update.timeline_messages[0].sender_device, expected);
    assert!(
        matches!(&update.timeline_changes[0], crate::TimelineMessageChange::Upsert { message, .. } if message.sender_device == expected)
    );
    assert!(matches!(
        app.account_storage("alice")
            .unwrap()
            .app_message(&group_hex, "new-label")
            .unwrap()
            .unwrap()
            .sender_device,
        crate::SenderDevice::Exact {
            client_label: None,
            ..
        }
    ));
    app.shared_storage().unwrap().close().unwrap();
    if let crate::SenderDevice::Exact { client_label, .. } = &mut expected {
        *client_label = None;
    }
    assert_eq!(
        app.message_by_id("alice", &group_hex, "legacy-label")
            .unwrap()
            .unwrap()
            .sender_device,
        expected
    );
    assert_eq!(
        app.timeline_message("alice", &group_hex, "legacy-label")
            .unwrap()
            .unwrap()
            .sender_device,
        expected
    );
}

#[tokio::test]
async fn device_labels_offline_upgrade_uses_publication_and_directory() {
    let (dir, app, mut client, group, bob) = member_device_label_fixture().await;
    client.publish_key_package().await.unwrap();
    let own = app.account_home().account("alice").unwrap().account_id_hex;
    let roster = client.members(&group).unwrap();
    for member in &roster {
        let device = &member.devices.as_ref().unwrap()[0];
        app.record_account_app_event(
            "alice",
            &AppMessageProjection {
                authority: None,
                message_id_hex: member.member_id_hex.clone(),
                source_message_id_hex: None,
                direction: "received".into(),
                group_id_hex: hex::encode(group.as_slice()),
                sender: member.member_id_hex.clone(),
                sender_device: crate::SenderDevice::Exact {
                    device_id: device.device_id.clone(),
                    leaf_index: device.leaf_index,
                    client_label: None,
                    is_this_installation: device.is_this_installation,
                },
                plaintext: "upgrade".into(),
                kind: MARMOT_APP_EVENT_KIND_CHAT,
                tags: Vec::new(),
                source_epoch: Some(1),
                retention: None,
                recorded_at: Some(unix_now_seconds()),
                origin_commit_id: None,
                moderation_grant: false,
            },
        )
        .unwrap();
    }
    let cached = app
        .shared_storage()
        .unwrap()
        .device_client_labels(&[own.clone(), bob.clone()])
        .unwrap();
    let conn = rusqlite::Connection::open(app.shared_storage_path()).unwrap();
    conn.execute("DELETE FROM device_client_labels", [])
        .unwrap();
    // The own publication must work even when the directory slot is absent.
    conn.execute(
        "UPDATE directory_users SET key_package_json=NULL WHERE account_id_hex=?1",
        [&own],
    )
    .unwrap();
    for cache in app.directory_caches().unwrap() {
        if let Some(mut entry) = cache.entry(&own).unwrap() {
            entry.key_package = None;
            cache.put_with_reason(&entry, "test").unwrap();
        }
    }
    drop(conn);
    drop(client);
    app.close_storage().unwrap();
    let reopened = app_at(&dir, Arc::new(ScriptedPushRelayClient::default()));
    let client = reopened.client("alice").await.unwrap();
    assert!(
        reopened
            .shared_storage()
            .unwrap()
            .device_client_labels(&[own.clone(), bob.clone()])
            .unwrap()
            .is_empty()
    );
    for account in [own.clone(), bob] {
        let members = client.members(&group).unwrap();
        let member = members.iter().find(|m| m.member_id_hex == account).unwrap();
        let expected = member.devices.as_ref().unwrap()[0].client_label.clone();
        assert!(expected.is_some());
        // Even a newer fallback row cannot override a higher-precedence source.
        let row = cached
            .iter()
            .find(|row| row.account_id_hex == account)
            .unwrap();
        reopened
            .shared_storage()
            .unwrap()
            .put_device_client_label(
                &account,
                &row.signature_key,
                "Lower precedence",
                row.created_at + 1,
            )
            .unwrap();
        let message = reopened
            .message_by_id("alice", &hex::encode(group.as_slice()), &account)
            .unwrap()
            .unwrap();
        assert!(
            matches!(message.sender_device, crate::SenderDevice::Exact { client_label, .. } if client_label == expected),
            "offline sender and roster must agree"
        );
    }
    // Cache churn cannot remove this installation's durable label.
    for i in 0..=storage_sqlite::DEVICE_CLIENT_LABELS_PER_ACCOUNT {
        reopened
            .shared_storage()
            .unwrap()
            .put_device_client_label(&own, &[i as u8; 32], "Sibling", 100 + i as u64)
            .unwrap();
    }
    let own_key = &cached
        .iter()
        .find(|row| row.account_id_hex == own)
        .unwrap()
        .signature_key;
    assert!(
        reopened
            .shared_storage()
            .unwrap()
            .device_client_label(&own, own_key)
            .unwrap()
            .is_none()
    );
    let member = client
        .members(&group)
        .unwrap()
        .into_iter()
        .find(|m| m.member_id_hex == own)
        .unwrap();
    let expected = member.devices.unwrap()[0].client_label.clone();
    let message = reopened
        .message_by_id("alice", &hex::encode(group.as_slice()), &own)
        .unwrap()
        .unwrap();
    assert!(
        matches!(message.sender_device, crate::SenderDevice::Exact { client_label, .. } if client_label == expected && client_label.is_some())
    );
}

#[tokio::test]
async fn device_labels_page_queries_are_bounded_by_distinct_senders() {
    let (_dir, app, client, group, account) = member_device_label_fixture().await;
    let device = client
        .members(&group)
        .unwrap()
        .into_iter()
        .find(|m| m.member_id_hex == account)
        .unwrap()
        .devices
        .unwrap()
        .remove(0);
    for i in 0..100 {
        app.record_account_app_event(
            "alice",
            &AppMessageProjection {
                authority: None,
                message_id_hex: format!("batch-{i}"),
                source_message_id_hex: None,
                direction: "received".into(),
                group_id_hex: hex::encode(group.as_slice()),
                sender: account.clone(),
                sender_device: crate::SenderDevice::Exact {
                    device_id: device.device_id.clone(),
                    leaf_index: device.leaf_index,
                    client_label: None,
                    is_this_installation: false,
                },
                plaintext: "batch".into(),
                kind: MARMOT_APP_EVENT_KIND_CHAT,
                tags: Vec::new(),
                source_epoch: Some(1),
                retention: None,
                recorded_at: Some(unix_now_seconds()),
                origin_commit_id: None,
                moderation_grant: false,
            },
        )
        .unwrap();
    }
    crate::client::device_labels::LABEL_QUERY_COUNT.with(|c| c.set(0));
    let page = app
        .timeline_messages_with_query(
            "alice",
            crate::TimelineMessageQuery {
                group_id_hex: Some(hex::encode(group.as_slice())),
                pagination: crate::TimelinePagination {
                    limit: Some(200),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
    assert!(page.messages.len() >= 100);
    let queries = crate::client::device_labels::LABEL_QUERY_COUNT.with(|c| c.get());
    assert!(
        queries <= 2,
        "one candidate query per distinct sender, got {queries}"
    );
}

#[tokio::test]
async fn device_labels_eviction_refreshes_roster_without_timeline() {
    device_labels_cross_account_roster_regression(false).await;
}

#[tokio::test]
async fn device_labels_admission_and_eviction_refresh_both_local_rosters() {
    device_labels_cross_account_roster_regression(true).await;
}

async fn device_labels_cross_account_roster_regression(two_accounts: bool) {
    let (dir, app, mut alice, group, bob) = member_device_label_fixture().await;
    let other_group = alice.create_group("roster only", &[&bob]).await.unwrap();
    let carol = if two_accounts {
        AccountHome::open(dir.path())
            .create_account("carol")
            .unwrap();
        let mut client = app.client("carol").await.unwrap();
        let group = client.create_group("Carol roster", &[&bob]).await.unwrap();
        Some((client, group))
    } else {
        None
    };
    let cache = app.shared_storage().unwrap();
    let key = cache
        .device_client_labels(std::slice::from_ref(&bob))
        .unwrap()
        .remove(0)
        .signature_key;
    // Force the bounded table to be the resolver source, without changing MLS membership.
    let conn = rusqlite::Connection::open(app.shared_storage_path()).unwrap();
    conn.execute(
        "UPDATE directory_users SET key_package_json=NULL WHERE account_id_hex=?1",
        [&bob],
    )
    .unwrap();
    for directory in app.directory_caches().unwrap() {
        if let Some(mut entry) = directory.entry(&bob).unwrap() {
            entry.key_package = None;
            directory.put_with_reason(&entry, "test").unwrap();
        }
    }
    drop(conn);
    assert!(
        alice
            .members(&other_group)
            .unwrap()
            .iter()
            .find(|m| m.member_id_hex == bob)
            .unwrap()
            .devices
            .as_ref()
            .unwrap()[0]
            .client_label
            .is_some()
    );
    if let Some((client, group)) = &carol {
        assert!(
            client
                .members(group)
                .unwrap()
                .iter()
                .find(|m| m.member_id_hex == bob)
                .unwrap()
                .devices
                .as_ref()
                .unwrap()[0]
                .client_label
                .is_some()
        );
    }
    let runtime = MarmotAppRuntime::new(app.clone());
    let mut label_events = runtime.subscribe_events();
    let mut windows = Vec::new();
    if let Some((client, carol_group)) = &carol {
        for (owner, window_group) in [(&alice, &other_group), (client, carol_group)] {
            let device = owner
                .members(window_group)
                .unwrap()
                .into_iter()
                .find(|m| m.member_id_hex == bob)
                .unwrap()
                .devices
                .unwrap()
                .remove(0);
            app.record_account_app_event(
                &owner.state.label,
                &AppMessageProjection {
                    authority: None,
                    message_id_hex: "shared-label-row".into(),
                    source_message_id_hex: None,
                    direction: "received".into(),
                    group_id_hex: hex::encode(window_group.as_slice()),
                    sender: bob.clone(),
                    sender_device: crate::SenderDevice::Exact {
                        device_id: device.device_id,
                        leaf_index: device.leaf_index,
                        client_label: None,
                        is_this_installation: false,
                    },
                    plaintext: "shared label".into(),
                    kind: MARMOT_APP_EVENT_KIND_CHAT,
                    tags: vec![],
                    source_epoch: Some(1),
                    retention: None,
                    recorded_at: Some(unix_now_seconds()),
                    origin_commit_id: None,
                    moderation_grant: false,
                },
            )
            .unwrap();
            windows.push(
                runtime
                    .subscribe_timeline_messages(
                        &owner.state.label,
                        crate::TimelineMessageQuery {
                            group_id_hex: Some(hex::encode(window_group.as_slice())),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
            );
        }
    }
    // A demanded known leaf changes its label. Both workers must see the change.
    alice.device_label_enrichment.schedule(
        &bob,
        &group,
        &cgka_traits::message::member_device_id(&group, &key),
    );
    alice.device_label_enrichment.begin();
    alice.finish_device_label_pass(vec![crate::client::device_labels::FetchedLabel {
        account: bob.clone(),
        key: key.clone(),
        label: "Updated".into(),
        created_at: u64::MAX / 2,
    }]);
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), label_events.recv())
        .await
        .expect("label admission must reach the host event stream")
        .unwrap();
    assert!(
        matches!(event, crate::MarmotAppEvent::DeviceLabelsChanged { account_id_hex: Some(account) } if account == bob)
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(150), label_events.recv())
            .await
            .is_err(),
        "admission emits one coalesced event"
    );
    assert_eq!(
        alice
            .members(&other_group)
            .unwrap()
            .iter()
            .find(|m| m.member_id_hex == bob)
            .unwrap()
            .devices
            .as_ref()
            .unwrap()[0]
            .client_label
            .as_deref(),
        Some("Updated")
    );
    if let Some((client, group)) = &carol {
        assert_eq!(
            client
                .members(group)
                .unwrap()
                .iter()
                .find(|m| m.member_id_hex == bob)
                .unwrap()
                .devices
                .as_ref()
                .unwrap()[0]
                .client_label
                .as_deref(),
            Some("Updated")
        );
    }

    for window in &mut windows {
        tokio::time::timeout(std::time::Duration::from_secs(5), window.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&window.take_snapshot().messages[0].sender_device, crate::SenderDevice::Exact { client_label: Some(label), .. } if label == "Updated")
        );
    }
    // Reconfirmation changes recency but not either account's displayed labels.
    alice.device_label_enrichment = Default::default();
    alice.device_label_enrichment.schedule(
        &bob,
        &group,
        &cgka_traits::message::member_device_id(&group, &key),
    );
    alice.device_label_enrichment.begin();
    alice.finish_device_label_pass(vec![crate::client::device_labels::FetchedLabel {
        account: bob.clone(),
        key: key.clone(),
        label: "Updated".into(),
        created_at: u64::MAX / 2,
    }]);
    for window in &mut windows {
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), window.recv())
                .await
                .is_err(),
            "unchanged labels must not refresh either account's window"
        );
    }
    // Fill the cache so the next demanded admission evicts Bob's roster leaf.
    for i in 0..storage_sqlite::DEVICE_CLIENT_LABELS_PER_ACCOUNT - 1 {
        cache
            .put_device_client_label(&bob, &[i as u8; 32], "Sibling", 2)
            .unwrap();
    }
    // Reset only the discovery cooldown by using a fresh demand owner.
    alice.device_label_enrichment = Default::default();
    let new_key = vec![254; 32];
    alice.device_label_enrichment.schedule(
        &bob,
        &group,
        &cgka_traits::message::member_device_id(&group, &new_key),
    );
    alice.device_label_enrichment.begin();
    alice.finish_device_label_pass(vec![crate::client::device_labels::FetchedLabel {
        account: bob.clone(),
        key: new_key,
        label: "New".into(),
        created_at: 3,
    }]);
    assert!(cache.device_client_label(&bob, &key).unwrap().is_none());
    let event = tokio::time::timeout(std::time::Duration::from_secs(1), label_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(event, crate::MarmotAppEvent::DeviceLabelsChanged { account_id_hex: Some(account) } if account == bob)
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(150), label_events.recv())
            .await
            .is_err(),
        "eviction emits one coalesced event"
    );
    assert!(
        alice
            .members(&other_group)
            .unwrap()
            .iter()
            .find(|m| m.member_id_hex == bob)
            .unwrap()
            .devices
            .as_ref()
            .unwrap()[0]
            .client_label
            .is_none()
    );
    for window in &mut windows {
        tokio::time::timeout(std::time::Duration::from_secs(5), window.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            &window.take_snapshot().messages[0].sender_device,
            crate::SenderDevice::Exact {
                client_label: None,
                ..
            }
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), window.recv())
                .await
                .is_err()
        );
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(150), label_events.recv())
            .await
            .is_err(),
        "unchanged admission emits nothing"
    );
}

// Historical senders need not remain in the current MLS roster.
async fn round6_device_labels_fixture(surface: &str, overflow: bool) {
    let remote_dir = tempfile::tempdir().unwrap();
    let remote_home = AccountHome::open(remote_dir.path());
    let remote = remote_home.create_account("remote").unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let remote_app = app_at(&remote_dir, relay.clone());
    let mut publisher = remote_app.client("remote").await.unwrap();
    publisher.publish_key_package().await.unwrap();
    let event = relay
        .published_events
        .lock()
        .unwrap()
        .iter()
        .find(|event| event.kind == KIND_MARMOT_KEY_PACKAGE)
        .unwrap()
        .clone();
    let record = crate::RelayEventRecord {
        event: event.clone(),
        endpoints: Vec::new(),
    };
    let (_, metadata) =
        crate::key_package_records::key_package_from_record_with_metadata(&record, |package| {
            cgka_engine::key_package::key_package_metadata(package)
                .map_err(|_| AppError::InvalidKeyPackageEvent("fixture".into()))
        })
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    AccountHome::open(dir.path())
        .create_account("alice")
        .unwrap();
    let fetcher = Arc::new(MemberResolutionDirectoryFetcher::default());
    *fetcher.events.lock().unwrap() = vec![event];
    let mut app = app_at(&dir, relay.clone());
    app.relay_plane = MarmotRelayPlane::new_with_directory_fetcher_for_test(
        Some(Duration::from_secs(120)),
        relay,
        fetcher.clone(),
        false,
    );
    let mut routes = AccountRelayListStatus::empty();
    routes.nip65.relays = vec!["wss://relay.example".into()];
    app.remember_directory_relay_lists(&remote.account_id_hex, &routes)
        .unwrap();
    let group = GroupId::new([19, 6]);
    let group_hex = hex::encode(group.as_slice());
    let count = if overflow { 140 } else { 1 };
    for i in 0..count {
        app.record_account_app_event(
            "alice",
            &AppMessageProjection {
                authority: None,
                message_id_hex: format!("round6-{i}"),
                source_message_id_hex: None,
                direction: "received".into(),
                group_id_hex: group_hex.clone(),
                sender: if i == 0 {
                    remote.account_id_hex.clone()
                } else {
                    format!("{i:064x}")
                },
                sender_device: crate::SenderDevice::Exact {
                    device_id: cgka_traits::message::member_device_id(
                        &group,
                        &metadata.signature_key,
                    ),
                    leaf_index: 0,
                    client_label: None,
                    is_this_installation: false,
                },
                plaintext: "historical sender".into(),
                kind: MARMOT_APP_EVENT_KIND_CHAT,
                tags: Vec::new(),
                source_epoch: Some(1),
                retention: None,
                // Newest (and therefore first) demand is the fetchable remote.
                recorded_at: Some(unix_now_seconds() - i as u64),
                origin_commit_id: None,
                moderation_grant: false,
            },
        )
        .unwrap();
    }
    let runtime = crate::MarmotAppRuntime::new(app.clone());
    runtime.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(120), async {
        while app.device_label_events.demand_receiver_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // No await between page admission and worker processing: overwrite the old
    // 128-row broadcast before its receiver can drain it.
    match surface {
        "page" => {
            let mut page = app
                .timeline_messages_with_query(
                    "alice",
                    crate::TimelineMessageQuery {
                        group_id_hex: Some(group_hex.clone()),
                        pagination: crate::TimelinePagination {
                            limit: Some(200),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(page.messages.len(), count);
            // Explicitly order the batch with fetchable demand first, independent
            // of the timeline's display ordering and timestamp tie-breaks.
            page.messages
                .sort_by_key(|row| row.sender != remote.account_id_hex);
            app.device_label_events.schedule("alice", &page.messages);
        }
        "query" => {
            app.messages_with_query("alice", AppMessageQuery::default())
                .unwrap();
        }
        "id" => {
            app.message_by_id("alice", &group_hex, "round6-0")
                .unwrap()
                .unwrap();
        }
        "timeline" => {
            app.timeline_message("alice", &group_hex, "round6-0")
                .unwrap()
                .unwrap();
        }
        _ => unreachable!(),
    }
    // Wait for admission itself, not a short scheduling window: sorted demand
    // may put this account behind all 139 misses, especially under parallel load.
    let result = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if !app
                .shared_storage()
                .unwrap()
                .device_client_labels(std::slice::from_ref(&remote.account_id_hex))
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    runtime.shutdown().await;
    result.expect("historical Exact sender must be fetched and admitted from this read alone");
    assert!(
        fetcher
            .requests
            .lock()
            .unwrap()
            .iter()
            .flat_map(|r| &r.queries)
            .any(
                |q| q.kind == KIND_MARMOT_KEY_PACKAGE && q.authors.contains(&remote.account_id_hex)
            )
    );
}

#[tokio::test]
async fn device_labels_round6_page_overflow_preserves_early_fetchable_demand() {
    round6_device_labels_fixture("page", true).await;
}
#[tokio::test]
async fn device_labels_round6_messages_query_requests_historical_sender() {
    round6_device_labels_fixture("query", false).await;
}
#[tokio::test]
async fn device_labels_round6_message_by_id_requests_historical_sender() {
    round6_device_labels_fixture("id", false).await;
}
#[tokio::test]
async fn device_labels_round6_timeline_message_requests_historical_sender() {
    round6_device_labels_fixture("timeline", false).await;
}

/// Seeded state is never presented as a successful provisional roster. Verified
/// retained membership is still complete when repair or UI confirmation is pending.
#[tokio::test]
async fn group_roster_completeness_hydration_pending_and_departed_views() {
    let dir = tempfile::tempdir().unwrap();
    AccountHome::open(dir.path())
        .create_account("alice")
        .unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let app = app_at(&dir, relay.clone());
    let mut client = app.client("alice").await.unwrap();
    let group = client
        .create_group("completeness frontier", &[])
        .await
        .unwrap();
    let group_hex = hex::encode(group.as_slice());
    assert!(client.group_roster_session(&group).unwrap().complete);
    drop(client);
    let plane = MarmotRelayPlane::new(None, relay);
    let mut client = app
        .local_client_with_relay_plane_and_hydration("alice", &plane, None, true, None)
        .await
        .unwrap();
    assert!(client.members(&group).is_err());
    assert!(client.group_roster_session(&group).is_err());
    assert!(client.group_read_snapshot().is_err());
    client
        .runtime
        .session_mut()
        .ensure_group_hydrated(&group)
        .unwrap();
    let hydrated = client.group_roster_session(&group).unwrap();
    assert!(hydrated.complete);
    assert!(
        client
            .group_read_snapshot()
            .unwrap()
            .group_roster(&group)
            .unwrap()
            .complete
    );

    // A pending SelfUpdate does not change the admitted account set.
    client
        .runtime
        .session_mut()
        .send(SendIntent::SelfUpdate {
            group_id: group.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        client.runtime.epoch_state(&group),
        Some(cgka_traits::EpochState::PendingPublish(_))
    ));
    assert!(client.group_roster_session(&group).unwrap().complete);

    // A newer stored departure overrides a previously complete worker snapshot.
    let snapshot = client.group_read_snapshot().unwrap();
    for membership in [SelfMembership::Left, SelfMembership::Removed] {
        app.set_group_self_membership("alice", &group_hex, SelfMembership::Member)
            .unwrap();
        app.set_group_self_membership("alice", &group_hex, membership)
            .unwrap();
        let roster = client.group_roster_session(&group).unwrap();
        assert_eq!(roster.group_record.self_membership, membership);
        assert!(!roster.complete);
        let public = crate::groups::app_group_roster_from_session(roster, "", HashMap::new());
        assert!(!public.complete);
        // The internal capture is immutable. Worker responses must overlay
        // current storage and invalidate this cached completeness evidence.
        assert!(snapshot.group_roster(&group).unwrap().complete);
    }
}

/// Unrecoverable freezes canonical MLS; it does not turn known admitted devices
/// into unknown rows or imply a partial roster at that frozen local epoch.
#[tokio::test]
async fn group_roster_completeness_survives_unrecoverable_reopen() {
    let (_dir, app, alice, group, _) = member_device_label_fixture().await;
    let before = alice.group_roster_session(&group).unwrap();
    assert!(before.complete);
    let storage = app.account_storage("alice").unwrap();
    let mut record = storage.get_group(&group).unwrap();
    record.unrecoverable = true;
    storage.put_group(&record).unwrap();
    drop(alice);
    let reopened = app.client("alice").await.unwrap();
    let frozen = reopened.group_roster_session(&group).unwrap();
    assert_eq!(
        frozen.mls_state.lifecycle_state,
        AppGroupLifecycleState::Unrecoverable
    );
    assert!(frozen.complete);
    assert_eq!(frozen.members, before.members);
}
