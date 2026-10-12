//! Bounded reads for verified public-event previews.
//!
//! Only exact event IDs, complete coordinates, NIP-09 kind-5 `#e`/`#a`
//! requests by a known author, and that author's kind-0 metadata can be asked
//! for. Every request uses a fresh anonymous SDK client whose only WebSocket
//! transport is [`PinnedPublicEventTransport`]: endpoints pass
//! [`super::RelaySafetyPolicy`], hostnames are resolved once, every address is
//! validated and the socket is pinned before TLS (see
//! `public_event_transport.rs`). At most four relays, the caller's absolute
//! deadline, a raw wire-byte budget charged below TLS/WebSocket parsing and an
//! item budget charged on decoded messages before the SDK verifies or
//! deduplicates anything. A phase meter is a child of the caller's
//! request-wide meter, so the totals hold across relays and phases. Sockets
//! belong to the request and are reused when connected; each phase retries
//! disconnected original endpoints within its deadline. Cancellation closes all.
//! Nothing here verifies, persists or logs event data; callers own admission.

use std::sync::Arc;
use std::time::Duration;

use cgka_traits::TransportEndpoint;
use nostr_sdk::prelude::{
    AcquisitionLimits, Client as NostrSdkClient, EventId, Filter, Kind, PublicKey, RelayUrl,
    ReqTarget, SingleLetterTag,
};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout, timeout_at};

use super::public_event_transport::{
    CloseSocketsOnDrop, PinnedPublicEventTransport, PublicEventPhaseRoute,
    PublicEventSocketRegistry, PublicEventTrafficLimits, PublicEventTrafficMeter,
};
use super::{DIRECTORY_RELAY_CONNECT_WAIT, RelaySafetyPolicy};

/// Maximum relays one public-event preview request may contact.
pub(crate) const PUBLIC_EVENT_QUERY_MAX_RELAYS: usize = 4;

/// One narrow filter shape. Values are canonical lowercase hex.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublicEventQueryFilter {
    EventId {
        event_id_hex: String,
    },
    Coordinate {
        kind: u16,
        author_hex: String,
        identifier: String,
    },
    EventDeletions {
        event_id_hex: String,
        author_hex: String,
    },
    /// `coordinate` is the NIP-01 `a` value: `kind:pubkey:identifier`.
    CoordinateDeletions {
        coordinate: String,
        author_hex: String,
    },
    AuthorMetadata {
        author_hex: String,
    },
}

/// Received-traffic budget for one phase across all of its relays. The
/// request-wide meter passed alongside it bounds every phase together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PublicEventQueryBudget {
    pub(crate) max_items: usize,
    pub(crate) max_bytes: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PublicEventQueryOutcome {
    /// Distinct serialized events in arrival order, within the budget.
    pub(crate) events_json: Vec<String>,
    /// Raw `EVENT` envelopes this phase received, duplicates, invalid and
    /// over-budget frames included.
    pub(crate) received_items: usize,
    /// Raw wire bytes this phase received (TLS, HTTP upgrade and every
    /// WebSocket frame, fragments included).
    pub(crate) received_bytes: usize,
}

impl super::MarmotRelayPlane {
    /// One anonymous request-owned connection set, shared by bounded phases.
    pub(crate) fn public_event_query_session(
        &self,
        endpoints: Vec<TransportEndpoint>,
        request: &Arc<PublicEventTrafficMeter>,
        deadline: Instant,
    ) -> PublicEventQuerySession {
        let policy = self.inner.relay_safety.clone();
        let endpoints = policy.retain_safe_endpoints(endpoints, "public event query");
        PublicEventQuerySession::new(policy, endpoints, request, deadline)
    }
}

fn sdk_filter(filter: &PublicEventQueryFilter) -> Option<Filter> {
    let tag = |name: char| SingleLetterTag::from_char(name).ok();
    Some(match filter {
        PublicEventQueryFilter::EventId { event_id_hex } => Filter::new()
            .ids([EventId::from_hex(event_id_hex).ok()?])
            .limit(1),
        PublicEventQueryFilter::Coordinate {
            kind,
            author_hex,
            identifier,
        } => {
            let filter = Filter::new()
                .kind(Kind::from(*kind))
                .authors([PublicKey::parse(author_hex).ok()?])
                .limit(4);
            // Ordinary replaceable kinds carry no `d` tag.
            if crate::public_event_preview::is_addressable(u32::from(*kind)) {
                filter.custom_tags(tag('d')?, [identifier.clone()])
            } else {
                filter
            }
        }
        PublicEventQueryFilter::EventDeletions {
            event_id_hex,
            author_hex,
        } => Filter::new()
            .kind(Kind::from(5u16))
            .authors([PublicKey::parse(author_hex).ok()?])
            .custom_tags(tag('e')?, [event_id_hex.clone()])
            .limit(4),
        PublicEventQueryFilter::CoordinateDeletions {
            coordinate,
            author_hex,
        } => Filter::new()
            .kind(Kind::from(5u16))
            .authors([PublicKey::parse(author_hex).ok()?])
            .custom_tags(tag('a')?, [coordinate.clone()])
            .limit(4),
        PublicEventQueryFilter::AuthorMetadata { author_hex } => Filter::new()
            .kind(Kind::from(0u16))
            .authors([PublicKey::parse(author_hex).ok()?])
            .limit(1),
    })
}

/// The sockets remain request-owned, never join a shared pool and are closed
/// on request completion or cancellation. Each phase still has its own meter.
pub(crate) struct PublicEventQuerySession {
    client: NostrSdkClient,
    sockets: Arc<PublicEventSocketRegistry>,
    _close_sockets: CloseSocketsOnDrop,
    route: Arc<PublicEventPhaseRoute>,
    request: Arc<PublicEventTrafficMeter>,
    urls: Vec<RelayUrl>,
    deadline: Instant,
}

impl PublicEventQuerySession {
    fn new(
        policy: RelaySafetyPolicy,
        endpoints: Vec<TransportEndpoint>,
        request: &Arc<PublicEventTrafficMeter>,
        deadline: Instant,
    ) -> Self {
        let mut urls = endpoints
            .iter()
            .filter_map(|endpoint| RelayUrl::parse(endpoint.as_str()).ok())
            .collect::<Vec<_>>();
        urls.sort();
        urls.dedup();
        urls.truncate(PUBLIC_EVENT_QUERY_MAX_RELAYS);
        let sockets = PublicEventSocketRegistry::new();
        let route = PublicEventPhaseRoute::new(request);
        let client = NostrSdkClient::builder()
            .websocket_transport(PinnedPublicEventTransport::new(
                policy,
                route.clone(),
                sockets.clone(),
                deadline,
            ))
            .build();
        Self {
            client,
            _close_sockets: CloseSocketsOnDrop(sockets.clone()),
            sockets,
            route,
            request: request.clone(),
            urls,
            deadline,
        }
    }

    pub(crate) async fn query(
        &mut self,
        filters: Vec<PublicEventQueryFilter>,
        budget: PublicEventQueryBudget,
        deadline: Instant,
    ) -> PublicEventQueryOutcome {
        let mut outcome = PublicEventQueryOutcome::default();
        let deadline = deadline.min(self.deadline);
        if budget.max_items == 0
            || budget.max_bytes == 0
            || filters.is_empty()
            || self.request.is_exhausted()
            || Instant::now() >= deadline
        {
            return outcome;
        }
        let Some(filters) = filters.iter().map(sdk_filter).collect::<Option<Vec<_>>>() else {
            return outcome;
        };
        let phase = PublicEventTrafficMeter::child(
            &self.request,
            PublicEventTrafficLimits {
                max_items: budget.max_items,
                max_bytes: budget.max_bytes,
                max_messages: usize::MAX,
            },
        );
        self.route.route_to(&phase);
        // Switching to the fresh phase precedes reconnection: exhaustion of the
        // target share must not prevent acquisition of reserved deletion evidence.
        let connected = connect(&self.client, self.urls.clone(), deadline).await;
        let _ = timeout_at(
            deadline,
            acquire(
                &self.client,
                &connected,
                filters,
                budget,
                deadline,
                &mut outcome,
            ),
        )
        .await;
        outcome.received_items = phase.items();
        outcome.received_bytes = phase.bytes();
        outcome
    }

    pub(crate) async fn shutdown(&self) {
        self.sockets.close_all();
        shutdown_before_deadline(self.deadline, self.client.shutdown()).await;
    }
}

#[cfg(test)]
pub(super) async fn query_public_events_on(
    policy: RelaySafetyPolicy,
    endpoints: Vec<TransportEndpoint>,
    filters: Vec<PublicEventQueryFilter>,
    budget: PublicEventQueryBudget,
    request: &Arc<PublicEventTrafficMeter>,
    deadline: Instant,
) -> PublicEventQueryOutcome {
    let mut session = PublicEventQuerySession::new(policy, endpoints, request, deadline);
    let outcome = session.query(filters, budget, deadline).await;
    session.shutdown().await;
    outcome
}

async fn shutdown_before_deadline(
    deadline: Instant,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let _ = timeout_at(deadline, shutdown).await;
}

async fn connect(client: &NostrSdkClient, urls: Vec<RelayUrl>, deadline: Instant) -> Vec<RelayUrl> {
    let mut connects = JoinSet::new();
    for url in urls {
        if client.add_relay(url.clone()).await.is_err() {
            continue;
        }
        let client = client.clone();
        connects.spawn(async move {
            let connected = matches!(
                timeout(
                    DIRECTORY_RELAY_CONNECT_WAIT,
                    client.try_connect_relay(url.clone(), DIRECTORY_RELAY_CONNECT_WAIT),
                )
                .await,
                Ok(Ok(()))
            );
            (url, connected)
        });
    }
    let mut connected = Vec::new();
    while let Ok(Some(result)) = timeout_at(deadline, connects.join_next()).await {
        if let Ok((url, true)) = result {
            connected.push(url);
        }
    }
    connected
}

async fn acquire(
    client: &NostrSdkClient,
    connected: &[RelayUrl],
    filters: Vec<Filter>,
    budget: PublicEventQueryBudget,
    deadline: Instant,
    outcome: &mut PublicEventQueryOutcome,
) {
    if connected.is_empty() {
        return;
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return;
    }
    // The SDK's own acquisition bounds are a second, retained-event limit; the
    // pinned transport has already charged every raw frame.
    let per_relay_items = (budget.max_items / connected.len()).max(1);
    let per_relay_bytes = (budget.max_bytes / connected.len()).max(1);
    let relay_count = connected.len();
    let target = ReqTarget::manual(connected.iter().cloned().map(|url| (url, filters.clone())));
    let Ok(handle) = client
        .acquire_events(
            target,
            AcquisitionLimits::new(
                relay_count,
                per_relay_items,
                per_relay_bytes,
                remaining.mul_f32(0.9).max(Duration::from_millis(1)),
            ),
        )
        .await
    else {
        return;
    };
    let Ok(report) = handle.finish().await else {
        return;
    };
    let mut retained_bytes = 0usize;
    for relay in report.relays.values() {
        for event in &relay.events {
            let json = event.as_json();
            if outcome.events_json.contains(&json) {
                continue;
            }
            retained_bytes = retained_bytes.saturating_add(json.len());
            if outcome.events_json.len() >= budget.max_items || retained_bytes > budget.max_bytes {
                return;
            }
            outcome.events_json.push(json);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr_sdk::local_relay::MockRelay;
    use nostr_sdk::prelude::{EventBuilder, FinalizeEvent, Keys};

    fn request_meter() -> Arc<PublicEventTrafficMeter> {
        PublicEventTrafficMeter::new(PublicEventTrafficLimits {
            max_items: 16,
            max_bytes: 4 * 1024 * 1024,
            max_messages: 64,
        })
    }

    async fn published_notes(count: usize) -> (MockRelay, TransportEndpoint, Vec<String>) {
        let relay = MockRelay::run().await.unwrap();
        let url = relay.url().await;
        let keys = Keys::generate();
        let publisher = NostrSdkClient::default();
        publisher.add_relay(url.clone()).await.unwrap();
        publisher.connect_relay(url.clone()).await.unwrap();
        let mut ids = Vec::new();
        for index in 0..count {
            let event = EventBuilder::new(Kind::TextNote, format!("note {index}"))
                .finalize(&keys)
                .unwrap();
            ids.push(event.id.to_hex());
            publisher.send_event(&event).await.unwrap();
        }
        publisher.shutdown().await;
        (relay, TransportEndpoint(url.to_string()), ids)
    }

    fn id_filters(ids: &[String]) -> Vec<PublicEventQueryFilter> {
        ids.iter()
            .map(|id| PublicEventQueryFilter::EventId {
                event_id_hex: id.clone(),
            })
            .collect()
    }

    #[tokio::test]
    async fn public_event_query_matches_exact_ids_and_charges_its_budget() {
        let (_relay, endpoint, ids) = published_notes(3).await;
        let policy = RelaySafetyPolicy::with_allow_loopback(true);
        let deadline = Instant::now() + Duration::from_secs(5);

        let request = request_meter();
        let exact = query_public_events_on(
            policy.clone(),
            vec![endpoint.clone()],
            id_filters(&ids[1..2]),
            PublicEventQueryBudget {
                max_items: 8,
                max_bytes: 1024 * 1024,
            },
            &request,
            deadline,
        )
        .await;
        assert_eq!(exact.events_json.len(), 1);
        assert!(exact.events_json[0].contains(&ids[1]));
        assert!(exact.received_items >= 1);
        assert!(exact.received_bytes >= exact.events_json[0].len());

        let request = request_meter();
        let bounded = query_public_events_on(
            policy,
            vec![endpoint],
            id_filters(&ids),
            PublicEventQueryBudget {
                max_items: 2,
                max_bytes: 1024 * 1024,
            },
            &request,
            deadline,
        )
        .await;
        assert!(bounded.events_json.len() <= 2);
        assert!(bounded.received_items <= 3);
        assert_eq!(request.items(), bounded.received_items);
    }

    #[tokio::test]
    async fn refused_relays_do_not_reduce_a_connected_relays_response_budget() {
        let (_relay, endpoint, ids) = published_notes(3).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let refused = TransportEndpoint(format!("ws://{}", listener.local_addr().unwrap()));
        drop(listener);
        let request = request_meter();
        let outcome = query_public_events_on(
            RelaySafetyPolicy::with_allow_loopback(true),
            vec![endpoint, refused],
            id_filters(&ids),
            PublicEventQueryBudget {
                max_items: 3,
                max_bytes: 1024 * 1024,
            },
            &request,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        assert_eq!(
            outcome.events_json.len(),
            3,
            "failed connections receive no share of the response budget"
        );
    }

    #[tokio::test]
    async fn a_relay_disconnected_between_phases_is_reconnected_for_evidence() {
        let (_live, live, ids) = published_notes(3).await;
        let (_other, other, _) = published_notes(1).await;
        let request = request_meter();
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut session = PublicEventQuerySession::new(
            RelaySafetyPolicy::with_allow_loopback(true),
            vec![live, other.clone()],
            &request,
            deadline,
        );
        let first = session
            .query(
                id_filters(&ids[..1]),
                PublicEventQueryBudget {
                    max_items: 8,
                    max_bytes: 1024 * 1024,
                },
                deadline,
            )
            .await;
        assert_eq!(first.events_json.len(), 1);
        session
            .client
            .disconnect_relay(RelayUrl::parse(other.as_str()).unwrap())
            .await
            .unwrap();
        let evidence = session
            .query(
                id_filters(&ids),
                PublicEventQueryBudget {
                    max_items: 6,
                    max_bytes: 1024 * 1024,
                },
                deadline,
            )
            .await;
        let reconnected = session
            .client
            .relay(RelayUrl::parse(other.as_str()).unwrap())
            .await
            .unwrap()
            .unwrap()
            .status()
            .is_connected();
        session.shutdown().await;
        assert!(
            reconnected,
            "the evidence phase retries every disconnected original endpoint"
        );
        assert_eq!(
            evidence.events_json.len(),
            3,
            "all requested evidence remains available after reconnection"
        );
    }

    #[tokio::test]
    async fn exhausted_target_phase_reconnects_for_reserved_evidence() {
        use futures::{SinkExt, StreamExt};
        let keys = Keys::generate();
        let note = EventBuilder::new(Kind::TextNote, "evidence")
            .finalize(&keys)
            .unwrap();
        let id = note.id.to_hex();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = TransportEndpoint(format!("ws://{}", listener.local_addr().unwrap()));
        let server = tokio::spawn(async move {
            for connection in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = pinned_tokio_tungstenite::accept_async(socket)
                    .await
                    .unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    let Ok(text) = message.to_text() else {
                        continue;
                    };
                    let Ok(frame) = serde_json::from_str::<serde_json::Value>(text) else {
                        continue;
                    };
                    if frame[0] != "REQ" {
                        continue;
                    }
                    let subscription = frame[1].as_str().unwrap();
                    if connection == 0 {
                        for _ in 0..9 {
                            let junk = serde_json::json!(["EVENT", subscription, {}]);
                            if ws
                                .send(pinned_tokio_tungstenite::tungstenite::Message::text(
                                    junk.to_string(),
                                ))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        break;
                    }
                    let event = serde_json::json!([
                        "EVENT",
                        subscription,
                        serde_json::from_str::<serde_json::Value>(&note.as_json()).unwrap()
                    ]);
                    ws.send(pinned_tokio_tungstenite::tungstenite::Message::text(
                        event.to_string(),
                    ))
                    .await
                    .unwrap();
                    ws.send(pinned_tokio_tungstenite::tungstenite::Message::text(
                        serde_json::json!(["EOSE", subscription]).to_string(),
                    ))
                    .await
                    .unwrap();
                    while ws.next().await.is_some() {}
                    return;
                }
            }
        });
        let request = request_meter();
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut session = PublicEventQuerySession::new(
            RelaySafetyPolicy::with_allow_loopback(true),
            vec![endpoint],
            &request,
            deadline,
        );
        let target = session
            .query(
                id_filters(std::slice::from_ref(&id)),
                PublicEventQueryBudget {
                    max_items: 1,
                    max_bytes: 1024 * 1024,
                },
                Instant::now() + Duration::from_secs(3),
            )
            .await;
        assert!(target.received_items > 1, "junk exhausts the target share");
        assert!(
            !request.is_exhausted(),
            "the request retains reserved evidence capacity"
        );
        let evidence = session
            .query(
                id_filters(std::slice::from_ref(&id)),
                PublicEventQueryBudget {
                    max_items: 8,
                    max_bytes: 1024 * 1024,
                },
                deadline,
            )
            .await;
        session.shutdown().await;
        server.abort();
        assert_eq!(
            evidence.events_json.len(),
            1,
            "fresh-phase reconnection recovers the signed evidence"
        );
        assert!(evidence.events_json[0].contains(&id));
    }

    #[tokio::test]
    async fn the_request_meter_is_shared_across_phases() {
        let (_relay, endpoint, ids) = published_notes(2).await;
        let policy = RelaySafetyPolicy::with_allow_loopback(true);
        let deadline = Instant::now() + Duration::from_secs(5);
        let phase = PublicEventQueryBudget {
            max_items: 8,
            max_bytes: 1024 * 1024,
        };
        let request = PublicEventTrafficMeter::new(PublicEventTrafficLimits {
            max_items: 1,
            max_bytes: 4 * 1024 * 1024,
            max_messages: 64,
        });
        let first = query_public_events_on(
            policy.clone(),
            vec![endpoint.clone()],
            id_filters(&ids[..1]),
            phase,
            &request,
            deadline,
        )
        .await;
        assert_eq!(first.events_json.len(), 1);
        let second = query_public_events_on(
            policy,
            vec![endpoint],
            id_filters(&ids[1..]),
            phase,
            &request,
            deadline,
        )
        .await;
        assert!(
            second.events_json.is_empty(),
            "the second phase cannot exceed the request-wide item budget"
        );
        assert!(request.is_exhausted());
        assert_eq!(
            request.items(),
            first.received_items + second.received_items
        );
    }

    #[tokio::test]
    async fn the_pinned_transport_refuses_loopback_without_the_dev_flag() {
        let (_relay, endpoint, ids) = published_notes(1).await;
        let request = request_meter();
        let outcome = query_public_events_on(
            RelaySafetyPolicy::default(),
            vec![endpoint],
            id_filters(&ids),
            PublicEventQueryBudget {
                max_items: 8,
                max_bytes: 1024 * 1024,
            },
            &request,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
        assert_eq!(outcome, PublicEventQueryOutcome::default());
        assert_eq!(request.items() + request.bytes(), 0, "nothing was received");
    }

    #[tokio::test]
    async fn public_event_query_rejects_malformed_filters_without_dialing() {
        let request = request_meter();
        let outcome = query_public_events_on(
            RelaySafetyPolicy::default(),
            vec![TransportEndpoint("wss://relay.example".to_owned())],
            vec![PublicEventQueryFilter::EventId {
                event_id_hex: "not-hex".to_owned(),
            }],
            PublicEventQueryBudget {
                max_items: 1,
                max_bytes: 1,
            },
            &request,
            Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert_eq!(outcome, PublicEventQueryOutcome::default());
    }

    #[tokio::test(start_paused = true)]
    async fn public_event_preview_regression_sdk_cleanup_obeys_deadline() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct CleanupWitness(Arc<AtomicBool>);
        impl Drop for CleanupWitness {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let witness = CleanupWitness(dropped.clone());
        let started = Instant::now();
        shutdown_before_deadline(started + Duration::from_secs(1), async move {
            let _witness = witness;
            std::future::pending::<()>().await;
        })
        .await;
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert!(
            dropped.load(Ordering::SeqCst),
            "expired cleanup is cancelled"
        );
    }

    #[test]
    fn replaceable_coordinates_omit_the_identifier_tag() {
        let author = Keys::generate().public_key().to_hex();
        let replaceable = sdk_filter(&PublicEventQueryFilter::Coordinate {
            kind: 10002,
            author_hex: author.clone(),
            identifier: String::new(),
        })
        .unwrap();
        let addressable = sdk_filter(&PublicEventQueryFilter::Coordinate {
            kind: 30023,
            author_hex: author,
            identifier: "entry".to_owned(),
        })
        .unwrap();
        let d = SingleLetterTag::from_char('d').unwrap();
        assert!(!replaceable.generic_tags.contains_key(&d));
        assert!(
            addressable
                .generic_tags
                .get(&d)
                .is_some_and(|values| values.contains("entry"))
        );
    }
}
