//! Disk-space outcomes (mdk#2239): a body the device cannot publish parks as
//! `StorageBlocked` with its checkpoint instead of refetching on a timer, and
//! readmits once free space covers the reserve or the user retries.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use storage_sqlite::AttachmentTransferState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MIB: u64 = 1024 * 1024;

struct Fixture {
    _dir: tempfile::TempDir,
    client: AppClient,
    storage: SqliteAccountStorage,
    policy: storage_sqlite::AttachmentDownloadPolicy,
    shared: RuntimeSharedServices,
    http: MediaHttpContext,
    completions: mpsc::UnboundedReceiver<MediaHttpDone>,
    admission: Admission,
}

/// An explicit-only account whose single job fetches `reference` from the
/// loopback fixture, with a durable policy the test can tighten and relax.
async fn explicit_fixture(
    reference: &crate::MediaAttachmentReference,
    transfer_limit: u64,
    disk_reserve: u64,
) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    AccountHome::open(dir.path())
        .create_account("alice")
        .unwrap();
    let app = MarmotApp::with_relay_and_config(
        dir.path(),
        "wss://relay.example",
        MarmotAppConfig {
            attachment_acquisition: Some(crate::AttachmentAcquisitionPolicy::default()),
            allow_loopback_blob_endpoints: true,
            ..Default::default()
        },
    )
    .with_test_relay_client(Arc::new(ScriptedPushRelayClient::default()));
    let mut client = app.client("alice").await.unwrap();
    let storage = app.account_storage("alice").unwrap();
    seed(&storage, reference, false);
    client.state.groups.push(projection(reference));
    storage
        .remember_encrypted_media_epoch_secret(
            GROUP,
            crate::GROUP_ENCRYPTED_MEDIA_V2_COMPONENT_ID,
            3,
            &[7; 32],
        )
        .unwrap();
    let now = crate::unix_now_seconds();
    admit_demands(&storage, now, true).unwrap();
    let asset = storage
        .due_attachment_acquisitions(now, 1)
        .unwrap()
        .remove(0);
    assert!(storage.explicitly_retry_attachment(&asset, now).unwrap());
    let mut policy = crate::runtime::attachment_controls::default_policy(&client.app.config);
    policy.automatic = false;
    policy.transfer_limit = transfer_limit;
    policy.disk_reserve = disk_reserve;
    storage
        .set_attachment_download_policy(&policy, now)
        .unwrap();
    let (http, completions) = context();
    Fixture {
        _dir: dir,
        client,
        storage,
        policy,
        shared: RuntimeSharedServices::default(),
        http,
        completions,
        admission: Admission::default(),
    }
}

impl Fixture {
    /// One scheduler turn plus the global-permit wait it may queue.
    async fn tick(&mut self) {
        schedule(&self.client, &self.shared, &self.http, &mut self.admission).unwrap();
        self.admission.ready().await;
        schedule(&self.client, &self.shared, &self.http, &mut self.admission).unwrap();
    }
    async fn completion(&mut self) -> MediaHttpDone {
        tokio::time::timeout(Duration::from_secs(120), self.completions.recv())
            .await
            .expect("attachment completion")
            .unwrap()
    }
    fn status(&self) -> storage_sqlite::AttachmentTransferStatus {
        self.storage
            .attachment_transfer_status(
                GROUP,
                &"11".repeat(32),
                &"22".repeat(32),
                0,
                crate::unix_now_seconds(),
                false,
            )
            .unwrap()
            .unwrap()
    }
    fn state(&self) -> AttachmentTransferState {
        self.status().state
    }
    fn set_reserve(&mut self, disk_reserve: u64) {
        self.policy.disk_reserve = disk_reserve;
        self.storage
            .set_attachment_download_policy(&self.policy, crate::unix_now_seconds())
            .unwrap();
    }
}

async fn request_headers(socket: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    loop {
        bytes.push(socket.read_u8().await.unwrap());
        assert!(bytes.len() < 16384);
        if bytes.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).unwrap().to_ascii_lowercase()
}

fn loopback_reference(
    listener: &tokio::net::TcpListener,
    plain: &[u8],
) -> (crate::MediaAttachmentReference, Vec<u8>) {
    let (mut reference, cipher) = crate::media::tests::attachment_worker_fixture(plain);
    reference.locators = vec![crate::MediaLocator {
        kind: "blossom-v1".into(),
        value: format!(
            "http://{}/{}",
            listener.local_addr().unwrap(),
            reference.ciphertext_sha256
        ),
    }];
    (reference, cipher)
}

/// The size arrives with the response headers after admission already passed:
/// the explicit transfer above `transfer_limit` parks as `StorageBlocked`
/// without reading the body, makes no further requests while parked, and
/// publishes through the file sink once the reserve fits again.
#[tokio::test]
async fn size_known_explicit_download_parks_without_fetching_the_body_until_space_frees() {
    let plain = vec![0x5a; 24 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let requests = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&requests);
    let (stop, mut stopped) = oneshot::channel::<()>();
    let (arrived, first_request) = oneshot::channel::<()>();
    let (go, proceed) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let mut arrived = Some(arrived);
        let mut proceed = Some(proceed);
        loop {
            let (mut socket, _) = tokio::select! {
                biased;
                _ = &mut stopped => return,
                accepted = listener.accept() => accepted.unwrap(),
            };
            served.fetch_add(1, Ordering::SeqCst);
            let request = request_headers(&mut socket).await;
            assert!(!request.contains("range:"), "{request}");
            // Hold the first response until the test has tightened the reserve,
            // so the size becomes known under pressure that admission never saw.
            if let Some(arrived) = arrived.take() {
                arrived.send(()).unwrap();
                proceed.take().unwrap().await.unwrap();
            }
            let _ = socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                        cipher.len()
                    )
                    .as_bytes(),
                )
                .await;
            // A parked client drops the response before the body; ignore the reset.
            let _ = socket.write_all(&cipher).await;
        }
    });
    let mut fixture = explicit_fixture(&reference, MIB, 0).await;
    fixture.tick().await;
    first_request.await.unwrap();
    // Between the thresholds: the reserve itself is still free (as admission
    // requires), but publishing 24 MiB needs 96 MiB more. Measured right
    // before the response so host free-space drift has ~48 MiB of slack.
    let free = fs4::available_space(fixture._dir.path()).unwrap();
    fixture.set_reserve(free.saturating_sub(48 * MIB));
    go.send(()).unwrap();
    let done = fixture.completion().await;
    let MediaHttpCompletion::Attachment { result, .. } = &done.completion else {
        panic!("attachment completion");
    };
    assert!(
        matches!(
            result,
            Err(AttachmentDownloadFailure::StorageBlocked(bytes)) if *bytes >= plain.len() as u64
        ),
        "a size-known body the device cannot publish parks instead of retrying"
    );
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    // The in-memory sink spends one header-only request before handing the
    // transfer to the file sink, which refuses on the declared length.
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    for _ in 0..3 {
        fixture.tick().await;
        assert!(fixture.completions.try_recv().is_err());
    }
    assert_eq!(
        requests.load(Ordering::SeqCst),
        2,
        "a parked job must not refetch in the background"
    );
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    assert_eq!(fixture.shared.attachment_transfer.available_permits(), 1);

    fixture.set_reserve(0);
    fixture.tick().await;
    let done = fixture.completion().await;
    assert!(
        matches!(
            &done.completion,
            MediaHttpCompletion::Attachment {
                result: Ok(crate::client::AcquiredMediaBody::File(_)),
                ..
            }
        ),
        "explicit bodies above the transfer limit stream through the file sink"
    );
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    let retained = fixture.completion().await;
    assert!(matches!(
        retained.completion,
        MediaHttpCompletion::AttachmentRetained { result: Ok(()), .. }
    ));
    complete_media_http(
        &mut fixture.client,
        retained,
        &fixture.shared,
        &fixture.http,
    )
    .await;
    assert_eq!(fixture.state(), AttachmentTransferState::Ready);
    assert_eq!(
        fixture.storage.retained_attachment_byte_count().unwrap(),
        plain.len() as u64
    );
    let reference = fixture.status().reference.unwrap();
    assert_eq!(
        &*fixture
            .storage
            .read_retained_attachment(&reference, crate::unix_now_seconds(), 0, 64)
            .unwrap()
            .unwrap(),
        &plain[..64]
    );
    assert_eq!(requests.load(Ordering::SeqCst), 4);
    fixture.tick().await;
    assert!(fixture.completions.try_recv().is_err());
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// Space shrinks after the whole body has arrived: the in-memory sink
/// checkpoints the complete ciphertext, the job parks, and the readmitted
/// attempt verifies and publishes locally without a second request.
#[tokio::test]
async fn pressure_after_the_body_arrives_parks_with_a_complete_checkpoint_and_readmits_offline() {
    let plain = vec![0x33; (2 * MIB + 71) as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let (go, proceed) = oneshot::channel::<()>();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request_headers(&mut socket).await;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                    cipher.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let first = MIB as usize;
        socket.write_all(&cipher[..first]).await.unwrap();
        // Let the progress interval elapse so the next chunk reports the
        // received count the test waits for before tightening the reserve.
        tokio::time::sleep(Duration::from_millis(400)).await;
        socket
            .write_all(&cipher[first..first + 4096])
            .await
            .unwrap();
        proceed.await.unwrap();
        socket.write_all(&cipher[first + 4096..]).await.unwrap();
        drop(socket);
        tokio::select! {
            biased;
            connection = listener.accept() => panic!("a checkpointed body was requested again: {connection:?}"),
            _ = stopped => {}
        }
    });
    let mut fixture = explicit_fixture(&reference, 64 * MIB, 0).await;
    fixture.tick().await;
    let started = tokio::time::Instant::now();
    while fixture.status().received < MIB {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{:?}",
            fixture.status()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let unavailable_reserve = fs4::total_space(fixture._dir.path())
        .unwrap()
        .saturating_add(1);
    fixture.set_reserve(unavailable_reserve);
    go.send(()).unwrap();
    let done = fixture.completion().await;
    let MediaHttpCompletion::Attachment { result, .. } = &done.completion else {
        panic!("attachment completion");
    };
    assert!(
        matches!(
            result,
            Err(AttachmentDownloadFailure::StorageBlocked(bytes)) if *bytes == plain.len() as u64
        ),
        "pressure after the body arrived parks the job with its plaintext size"
    );
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    fixture.tick().await;
    assert!(fixture.completions.try_recv().is_err());
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);

    fixture.set_reserve(0);
    fixture.tick().await;
    let done = fixture.completion().await;
    let MediaHttpCompletion::Attachment { result, .. } = &done.completion else {
        panic!("attachment completion");
    };
    let Ok(crate::client::AcquiredMediaBody::Memory(body)) = result else {
        panic!("checkpointed body publishes through the in-memory sink");
    };
    assert_eq!(body.plaintext.len(), plain.len());
    assert!(
        body.plaintext.capacity() <= plain.len() + 16,
        "the body buffer is reserved once at the ciphertext length, capacity {}",
        body.plaintext.capacity()
    );
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    assert_eq!(fixture.state(), AttachmentTransferState::Ready);
    assert_eq!(
        fixture.storage.retained_attachment_byte_count().unwrap(),
        plain.len() as u64
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}
