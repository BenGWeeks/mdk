//! Disk-space outcomes (mdk#2239): a body the device cannot publish parks as
//! `StorageBlocked` with its checkpoint instead of refetching on a timer, and
//! readmits once free space covers its recorded target or the user retries.
//!
//! Free space is pinned per account directory through the test seam in
//! `attachment_resume`, so the window tests place it exactly where the first
//! implementation looped: above the admission reserve and the bare `4 x body`
//! publication check, but below what the attempt's own staging and
//! checkpoints add on top. The row must park once and stay parked across
//! scheduler ticks until space frees.
use super::*;
use crate::media::attachment_resume::test_free_space;
use std::sync::atomic::{AtomicUsize, Ordering};
use storage_sqlite::{AttachmentTransferState, STORAGE_READMIT_MARGIN_BYTES};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MIB: u64 = 1024 * 1024;
const PLENTY: u64 = 1 << 40;

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

impl Drop for Fixture {
    fn drop(&mut self) {
        test_free_space::clear(self._dir.path());
    }
}

/// An account whose single job fetches `reference` from the loopback fixture,
/// with a durable policy the test can tighten and relax and pinned free space.
async fn fixture(
    reference: &crate::MediaAttachmentReference,
    transfer_limit: u64,
    explicit: bool,
) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    test_free_space::set(dir.path(), PLENTY);
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
    if explicit {
        assert!(storage.explicitly_retry_attachment(&asset, now).unwrap());
    }
    let mut policy = crate::runtime::attachment_controls::default_policy(&client.app.config);
    policy.automatic = !explicit;
    policy.transfer_limit = transfer_limit;
    policy.disk_reserve = 0;
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

async fn explicit_fixture(
    reference: &crate::MediaAttachmentReference,
    transfer_limit: u64,
) -> Fixture {
    fixture(reference, transfer_limit, true).await
}

impl Fixture {
    /// One scheduler turn plus the global-permit wait it may queue.
    async fn tick(&mut self) {
        schedule(&self.client, &self.shared, &self.http, &mut self.admission).unwrap();
        self.admission.ready().await;
        schedule(&self.client, &self.shared, &self.http, &mut self.admission).unwrap();
    }
    async fn completion(&mut self) -> MediaHttpDone {
        tokio::time::timeout(Duration::from_secs(180), self.completions.recv())
            .await
            .expect("attachment completion")
            .unwrap()
    }
    /// Tick until a readmitted row becomes due and its attempt completes; a
    /// repeated park waits out its park streak first.
    async fn completion_after_backoff(&mut self) -> MediaHttpDone {
        let started = tokio::time::Instant::now();
        loop {
            self.tick().await;
            if let Ok(done) =
                tokio::time::timeout(Duration::from_secs(2), self.completions.recv()).await
            {
                return done.unwrap();
            }
            assert!(
                started.elapsed() < Duration::from_secs(240),
                "no completion within the park streak backoff"
            );
        }
    }
    fn status(&self) -> storage_sqlite::AttachmentTransferStatus {
        self.storage
            .attachment_transfer_status(
                GROUP,
                &"11".repeat(32),
                &"22".repeat(32),
                0,
                crate::unix_now_seconds(),
                self.policy.automatic,
            )
            .unwrap()
            .unwrap()
    }
    fn state(&self) -> AttachmentTransferState {
        self.status().state
    }
    /// Pin the free space every capacity decision and readmission will see.
    fn set_free(&self, free: u64) {
        test_free_space::set(self._dir.path(), free);
    }
    /// Take the parked completion, then prove the row stays parked and silent
    /// across scheduler ticks. Returns the recorded headroom.
    async fn park_and_hold(&mut self, requests: &AtomicUsize) -> u64 {
        let done = self.completion().await;
        let headroom = parked_headroom(&done);
        complete_media_http(&mut self.client, done, &self.shared, &self.http).await;
        assert_eq!(self.state(), AttachmentTransferState::StorageBlocked);
        let parked_requests = requests.load(Ordering::SeqCst);
        for _ in 0..3 {
            self.tick().await;
            assert!(self.completions.try_recv().is_err());
        }
        assert_eq!(
            requests.load(Ordering::SeqCst),
            parked_requests,
            "a parked job must not refetch in the background"
        );
        assert_eq!(self.state(), AttachmentTransferState::StorageBlocked);
        assert_eq!(self.shared.attachment_transfer.available_permits(), 1);
        headroom
    }
    /// Drive a file-sink completion through retention to `Ready`.
    async fn publish_file(&mut self, plain: &[u8]) {
        let done = self.completion().await;
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
        complete_media_http(&mut self.client, done, &self.shared, &self.http).await;
        let retained = self.completion().await;
        assert!(matches!(
            retained.completion,
            MediaHttpCompletion::AttachmentRetained { result: Ok(()), .. }
        ));
        complete_media_http(&mut self.client, retained, &self.shared, &self.http).await;
        self.assert_ready(plain);
    }
    /// Drive an in-memory completion to `Ready` and return its buffer capacity.
    async fn publish_memory(&mut self, plain: &[u8]) -> usize {
        let done = self.completion().await;
        let MediaHttpCompletion::Attachment { result, .. } = &done.completion else {
            panic!("attachment completion");
        };
        let Ok(crate::client::AcquiredMediaBody::Memory(body)) = result else {
            panic!("bodies within the transfer limit publish through the in-memory sink");
        };
        assert_eq!(body.plaintext.len(), plain.len());
        let capacity = body.plaintext.capacity();
        complete_media_http(&mut self.client, done, &self.shared, &self.http).await;
        self.assert_ready(plain);
        capacity
    }
    fn assert_ready(&self, plain: &[u8]) {
        assert_eq!(self.state(), AttachmentTransferState::Ready);
        assert_eq!(
            self.storage.retained_attachment_byte_count().unwrap(),
            plain.len() as u64
        );
        let reference = self.status().reference.unwrap();
        assert_eq!(
            &*self
                .storage
                .read_retained_attachment(&reference, crate::unix_now_seconds(), 0, 64)
                .unwrap()
                .unwrap(),
            &plain[..64]
        );
    }
}

fn parked_headroom(done: &MediaHttpDone) -> u64 {
    let MediaHttpCompletion::Attachment { result, .. } = &done.completion else {
        panic!("attachment completion");
    };
    match result {
        Err(AttachmentDownloadFailure::StorageBlocked { headroom, .. }) => *headroom,
        Err(other) => panic!("a body the device cannot publish parks: {other:?}"),
        Ok(_) => panic!("a body the device cannot publish must not publish"),
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

#[derive(Clone, Copy)]
struct Framing {
    etag: bool,
    chunked: bool,
}

/// Serve `cipher` for every request until `stop`, counting requests. Writes are
/// tolerant: a parked client drops the response early. A `Range` request gets
/// the matching suffix as a validated 206.
fn serve(
    listener: tokio::net::TcpListener,
    cipher: Vec<u8>,
    framing: Framing,
    requests: Arc<AtomicUsize>,
    mut stop: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = tokio::select! {
                biased;
                _ = &mut stop => return,
                accepted = listener.accept() => accepted.unwrap(),
            };
            requests.fetch_add(1, Ordering::SeqCst);
            let request = request_headers(&mut socket).await;
            let etag = if framing.etag { "ETag: \"v1\"\r\n" } else { "" };
            let range = request
                .lines()
                .find_map(|line| line.strip_prefix("range: bytes="))
                .and_then(|value| value.trim_end_matches('-').parse::<usize>().ok());
            if let Some(start) = range {
                assert!(framing.etag && !framing.chunked, "{request}");
                let _ = socket
                    .write_all(
                        format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{}/{}\r\n{etag}Connection: close\r\n\r\n",
                            cipher.len() - start,
                            cipher.len() - 1,
                            cipher.len()
                        )
                        .as_bytes(),
                    )
                    .await;
                let _ = socket.write_all(&cipher[start..]).await;
                continue;
            }
            let length = if framing.chunked {
                "Transfer-Encoding: chunked\r\n".to_owned()
            } else {
                format!("Content-Length: {}\r\n", cipher.len())
            };
            let _ = socket
                .write_all(
                    format!("HTTP/1.1 200 OK\r\n{length}{etag}Connection: close\r\n\r\n")
                        .as_bytes(),
                )
                .await;
            if framing.chunked {
                for chunk in cipher.chunks(64 * 1024) {
                    if socket
                        .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                        .await
                        .is_err()
                        || socket.write_all(chunk).await.is_err()
                        || socket.write_all(b"\r\n").await.is_err()
                    {
                        break;
                    }
                }
                let _ = socket.write_all(b"0\r\n\r\n").await;
            } else {
                let _ = socket.write_all(&cipher).await;
            }
        }
    })
}

/// File sink, no validator, free space inside the old loop window
/// `[4P, 5P)`: the first implementation downloaded until its own staging
/// tripped the reserve, parked with the bytes received, and was readmitted the
/// same tick. Now the declared size parks the job before any body byte, the
/// target includes the staging a retry needs, the row stays parked, and the
/// readmitted attempt opens the file sink directly.
#[tokio::test]
async fn file_sink_without_validator_parks_once_inside_the_loop_window() {
    let plain = vec![0x5a; 32 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let total = cipher.len() as u64;
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: false,
            chunked: false,
        },
        Arc::clone(&requests),
        stopped,
    );
    let mut fixture = explicit_fixture(&reference, MIB).await;
    // 4.25 bodies of headroom: enough for the bare publication reserve, not
    // for the ciphertext file a retry stages on top of it.
    fixture.set_free(total * 17 / 4);
    fixture.tick().await;
    let headroom = fixture.park_and_hold(&requests).await;
    assert_eq!(
        headroom,
        5 * total,
        "the target is the reserve plus one staged body"
    );
    // The in-memory sink spends one header-only request before handing the
    // transfer to the file sink, which refuses on the declared length.
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    fixture.set_free(5 * total + STORAGE_READMIT_MARGIN_BYTES);
    fixture.tick().await;
    fixture.publish_file(&plain).await;
    assert_eq!(
        requests.load(Ordering::SeqCst),
        3,
        "the readmitted attempt remembers the file sink and skips the header probe"
    );
    fixture.tick().await;
    assert!(fixture.completions.try_recv().is_err());
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// In-memory sink with a validator, free space inside `[4P, 5P)`: the header
/// check counts the checkpoint the download would add, so the job parks before
/// fetching anything, the readmission wakes transfer subscribers, and the retry
/// is a single fresh request.
#[tokio::test]
async fn memory_sink_with_validator_parks_at_the_header_inside_the_loop_window() {
    let plain = vec![0x6b; 24 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let total = cipher.len() as u64;
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: true,
            chunked: false,
        },
        Arc::clone(&requests),
        stopped,
    );
    // The transfer limit is the body itself, so admission (four limits) passes
    // inside the window while the header check (four bodies plus the
    // checkpoint) does not.
    let mut fixture = explicit_fixture(&reference, total).await;
    fixture.set_free(total * 17 / 4);
    fixture.tick().await;
    let headroom = fixture.park_and_hold(&requests).await;
    assert_eq!(
        headroom,
        4 * plain.len() as u64 + total,
        "the target is the reserve plus the checkpoint a validated body adds"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 1, "nothing was fetched");
    // Subscribers learn about readmission from the shared update channel, not
    // from the one-second refresh set reserved for active transfers.
    let mut updates = fixture.shared.attachment_updates.subscribe();
    updates.borrow_and_update();
    fixture.set_free(headroom + STORAGE_READMIT_MARGIN_BYTES);
    schedule(
        &fixture.client,
        &fixture.shared,
        &fixture.http,
        &mut fixture.admission,
    )
    .unwrap();
    assert!(
        updates.has_changed().unwrap(),
        "readmission must wake transfer subscriptions"
    );
    assert_eq!(fixture.state(), AttachmentTransferState::Queued);
    fixture.admission.ready().await;
    schedule(
        &fixture.client,
        &fixture.shared,
        &fixture.http,
        &mut fixture.admission,
    )
    .unwrap();
    let capacity = fixture.publish_memory(&plain).await;
    assert!(capacity <= plain.len() + 16);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// File sink, chunked body with neither length nor validator: the job parks
/// mid-transfer at the first checkpoint boundary its staging cannot cover and
/// does not refetch until space frees. Such a body restarts from zero, so each
/// later park records at least twice the previous target and a disk that
/// empties exactly as fast as each park asks is refetched a logarithmic number
/// of times, not once per few MiB.
#[tokio::test]
async fn file_sink_with_unknown_length_restarts_a_logarithmic_number_of_times() {
    let plain = vec![0x7c; 16 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let total = cipher.len() as u64;
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: false,
            chunked: true,
        },
        Arc::clone(&requests),
        stopped,
    );
    let mut fixture = explicit_fixture(&reference, MIB).await;
    // Twelve MiB of headroom: the reserve alone passes, the body trips the
    // staged-bytes check a few checkpoints in.
    fixture.set_free(12 * MIB);
    fixture.tick().await;
    let mut target = fixture.park_and_hold(&requests).await;
    assert!(
        target >= 5 * MIB && target < 5 * total,
        "the target projects the bytes received so far: {target}"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    let mut parks = 1;
    loop {
        // One byte short of the recorded target keeps the row parked and
        // silent, which pins the target the storage layer recorded.
        fixture.set_free(target + STORAGE_READMIT_MARGIN_BYTES - 1);
        let before = requests.load(Ordering::SeqCst);
        fixture.tick().await;
        assert!(fixture.completions.try_recv().is_err());
        assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
        assert_eq!(requests.load(Ordering::SeqCst), before);
        // Free exactly what the last park asked for, as a slowly emptying disk would.
        fixture.set_free(target + STORAGE_READMIT_MARGIN_BYTES);
        let done = fixture.completion_after_backoff().await;
        match &done.completion {
            MediaHttpCompletion::Attachment {
                result: Err(AttachmentDownloadFailure::StorageBlocked { headroom, .. }),
                ..
            } => {
                // This attempt saw more bytes than the last one, and storage
                // records at least twice the previous target.
                assert!(*headroom > target, "{headroom} after {target}");
                target = (*headroom).max(2 * target);
                parks += 1;
                complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http)
                    .await;
                assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
            }
            MediaHttpCompletion::Attachment {
                result: Ok(crate::client::AcquiredMediaBody::File(_)),
                ..
            } => {
                complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http)
                    .await;
                let retained = fixture.completion().await;
                complete_media_http(
                    &mut fixture.client,
                    retained,
                    &fixture.shared,
                    &fixture.http,
                )
                .await;
                fixture.assert_ready(&plain);
                break;
            }
            _ => panic!("unexpected completion"),
        }
        assert!(parks <= 4, "parks must stay logarithmic in the body size");
    }
    // Twelve MiB of headroom against a 16 MiB body: targets of 20, 40 and
    // 80 MiB reach the 64 MiB the body needs in three parks, where recording
    // only each attempt's own projection (20, 35, 50, 70 MiB) took four.
    assert_eq!(parks, 3);
    assert_eq!(
        requests.load(Ordering::SeqCst),
        2 + parks,
        "one request per restart after the first probe"
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// File sink with a strong validator and a saved prefix: a job parked by the
/// mid-transfer check and readmitted at exactly its target plus the margin
/// restores the prefix, resumes with a Range request and publishes without
/// parking again.
#[tokio::test]
async fn readmitted_file_checkpoint_resumes_without_parking_again() {
    use crate::media::attachment_resume::AttachmentResume;
    use sha2::{Digest, Sha256};
    let plain = vec![0x5a; 20 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let total = cipher.len() as u64;
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let server = serve(
        listener,
        cipher.clone(),
        Framing {
            etag: true,
            chunked: false,
        },
        Arc::clone(&requests),
        stopped,
    );
    let mut fixture = explicit_fixture(&reference, MIB).await;
    let now = crate::unix_now_seconds();
    let asset = fixture
        .storage
        .due_attachment_acquisitions(now, 1)
        .unwrap()
        .remove(0);
    let job = fixture
        .storage
        .claim_attachment_acquisition(&asset, now, now + 1200)
        .unwrap()
        .unwrap();
    let digest: [u8; 32] = hex::decode(&reference.ciphertext_sha256)
        .unwrap()
        .try_into()
        .unwrap();
    let identity = storage_sqlite::AttachmentPartialIdentity {
        ciphertext_digest: digest,
        locator_digest: Sha256::digest(reference.locators[0].value.as_bytes()).into(),
        etag: "\"v1\"".into(),
        total,
    };
    let saved = 16 * MIB;
    for i in 0..16u64 {
        assert!(
            fixture
                .storage
                .checkpoint_attachment_partial(
                    &job,
                    &identity,
                    i * MIB,
                    &cipher[(i * MIB) as usize..((i + 1) * MIB) as usize],
                    now,
                    fixture.policy.retained_bytes,
                )
                .unwrap()
        );
    }
    // The file sink's mid-transfer check with L bytes staged and S saved.
    let context = Arc::new(AttachmentResume {
        explicit_permission: Default::default(),
        automatic: false,
        permission: None,
        finishing: Default::default(),
        updates: None,
        storage: fixture.storage.clone(),
        job: job.clone(),
        ciphertext_digest: digest,
        budget: fixture.policy.retained_bytes,
        directory: fixture.client.app.account_dir(&fixture.client.state.label),
        policy: fixture.policy.clone(),
    });
    let staged = saved + MIB / 2;
    fixture.set_free(0);
    let parked = context
        .publication_capacity(total, (total - staged) + (total - saved), staged, None)
        .await;
    let Err(AttachmentDownloadFailure::StorageBlocked { headroom, .. }) = parked else {
        panic!("expected a park");
    };
    assert_eq!(headroom, 6 * total - saved);
    assert!(
        fixture
            .storage
            .block_attachment_storage(&job, headroom, None, now)
            .unwrap()
    );
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    // Exactly the recorded target plus the margin, in the worker's frame.
    fixture.set_free(headroom + STORAGE_READMIT_MARGIN_BYTES);
    fixture.tick().await;
    fixture.publish_file(&plain).await;
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "the checkpoint gate selects the file sink and one Range request finishes the body"
    );
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
    let mut fixture = explicit_fixture(&reference, 64 * MIB).await;
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
    // Enough for the 4 MiB a checkpoint write wants, not for four bodies.
    fixture.set_free(6 * MIB);
    go.send(()).unwrap();
    let done = fixture.completion().await;
    assert_eq!(
        parked_headroom(&done),
        4 * plain.len() as u64,
        "pressure after the body arrived parks with the publication reserve alone"
    );
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    fixture.tick().await;
    assert!(fixture.completions.try_recv().is_err());
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);

    fixture.set_free(PLENTY);
    fixture.tick().await;
    let capacity = fixture.publish_memory(&plain).await;
    assert!(
        capacity <= plain.len() + 16,
        "the body buffer is reserved once at the ciphertext length, capacity {capacity}"
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// The database itself running out of room during in-memory publication parks
/// the job like a failed free-space check, instead of a retry timer or an
/// opted-in body with no retention. The recorded target never sits below the
/// free space seen at the refusal plus the body, so free space that merely
/// looked sufficient does not refetch into the same full database; once the
/// volume gains room the readmitted attempt publishes.
#[tokio::test]
async fn memory_publication_into_a_full_database_parks_until_room_returns() {
    let plain = vec![0x44; (2 * MIB + 9) as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: false,
            chunked: false,
        },
        Arc::clone(&requests),
        stopped,
    );
    let mut fixture = explicit_fixture(&reference, 4 * MIB).await;
    let free = 64 * MIB;
    fixture.set_free(free);
    fixture.tick().await;
    let done = fixture.completion().await;
    assert!(matches!(
        &done.completion,
        MediaHttpCompletion::Attachment {
            result: Ok(crate::client::AcquiredMediaBody::Memory(_)),
            ..
        }
    ));
    // The volume reports room, but the database refuses the body; small
    // bookkeeping updates still fit.
    fixture.storage.limit_database_pages_for_test(16).unwrap();
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    assert_eq!(fixture.storage.retained_attachment_byte_count().unwrap(), 0);
    assert_eq!(fixture.status().retry_at, None, "no retry timer runs");
    let target = free + plain.len() as u64;
    fixture.set_free(target + STORAGE_READMIT_MARGIN_BYTES - 1);
    fixture.tick().await;
    assert!(
        fixture.completions.try_recv().is_err(),
        "free space short of the refusal plus a body readmits nothing"
    );
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    fixture
        .storage
        .limit_database_pages_for_test(1 << 30)
        .unwrap();
    fixture.set_free(target + STORAGE_READMIT_MARGIN_BYTES);
    fixture.tick().await;
    fixture.publish_memory(&plain).await;
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// A full database during file-sink publication parks as well: the decrypted
/// file this attempt held is counted into the target, the remembered file sink
/// skips the header probe on readmission, and the retried attempt publishes.
#[tokio::test]
async fn file_publication_into_a_full_database_parks_until_room_returns() {
    let plain = vec![0x45; (3 * MIB + 5) as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: false,
            chunked: false,
        },
        Arc::clone(&requests),
        stopped,
    );
    let mut fixture = explicit_fixture(&reference, MIB).await;
    let free = 64 * MIB;
    fixture.set_free(free);
    fixture.tick().await;
    let done = fixture.completion().await;
    assert!(matches!(
        &done.completion,
        MediaHttpCompletion::Attachment {
            result: Ok(crate::client::AcquiredMediaBody::File(_)),
            ..
        }
    ));
    fixture.storage.limit_database_pages_for_test(16).unwrap();
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    let retained = fixture.completion().await;
    complete_media_http(
        &mut fixture.client,
        retained,
        &fixture.shared,
        &fixture.http,
    )
    .await;
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    assert_eq!(fixture.storage.retained_attachment_byte_count().unwrap(), 0);
    // Measured with the decrypted file still on disk, which the worker's
    // frame gets back once the attempt drops it.
    let target = free + 2 * plain.len() as u64;
    fixture.set_free(target + STORAGE_READMIT_MARGIN_BYTES - 1);
    fixture.tick().await;
    assert!(fixture.completions.try_recv().is_err());
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    fixture
        .storage
        .limit_database_pages_for_test(1 << 30)
        .unwrap();
    fixture.set_free(target + STORAGE_READMIT_MARGIN_BYTES);
    fixture.tick().await;
    fixture.publish_file(&plain).await;
    assert_eq!(requests.load(Ordering::SeqCst), 3);
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// Parking and readmission both reach a transfer subscription. A parked row
/// sits outside the one-second refresh set, so a subscriber whose rows are all
/// parked otherwise polls only every 30 seconds: readmission must wake it
/// through the shared update signal, well inside that interval. The park
/// itself always follows an active state and is signalled as well.
#[tokio::test]
async fn transfer_subscribers_see_park_and_readmission_without_the_active_refresh() {
    let plain = vec![0x6c; 2 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let total = cipher.len() as u64;
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: true,
            chunked: false,
        },
        Arc::clone(&requests),
        stopped,
    );
    let mut fixture = explicit_fixture(&reference, total).await;
    let runtime = fixture.client.app.runtime();
    fixture.shared = runtime.shared.clone();
    let stream = runtime
        .subscribe_attachment_transfers(
            "alice",
            &GroupId::new(hex::decode(GROUP).unwrap()),
            vec![crate::AttachmentLocalTarget {
                message_id_hex: "11".repeat(32),
                source_message_id_hex: "22".repeat(32),
                attachment_index: 0,
            }],
        )
        .await
        .unwrap();
    let state = |rows: Vec<Option<storage_sqlite::AttachmentTransferStatus>>| {
        rows.into_iter().next().flatten().unwrap().state
    };
    assert_eq!(
        state(stream.next().await.unwrap().unwrap()),
        AttachmentTransferState::Queued
    );
    fixture.set_free(total * 17 / 4);
    fixture.tick().await;
    let done = fixture.completion().await;
    parked_headroom(&done);
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    let started = tokio::time::Instant::now();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("the park reaches the subscriber")
            .unwrap()
            .unwrap();
        if state(frame) == AttachmentTransferState::StorageBlocked {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(10));
    }
    fixture.set_free(PLENTY);
    schedule(
        &fixture.client,
        &fixture.shared,
        &fixture.http,
        &mut fixture.admission,
    )
    .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("readmission must wake a subscriber of a parked row")
        .unwrap()
        .unwrap();
    assert_eq!(state(frame), AttachmentTransferState::Queued);
    stream.close();
    drop(fixture);
    runtime.shutdown_and_close().await.unwrap();
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// The remembered file sink only short-cuts explicit work: an automatic job
/// carrying the marker still takes the in-memory sink and its transfer limit.
#[tokio::test]
async fn a_remembered_file_sink_never_routes_automatic_work() {
    let plain = vec![0x56; (MIB + 3) as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: false,
            chunked: false,
        },
        Arc::clone(&requests),
        stopped,
    );
    let mut fixture = fixture(&reference, 4 * MIB, false).await;
    let now = crate::unix_now_seconds();
    let asset = fixture.status().reference.unwrap();
    // The policy write paused automatic work; the worker resumes it each tick.
    fixture
        .storage
        .resume_permitted_attachments(now, [true; 4])
        .unwrap();
    let job = fixture
        .storage
        .claim_attachment_acquisition(&asset, now, now + 100)
        .unwrap()
        .unwrap();
    assert!(fixture.storage.record_attachment_file_sink(&job).unwrap());
    assert!(
        fixture
            .storage
            .fail_attachment_acquisition(&job, Some(now))
            .unwrap()
    );
    fixture.tick().await;
    fixture.publish_memory(&plain).await;
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// An automatic body with no declared length grows its buffer geometrically
/// but never past the transfer limit, instead of doubling to twice the limit.
#[tokio::test]
async fn automatic_unknown_length_body_never_grows_past_the_transfer_limit() {
    let plain = vec![0x55; 3 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: false,
            chunked: true,
        },
        Arc::clone(&requests),
        stopped,
    );
    // Unbounded doubling would land on 4 MiB for a 3 MiB body; the limit sits
    // just above the body so the bound is observable.
    let limit = 3 * MIB + 128 * 1024;
    let mut fixture = fixture(&reference, limit, false).await;
    fixture.tick().await;
    let capacity = fixture.publish_memory(&plain).await;
    assert!(
        capacity as u64 <= limit,
        "capacity {capacity} exceeds the transfer limit {limit}"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// mdk#2241: receiving through the in-memory sink holds about one body. The
/// Rust heap peak of this test's own threads during an explicit 100 MiB
/// download (reserve, checkpoint copies, in-place decryption, verification and
/// publication) is the body plus a bounded allowance. SQLite's own peak for
/// the publication step is measured by the storage-sqlite test binary.
#[test]
fn explicit_in_memory_receive_of_one_hundred_mib_holds_about_one_body() {
    crate::tests::alloc_audit::measure(receive_peak(100 * MIB, 128 * MIB, true, Sink::Memory));
}

/// mdk#2241: an automatic download at the transfer limit holds about one body.
#[test]
fn automatic_receive_at_the_transfer_limit_holds_about_one_body() {
    crate::tests::alloc_audit::measure(receive_peak(64 * MIB - 16, 64 * MIB, false, Sink::Memory));
}

/// mdk#2241: with the default 64 MiB transfer limit, an explicit 100 MiB
/// download streams through the file sink and holds no body-sized buffer at
/// any point: staging, checkpoints, decryption and publication all run in
/// bounded chunks.
#[test]
fn explicit_file_sink_receive_of_one_hundred_mib_holds_no_body_sized_buffer() {
    crate::tests::alloc_audit::measure(receive_peak(100 * MIB, 64 * MIB, true, Sink::File));
}

/// mdk#2241: a body just under the 512 MiB explicit limit (the largest a
/// White Noise sender produces) takes the same file sink as the 100 MiB
/// measurement and stays within the same bound. Ignored by default for its
/// size, about 2.5 GiB of temporary disk and two fixture buffers of the body;
/// run with `--ignored` to measure.
#[test]
#[ignore = "receives a 512 MiB ciphertext; run explicitly to measure"]
fn explicit_file_sink_receive_just_under_512_mib_holds_no_body_sized_buffer() {
    crate::tests::alloc_audit::measure(receive_peak(512 * MIB - 16, 64 * MIB, true, Sink::File));
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sink {
    Memory,
    File,
}

async fn receive_peak(plain_len: u64, transfer_limit: u64, explicit: bool, sink: Sink) {
    use crate::tests::alloc_audit;
    let plain = vec![0x5a; plain_len as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let total = cipher.len() as u64;
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: true,
            chunked: false,
        },
        Arc::clone(&requests),
        stopped,
    );
    let mut fixture = fixture(&reference, transfer_limit, explicit).await;
    let baseline = alloc_audit::live();
    alloc_audit::reset_peak();
    fixture.tick().await;
    let capacity = match sink {
        Sink::Memory => Some(fixture.publish_memory(&plain).await),
        Sink::File => {
            fixture.publish_file(&plain).await;
            None
        }
    };
    let peak = alloc_audit::peak().saturating_sub(baseline).max(0) as u64;
    eprintln!(
        "MDK_BENCH attachment_receive explicit={explicit} file_sink={} ciphertext_bytes={total} rust_peak_bytes={peak}",
        sink == Sink::File
    );
    match sink {
        Sink::Memory => {
            // One ciphertext buffer, decrypted in place, plus bounded
            // checkpoint copies; the body itself must be visible.
            assert!(peak >= total, "the audit must see the body buffer: {peak}");
            assert!(
                peak <= total + 16 * MIB,
                "receive held {peak} bytes for a {total} byte body"
            );
            assert_eq!(capacity, Some(total as usize), "one exact reservation");
            assert_eq!(requests.load(Ordering::SeqCst), 1);
        }
        Sink::File => {
            assert!(
                peak <= 16 * MIB,
                "the file sink held {peak} bytes for a {total} byte body"
            );
            // The in-memory sink's header probe, then the file sink.
            assert_eq!(requests.load(Ordering::SeqCst), 2);
        }
    }
    stop.send(()).unwrap();
    server.await.unwrap();
}
