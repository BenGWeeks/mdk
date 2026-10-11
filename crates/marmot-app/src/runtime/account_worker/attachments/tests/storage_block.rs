//! Disk-space outcomes (mdk#2239): a body the device cannot publish parks as
//! `StorageBlocked` with its checkpoint instead of refetching on a timer, and
//! readmits once free space covers its recorded target or the user retries.
//!
//! Free space is pinned per account directory through the test seam in
//! `attachment_resume`, which models a volume: the pinned figure is free space
//! in the worker's frame, and an attempt's own staging files and checkpoint
//! growth consume it as the attempt writes them. The window tests place it
//! exactly where the first implementation looped: above the admission reserve
//! and the bare `4 x body` publication check, but below what the attempt's own
//! staging and checkpoints add on top. The row must park once and stay parked
//! across scheduler ticks until space frees. The scheduler's clock can run
//! ahead of wall time, so readmission backoffs are crossed without sleeping.
use super::*;
use crate::media::attachment_resume::test_free_space;
use crate::runtime::account_worker::attachments::test_clock;
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
        test_clock::clear(self._dir.path());
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
    /// Pin free space in the worker's frame; the attempt's own staging and
    /// checkpoint growth are charged against it as they happen.
    fn set_free(&self, free: u64) {
        test_free_space::set_modeled(
            self._dir.path(),
            free,
            &crate::media::file_transfer::media_staging_directory(
                &self.client.app,
                &self.client.state.label,
            ),
            &self.storage,
        );
    }
    /// Run the scheduler `seconds` ahead, past a readmission backoff.
    fn advance(&self, seconds: u64) {
        test_clock::advance(self._dir.path(), seconds);
    }
    /// The parked row's readmission due time, in the scheduler's clock.
    fn due(&self) -> Option<u64> {
        self.storage
            .attachment_acquisition_status(&self.status().reference.unwrap())
            .unwrap()
            .unwrap()
            .due
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

/// File sink, chunked body with neither length nor validator, on a disk that
/// frees exactly what each park asks for. Every attempt starts the body over
/// and parks a little later. The recorded target is the largest need an
/// attempt proved, so it never exceeds what the body really needs and the job
/// publishes as soon as that much is free (a doubled target would demand more
/// than the body needs and block it). Refetches are bounded in time: each
/// readmission waits out the park streak, which the test crosses by running
/// the scheduler's clock ahead instead of sleeping.
#[tokio::test]
async fn unknown_length_restarts_record_the_proven_need_and_back_off() {
    let plain = vec![0x7c; 8 * MIB as usize];
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
    // The reserve alone passes; the staged body trips the check a few MiB in.
    fixture.set_free(12 * MIB);
    fixture.tick().await;
    let mut target = fixture.park_and_hold(&requests).await;
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    let mut parks = 1;
    let mut delays: Vec<std::ops::RangeInclusive<u64>> = vec![];
    loop {
        // The body needs five of itself (staging plus the publication
        // reserve); no recorded target may ask for more.
        assert!(
            target <= 5 * total,
            "target {target} exceeds the body's need"
        );
        // One byte short of the recorded target keeps the row parked, which
        // pins the target storage recorded: the attempt's own projection.
        fixture.set_free(target + STORAGE_READMIT_MARGIN_BYTES - 1);
        let before = requests.load(Ordering::SeqCst);
        fixture.tick().await;
        assert!(fixture.completions.try_recv().is_err());
        assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
        assert_eq!(requests.load(Ordering::SeqCst), before);
        // Free exactly what the last park asked for, as a slowly emptying disk would.
        fixture.set_free(target + STORAGE_READMIT_MARGIN_BYTES);
        let scheduler_now =
            || crate::unix_now_seconds() + test_clock::skew_for_test(fixture._dir.path());
        let readmit_from = scheduler_now();
        schedule(
            &fixture.client,
            &fixture.shared,
            &fixture.http,
            &mut fixture.admission,
        )
        .unwrap();
        let now = scheduler_now();
        assert_eq!(fixture.state(), AttachmentTransferState::Queued);
        let due = fixture.due().unwrap();
        // The scheduler read its clock somewhere in this window.
        delays.push((due - now)..=(due - readmit_from));
        if due > now {
            // Not a moment earlier: a tick before the due time fetches nothing.
            fixture.advance(due - now - 1);
            fixture.tick().await;
            assert!(fixture.completions.try_recv().is_err());
            assert_eq!(requests.load(Ordering::SeqCst), before);
            fixture.advance(1);
        }
        fixture.tick().await;
        let done = fixture.completion().await;
        match &done.completion {
            MediaHttpCompletion::Attachment {
                result: Err(AttachmentDownloadFailure::StorageBlocked { headroom, restarts }),
                ..
            } => {
                assert!(restarts, "a body without length or validator starts over");
                assert!(*headroom > target, "{headroom} after {target}");
                target = *headroom;
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
        assert!(parks <= 16, "each restart must get further than the last");
    }
    // Readmissions are spaced by the park streak: the first at once, then
    // 15 s doubling, so a trickling disk cannot drive refetches faster.
    let streak: Vec<u64> = (1..=parks as u32)
        .map(|count| {
            if count <= 1 {
                0
            } else {
                (15u64 << (count - 2).min(8)).min(3600)
            }
        })
        .collect();
    assert_eq!(delays.len(), streak.len());
    for (window, expected) in delays.iter().zip(&streak) {
        assert!(
            window.contains(expected),
            "readmission delay {window:?} does not allow the streak's {expected} s: {delays:?}"
        );
    }
    assert!(
        parks >= 3,
        "the trickle must exercise the streak: {parks} parks"
    );
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1 + parks + 1,
        "the header probe, one request per park, and the publishing attempt"
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// File sink with a strong validator, end to end: free space drops while the
/// body is mid-transfer, so the mid-transfer check parks with `6T - S` in the
/// worker's frame (four bodies, the staging and checkpoint still to grow, and
/// the staging already held). Readmitted at exactly that target plus the
/// margin, the attempt restores the saved prefix, finishes with one Range
/// request and publishes without parking again. A target that dropped the
/// held staging or the remaining checkpoint growth would re-park here.
#[tokio::test]
async fn mid_transfer_park_readmits_at_its_target_and_resumes() {
    let plain = vec![0x5a; 16 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let total = cipher.len() as u64;
    let requests = Arc::new(AtomicUsize::new(0));
    let (proceed, gate) = watch::channel(false);
    let first = 6 * MIB as usize;
    let server = {
        let requests = Arc::clone(&requests);
        let cipher = cipher.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                requests.fetch_add(1, Ordering::SeqCst);
                let cipher = cipher.clone();
                let mut gate = gate.clone();
                tokio::spawn(async move {
                    let request = request_headers(&mut socket).await;
                    let range = request
                        .lines()
                        .find_map(|line| line.strip_prefix("range: bytes="))
                        .and_then(|value| value.trim_end_matches('-').parse::<usize>().ok());
                    if let Some(start) = range {
                        let _ = socket
                            .write_all(
                                format!(
                                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{}/{}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                                    cipher.len() - start,
                                    cipher.len() - 1,
                                    cipher.len()
                                )
                                .as_bytes(),
                            )
                            .await;
                        let _ = socket.write_all(&cipher[start..]).await;
                        return;
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
                    if socket.write_all(&cipher[..first]).await.is_err() {
                        return;
                    }
                    // A little past the last whole checkpoint, then hold the
                    // rest until released.
                    if socket
                        .write_all(&cipher[first..first + 4096])
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let _ = gate.wait_for(|released| *released).await;
                    let _ = socket.write_all(&cipher[first + 4096..]).await;
                });
            }
        })
    };
    let mut fixture = explicit_fixture(&reference, MIB).await;
    fixture.tick().await;
    // Wait on the saved checkpoints, which track processed bytes exactly,
    // rather than on throttled progress reports.
    let started = tokio::time::Instant::now();
    while fixture.storage.attachment_partial_bytes_for_test().unwrap() < first as u64 {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "{:?}",
            fixture.status()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // Something else fills the disk: enough left for checkpoint writes, not
    // for the publication reserve.
    fixture.set_free(32 * MIB);
    proceed.send(true).unwrap();
    let done = fixture.completion().await;
    let headroom = parked_headroom(&done);
    let saved = fixture.storage.attachment_partial_bytes_for_test().unwrap();
    assert!(saved >= 6 * MIB && saved < total, "saved {saved}");
    assert_eq!(
        headroom,
        6 * total - saved,
        "four bodies, the staging and checkpoint growth, and the held staging"
    );
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    let before = requests.load(Ordering::SeqCst);
    // Exactly the recorded target plus the margin, in the worker's frame.
    fixture.set_free(headroom + STORAGE_READMIT_MARGIN_BYTES);
    fixture.tick().await;
    fixture.publish_file(&plain).await;
    assert_eq!(
        requests.load(Ordering::SeqCst),
        before + 1,
        "the remembered file sink restores the prefix and one Range request finishes the body"
    );
    server.abort();
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
    // The refusal saw the pinned space less the decrypted file it held; the
    // worker's frame gets that file back, so the target is the refusal's own
    // free space plus one more body.
    let target = free + plain.len() as u64;
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

/// On a truly full disk even the park write can fail. The worker keeps the
/// park in memory while the attempt's lease fences the row, records it on the
/// next tick, and nothing is refetched in between.
#[tokio::test]
async fn a_park_the_database_refuses_is_recorded_on_the_next_tick() {
    let plain = vec![0x46; (2 * MIB + 9) as usize];
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
    fixture.set_free(16 * MIB);
    fixture.tick().await;
    let done = fixture.completion().await;
    fixture.storage.limit_database_pages_for_test(16).unwrap();
    fixture.storage.fail_next_storage_parks_for_test(1).unwrap();
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    assert_eq!(
        fixture.state(),
        AttachmentTransferState::VerifyingPlaintext,
        "the refused park leaves the leased attempt in place"
    );
    assert_eq!(fixture.storage.retained_attachment_byte_count().unwrap(), 0);
    let mut updates = fixture.shared.attachment_updates.subscribe();
    updates.borrow_and_update();
    fixture.tick().await;
    assert_eq!(fixture.state(), AttachmentTransferState::StorageBlocked);
    assert!(
        updates.has_changed().unwrap(),
        "the recorded park wakes subscribers"
    );
    assert!(fixture.completions.try_recv().is_err());
    assert_eq!(requests.load(Ordering::SeqCst), 1, "nothing was refetched");
    fixture
        .storage
        .limit_database_pages_for_test(1 << 30)
        .unwrap();
    fixture.set_free(PLENTY);
    fixture.tick().await;
    fixture.publish_memory(&plain).await;
    assert_eq!(requests.load(Ordering::SeqCst), 2);
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
/// to half the transfer limit and then once to the limit: the buffer never
/// exceeds the limit, and the copy while it grows never holds more than 1.5
/// times it. Plain doubling would hold a 4 MiB buffer and its 4.125 MiB
/// successor at once here, about twice the limit.
#[test]
fn automatic_unknown_length_body_stays_within_its_growth_bound() {
    crate::tests::alloc_audit::measure(async {
        use crate::tests::alloc_audit;
        let plain = vec![0x55; (4 * MIB + 32 * 1024) as usize];
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
        let limit = 4 * MIB + 128 * 1024;
        let mut fixture = fixture(&reference, limit, false).await;
        let baseline = alloc_audit::live();
        alloc_audit::reset_peak();
        fixture.tick().await;
        let capacity = fixture.publish_memory(&plain).await;
        let peak = alloc_audit::peak().saturating_sub(baseline).max(0) as u64;
        eprintln!(
            "MDK_BENCH attachment_unknown_length_growth limit={limit} rust_peak_bytes={peak}"
        );
        assert!(
            capacity as u64 <= limit,
            "capacity {capacity} exceeds the transfer limit {limit}"
        );
        // 1.5 times the limit for the growth copy, plus 1 MiB for the HTTP
        // client's own buffers.
        assert!(
            peak <= limit * 3 / 2 + MIB,
            "growth held {peak} bytes against a {limit} byte limit"
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        stop.send(()).unwrap();
        server.await.unwrap();
    });
}

/// mdk#2241: an automatic download at the transfer limit (32 MiB here) holds
/// one body plus a bounded allowance in the in-memory sink: the buffer is
/// reserved once, decrypted in place and published through bounded BLOB
/// writes. SQLite's own peak is measured by the storage-sqlite binary.
#[test]
fn automatic_receive_at_the_transfer_limit_holds_about_one_body() {
    crate::tests::alloc_audit::measure(receive_peak(32 * MIB - 16, 32 * MIB, false, Sink::Memory));
}

/// mdk#2241: an explicit download above the transfer limit streams through the
/// file sink and holds no body-sized buffer: staging, checkpoints, decryption
/// and publication all run in bounded chunks.
#[test]
fn explicit_file_sink_receive_holds_no_body_sized_buffer() {
    crate::tests::alloc_audit::measure(receive_peak(32 * MIB, 16 * MIB, true, Sink::File));
}

/// mdk#2241 acceptance size: an explicit 100 MiB body through the in-memory
/// sink (transfer limit raised to 128 MiB). Ignored by default for its run
/// time; the 32 MiB measurement covers the same path in the regular suite.
#[test]
#[ignore = "receives a 100 MiB body; run explicitly to measure"]
fn explicit_in_memory_receive_of_one_hundred_mib_holds_about_one_body() {
    crate::tests::alloc_audit::measure(receive_peak(100 * MIB, 128 * MIB, true, Sink::Memory));
}

/// mdk#2241 acceptance size: an automatic download at the default 64 MiB
/// transfer limit. Ignored by default for its run time.
#[test]
#[ignore = "receives a 64 MiB body; run explicitly to measure"]
fn automatic_receive_at_the_default_transfer_limit_holds_about_one_body() {
    crate::tests::alloc_audit::measure(receive_peak(64 * MIB - 16, 64 * MIB, false, Sink::Memory));
}

/// mdk#2241 acceptance size: with the default 64 MiB transfer limit, an
/// explicit 100 MiB body streams through the file sink. Ignored by default for
/// its run time and disk use.
#[test]
#[ignore = "receives a 100 MiB body; run explicitly to measure"]
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
