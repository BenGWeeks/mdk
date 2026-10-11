//! Disk-space outcomes (mdk#2239): a body the device cannot publish parks as
//! `StorageBlocked` with its checkpoint instead of refetching on a timer, and
//! readmits once free space covers its recorded target or the user retries.
//!
//! The window tests place free space where the first implementation looped:
//! above the admission reserve and the bare `4 x body` publication check, but
//! below what the attempt's own staging and checkpoints add on top. The row
//! must park once and stay parked across scheduler ticks until space frees.
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
    fn free(&self) -> u64 {
        fs4::available_space(self._dir.path()).unwrap()
    }
    /// Park the first attempt's completion, then prove the row stays parked and
    /// silent across scheduler ticks.
    async fn park_and_hold(&mut self, requests: &AtomicUsize) -> u64 {
        let done = self.completion().await;
        let MediaHttpCompletion::Attachment { result, .. } = &done.completion else {
            panic!("attachment completion");
        };
        let headroom = match result {
            Err(AttachmentDownloadFailure::StorageBlocked(headroom)) => *headroom,
            Err(other) => panic!("a body the device cannot publish parks: {other:?}"),
            Ok(_) => panic!("a body the device cannot publish must not publish"),
        };
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
    /// Free space, readmit, and drive a file-sink completion to `Ready`.
    async fn free_and_publish_file(&mut self, plain: &[u8]) {
        self.set_reserve(0);
        self.tick().await;
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

/// Serve `cipher` for every request until `stop`, counting requests. The first
/// response waits for `go` after `arrived`, so a test can tighten the reserve
/// once admission has passed but before the size reaches the client. Writes
/// are tolerant: a parked client drops the response early.
fn serve(
    listener: tokio::net::TcpListener,
    cipher: Vec<u8>,
    framing: Framing,
    requests: Arc<AtomicUsize>,
    gate: (oneshot::Sender<()>, oneshot::Receiver<()>),
    mut stop: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (mut arrived, mut proceed) = (Some(gate.0), Some(gate.1));
        loop {
            let (mut socket, _) = tokio::select! {
                biased;
                _ = &mut stop => return,
                accepted = listener.accept() => accepted.unwrap(),
            };
            requests.fetch_add(1, Ordering::SeqCst);
            let request = request_headers(&mut socket).await;
            assert!(!request.contains("range:"), "{request}");
            if let Some(arrived) = arrived.take() {
                arrived.send(()).unwrap();
                proceed.take().unwrap().await.unwrap();
            }
            let etag = if framing.etag { "ETag: \"v1\"\r\n" } else { "" };
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
/// target includes the staging a retry needs, and the row stays parked.
#[tokio::test]
async fn file_sink_without_validator_parks_once_inside_the_loop_window() {
    let plain = vec![0x5a; 32 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let total = cipher.len() as u64;
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let (arrived, first_request) = oneshot::channel();
    let (go, proceed) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: false,
            chunked: false,
        },
        Arc::clone(&requests),
        (arrived, proceed),
        stopped,
    );
    let mut fixture = explicit_fixture(&reference, MIB).await;
    fixture.tick().await;
    first_request.await.unwrap();
    // 4.25 bodies of headroom: enough for the bare publication reserve, not
    // for the ciphertext file a retry stages on top of it.
    let free = fixture.free();
    fixture.set_reserve(free.saturating_sub(total * 17 / 4));
    go.send(()).unwrap();
    let headroom = fixture.park_and_hold(&requests).await;
    assert_eq!(
        headroom,
        5 * total,
        "the target is the reserve plus one staged body"
    );
    // The in-memory sink spends one header-only request before handing the
    // transfer to the file sink, which refuses on the declared length.
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    fixture.free_and_publish_file(&plain).await;
    assert_eq!(requests.load(Ordering::SeqCst), 4);
    fixture.tick().await;
    assert!(fixture.completions.try_recv().is_err());
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// In-memory sink with a validator, free space inside `[4P, 5P)`: the header
/// check counts the checkpoint the download would add, so the job parks
/// before fetching anything and the retry is a single fresh request.
#[tokio::test]
async fn memory_sink_with_validator_parks_at_the_header_inside_the_loop_window() {
    let plain = vec![0x6b; 24 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let total = cipher.len() as u64;
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let (arrived, first_request) = oneshot::channel();
    let (go, proceed) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: true,
            chunked: false,
        },
        Arc::clone(&requests),
        (arrived, proceed),
        stopped,
    );
    let mut fixture = explicit_fixture(&reference, 64 * MIB).await;
    fixture.tick().await;
    first_request.await.unwrap();
    let free = fixture.free();
    fixture.set_reserve(free.saturating_sub(total * 17 / 4));
    go.send(()).unwrap();
    let headroom = fixture.park_and_hold(&requests).await;
    assert_eq!(
        headroom,
        4 * plain.len() as u64 + total,
        "the target is the reserve plus the checkpoint a validated body adds"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 1, "nothing was fetched");
    fixture.set_reserve(0);
    fixture.tick().await;
    let done = fixture.completion().await;
    let MediaHttpCompletion::Attachment { result, .. } = &done.completion else {
        panic!("attachment completion");
    };
    let Ok(crate::client::AcquiredMediaBody::Memory(body)) = result else {
        panic!("bodies within the transfer limit publish through the in-memory sink");
    };
    assert!(body.plaintext.capacity() <= plain.len() + 16);
    complete_media_http(&mut fixture.client, done, &fixture.shared, &fixture.http).await;
    fixture.assert_ready(&plain);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    stop.send(()).unwrap();
    server.await.unwrap();
}

/// File sink, chunked body with neither length nor validator: the job parks
/// mid-transfer at the first checkpoint boundary its staging cannot cover,
/// with a target that projects the bytes received, and does not refetch until
/// space frees; the retry then starts over because nothing could be saved.
#[tokio::test]
async fn file_sink_with_unknown_length_parks_mid_transfer_without_refetching() {
    let plain = vec![0x7c; 8 * MIB as usize];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (reference, cipher) = loopback_reference(&listener, &plain);
    let total = cipher.len() as u64;
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = oneshot::channel();
    let (arrived, first_request) = oneshot::channel();
    let (go, proceed) = oneshot::channel();
    let server = serve(
        listener,
        cipher,
        Framing {
            etag: false,
            chunked: true,
        },
        Arc::clone(&requests),
        (arrived, proceed),
        stopped,
    );
    let mut fixture = explicit_fixture(&reference, MIB).await;
    fixture.tick().await;
    first_request.await.unwrap();
    // Twelve MiB of headroom: the reserve alone passes, the body trips the
    // staged-bytes check a few checkpoints in.
    let free = fixture.free();
    fixture.set_reserve(free.saturating_sub(12 * MIB));
    go.send(()).unwrap();
    let headroom = fixture.park_and_hold(&requests).await;
    assert!(
        headroom >= 5 * MIB && headroom < 5 * total,
        "the target projects the bytes received so far: {headroom}"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    fixture.free_and_publish_file(&plain).await;
    assert_eq!(requests.load(Ordering::SeqCst), 4);
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
            Err(AttachmentDownloadFailure::StorageBlocked(headroom)) if *headroom == 4 * plain.len() as u64
        ),
        "pressure after the body arrived parks with the publication reserve alone"
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
    fixture.assert_ready(&plain);
    stop.send(()).unwrap();
    server.await.unwrap();
}
