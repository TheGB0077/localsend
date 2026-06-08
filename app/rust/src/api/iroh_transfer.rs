use localsend::iroh::peer_resolver::{IrohMulticastMessage, PeerResolver};
use localsend::iroh::provider::LocalSendProvider;
use localsend::iroh::receiver::LocalSendReceiver;
use localsend::iroh::LocalSendFileMeta;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Opaque handle to a sender (provider) session.
pub struct RsIrohSender {
    inner: Arc<Mutex<LocalSendProvider>>,
    peer_resolver: Arc<PeerResolver>,
}

/// Opaque handle to a receiver session.
/// No PeerResolver — discovery is handled by the existing Dart multicast listener.
/// The ticket comes from the Device model (IrohDiscovery).
pub struct RsIrohReceiver {
    inner: Arc<Mutex<LocalSendReceiver>>,
}

// ─── Sender FFI ─────────────────────────────────────────────────────

/// Create a new sender session.
///
/// - `data_dir`: path for the blob store
/// - `use_relay`: true for internet transfers (n0 relay), false for LAN-only
pub async fn iroh_sender_new(data_dir: String, use_relay: bool) -> Result<RsIrohSender, String> {
    let provider = LocalSendProvider::new(PathBuf::from(data_dir), use_relay)
        .await
        .map_err(|e| e.to_string())?;
    let peer_resolver = PeerResolver::new().await.map_err(|e| e.to_string())?;
    Ok(RsIrohSender {
        inner: Arc::new(Mutex::new(provider)),
        peer_resolver: Arc::new(peer_resolver),
    })
}

/// Import a file into the blob store.
///
/// Handles `content://` URIs on Android by opening the SAF fd and creating
/// a `/proc/self/fd/N` path for zero-copy mmap import (no full-file read
/// into memory). Returns "hash_hex:file_size".
pub async fn iroh_sender_import_file(
    sender: &RsIrohSender,
    file_path: String,
) -> Result<String, String> {
    let mut guard = sender.inner.lock().await;

    let (hash_hex, size) = if file_path.starts_with("content://") {
        #[cfg(target_os = "android")]
        {
            let (path, _file_handle) = crate::api::saf::open_uri_as_path(&file_path)?;
            guard.import_file(&path).await.map_err(|e| e.to_string())?
        }
        #[cfg(not(target_os = "android"))]
        {
            return Err("content:// URIs are only supported on Android".to_string());
        }
    } else {
        guard
            .import_file(&PathBuf::from(&file_path))
            .await
            .map_err(|e| e.to_string())?
    };

    Ok(format!("{}:{}", hash_hex, size))
}

/// Start the sender: create a ticket from imported files, begin serving blobs,
/// and broadcast the ticket over multicast.
///
/// Spawns serving and announcing in background tokio tasks and returns
/// immediately with the ticket string.
///
/// `files_json`: JSON array of `LocalSendFileMeta` objects.
/// `sender_info_json`: JSON object with `alias`, `fingerprint`, `version`, `port`, `protocol` fields.
pub async fn iroh_sender_start(
    sender: &RsIrohSender,
    sender_info_json: String,
    files_json: String,
) -> Result<String, String> {
    // Parse file metadata
    let files: Vec<LocalSendFileMeta> =
        serde_json::from_str(&files_json).map_err(|e: serde_json::Error| e.to_string())?;

    // Parse sender info
    let info: serde_json::Value =
        serde_json::from_str(&sender_info_json).map_err(|e: serde_json::Error| e.to_string())?;

    let sender_alias = info["alias"].as_str().unwrap_or("").to_string();
    let sender_fingerprint = info["fingerprint"].as_str().unwrap_or("").to_string();
    let version = info["version"].as_str().unwrap_or("").to_string();
    let port = info["port"].as_u64().unwrap_or(53317) as u16;
    let protocol = info["protocol"].as_str().map(|s| s.to_string());

    // Create collection + ticket
    let collection = localsend::iroh::LocalSendCollection {
        sender_alias: sender_alias.clone(),
        sender_fingerprint: sender_fingerprint.clone(),
        version: version.clone(),
        files,
    };

    let ticket = {
        let guard = sender.inner.lock().await;
        guard.create_ticket(&collection).await.map_err(|e| e.to_string())?
    };

    // Spawn serving in background
    {
        let inner = sender.inner.clone();
        tokio::spawn(async move {
            let guard = inner.lock().await;
            if let Err(e) = guard.serve().await {
                tracing::error!("Sender serve error: {e}");
            }
        });
    }

    // Broadcast ticket over multicast (separate socket, doesn't conflict
    // with the Dart multicast listener which binds to port 53317 for receiving).
    // Our PeerResolver binds to an ephemeral port for sending.
    let msg = IrohMulticastMessage {
        alias: sender_alias,
        fingerprint: sender_fingerprint,
        version,
        device_model: info["deviceModel"].as_str().map(|s| s.to_string()),
        device_type: info["deviceType"].as_str().map(|s| s.to_string()),
        port,
        protocol,
        announce: true,
        iroh_ticket: Some(ticket.clone()),
    };

    sender
        .peer_resolver
        .announce_burst(
            &msg,
            &[
                std::time::Duration::from_millis(100),
                std::time::Duration::from_millis(500),
                std::time::Duration::from_millis(2000),
            ],
        )
        .await
        .map_err(|e| e.to_string())?;

    Ok(ticket)
}

/// Cancel the sender session (stops serving and announcing).
pub async fn iroh_sender_cancel(sender: &RsIrohSender) -> Result<(), String> {
    let guard = sender.inner.lock().await;
    guard.cancel();
    sender.peer_resolver.cancel();
    Ok(())
}

// ─── Receiver FFI ───────────────────────────────────────────────────

/// Create a new receiver session.
///
/// No peer discovery — the ticket comes from the Device model (IrohDiscovery)
/// which is populated by the existing Dart multicast listener.
pub async fn iroh_receiver_new(use_relay: bool) -> Result<RsIrohReceiver, String> {
    let receiver = LocalSendReceiver::new(use_relay)
        .await
        .map_err(|e| e.to_string())?;
    Ok(RsIrohReceiver {
        inner: Arc::new(Mutex::new(receiver)),
    })
}

/// Fetch the collection metadata from a provider using a ticket.
///
/// `ticket`: the iroh ticket from a Device's IrohDiscovery (obtained via
/// the existing Dart multicast discovery flow).
/// Returns the `LocalSendCollection` as JSON (contains sender info + file list).
pub async fn iroh_receiver_fetch_collection(
    receiver: &RsIrohReceiver,
    ticket: String,
) -> Result<String, String> {
    let guard = receiver.inner.lock().await;
    let (collection, _file_entries) = guard
        .fetch_collection(&ticket)
        .await
        .map_err(|e| e.to_string())?;
    serde_json::to_string(&collection).map_err(|e: serde_json::Error| e.to_string())
}

/// Download selected files from the provider.
///
/// - `ticket`: the iroh ticket
/// - `file_indices_json`: JSON array of 0-based indices into the collection's file list
/// - `output_dir`: directory to save files to
/// Returns total bytes downloaded.
pub async fn iroh_receiver_download(
    receiver: &RsIrohReceiver,
    ticket: String,
    file_indices_json: String,
    output_dir: String,
) -> Result<u64, String> {
    let file_indices: Vec<usize> =
        serde_json::from_str(&file_indices_json).map_err(|e: serde_json::Error| e.to_string())?;

    let (tx, _rx) = tokio::sync::mpsc::channel::<(usize, u64)>(100);

    let guard = receiver.inner.lock().await;
    guard
        .download_files(&ticket, &file_indices, &PathBuf::from(output_dir), tx)
        .await
        .map_err(|e| e.to_string())
}

/// Cancel the receiver session.
pub async fn iroh_receiver_cancel(receiver: &RsIrohReceiver) -> Result<(), String> {
    let guard = receiver.inner.lock().await;
    guard.cancel();
    Ok(())
}
