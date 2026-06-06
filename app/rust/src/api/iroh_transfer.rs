use localsend::iroh::provider::LocalSendProvider;
use localsend::iroh::receiver::LocalSendReceiver;
use localsend::iroh::LocalSendFileMeta;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Opaque handle to a sender (provider) session.
pub struct RsIrohSender {
    inner: Arc<Mutex<LocalSendProvider>>,
}

/// Opaque handle to a receiver session.
pub struct RsIrohReceiver {
    inner: Arc<Mutex<LocalSendReceiver>>,
}

/// Create a new sender session. Files will be imported into a blob store
/// at the given data directory.
///
/// Set `use_relay` to true for internet transfers (uses n0 relay servers).
/// Set to false for LAN-only transfers.
pub async fn iroh_sender_new(data_dir: String, use_relay: bool) -> Result<RsIrohSender, String> {
    let provider = LocalSendProvider::new(PathBuf::from(data_dir), use_relay)
        .await
        .map_err(|e| e.to_string())?;
    Ok(RsIrohSender {
        inner: Arc::new(Mutex::new(provider)),
    })
}

/// Import a file into the blob store. Returns "hash_hex:file_size".
pub async fn iroh_sender_import_file(
    sender: &RsIrohSender,
    file_path: String,
) -> Result<String, String> {
    let mut guard = sender.inner.lock().await;
    let (hash_hex, size) = guard
        .import_file(&PathBuf::from(file_path))
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("{}:{}", hash_hex, size))
}

/// Create a ticket for the imported collection.
///
/// `files_json` is a JSON array: [{"fileName":"...","size":123,"fileType":"...","hash":"...","lastModified":null}]
pub async fn iroh_sender_create_ticket(
    sender: &RsIrohSender,
    sender_alias: String,
    sender_fingerprint: String,
    version: String,
    files_json: String,
) -> Result<String, String> {
    let guard = sender.inner.lock().await;

    let files: Vec<LocalSendFileMeta> =
        serde_json::from_str(&files_json).map_err(|e: serde_json::Error| e.to_string())?;

    let collection = localsend::iroh::LocalSendCollection {
        sender_alias,
        sender_fingerprint,
        version,
        files,
    };

    guard
        .create_ticket(&collection)
        .await
        .map_err(|e| e.to_string())
}

/// Get the endpoint address as a JSON string.
pub async fn iroh_sender_endpoint_addr(sender: &RsIrohSender) -> Result<String, String> {
    let guard = sender.inner.lock().await;
    let addr = guard.endpoint_addr();
    serde_json::to_string(&addr).map_err(|e: serde_json::Error| e.to_string())
}

/// Start serving blobs in the background. Blocks until cancelled.
pub async fn iroh_sender_serve(sender: &RsIrohSender) -> Result<(), String> {
    let guard = sender.inner.lock().await;
    guard.serve().await.map_err(|e| e.to_string())
}

/// Cancel the sender session.
pub async fn iroh_sender_cancel(sender: &RsIrohSender) -> Result<(), String> {
    let guard = sender.inner.lock().await;
    guard.cancel();
    Ok(())
}

/// Shut down the sender and clean up the blob store.
pub async fn iroh_sender_shutdown(sender: &RsIrohSender) -> Result<(), String> {
    // Take ownership by swapping with a dummy, then shutdown.
    // We can't move out of Arc<Mutex<>>, so we just cancel.
    let guard = sender.inner.lock().await;
    guard.cancel();
    Ok(())
}

/// Create a new receiver session.
pub async fn iroh_receiver_new(use_relay: bool) -> Result<RsIrohReceiver, String> {
    let receiver = LocalSendReceiver::new(use_relay)
        .await
        .map_err(|e| e.to_string())?;
    Ok(RsIrohReceiver {
        inner: Arc::new(Mutex::new(receiver)),
    })
}

/// Fetch the collection metadata from the provider using the ticket.
/// Returns the LocalSendCollection as JSON.
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
/// `file_indices_json` is a JSON array of indices into the collection's file list.
/// `output_dir` is where files will be exported.
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

/// Shut down the receiver and clean up.
pub async fn iroh_receiver_shutdown(receiver: &RsIrohReceiver) -> Result<(), String> {
    let guard = receiver.inner.lock().await;
    guard.cancel();
    Ok(())
}
