use anyhow::Result;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use futures_util::StreamExt;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode, SecretKey};
use iroh_blobs::api::blobs::ExportProgressItem;
use iroh_blobs::api::remote::GetProgressItem;
use iroh_blobs::format::collection::Collection;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::{Hash, HashAndFormat};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tokio::sync::watch;
use tracing;

use super::LocalSendCollection;

/// Handles the receiver side: connects to a provider, downloads blobs,
/// and exports them to the user's chosen directory.
///
/// Follows the same pattern as alt-sendme: uses `execute_get` with streaming
/// for the download, then exports blobs from the local store.
pub struct LocalSendReceiver {
    /// The iroh networking endpoint.
    endpoint: Endpoint,
    /// Temporary blob store for incoming data.
    store: FsStore,
    /// Directory for the blob store data.
    data_dir: PathBuf,
    /// Cancel channel.
    cancel_tx: watch::Sender<bool>,
    cancel_rx: watch::Receiver<bool>,
}

impl LocalSendReceiver {
    /// Create a new receiver. Uses a temporary directory for the blob store.
    pub async fn new(use_relay: bool) -> Result<Self> {
        let data_dir = std::env::temp_dir().join(format!(
            ".localsend-recv-{}",
            uuid::Uuid::new_v4()
        ));
        let store = FsStore::load(&data_dir).await?;
        let secret_key = SecretKey::generate();

        let relay = if use_relay {
            RelayMode::Default
        } else {
            RelayMode::Disabled
        };

        let endpoint = Endpoint::builder(presets::Minimal)
            .alpns(vec![])
            .secret_key(secret_key)
            .relay_mode(relay)
            .bind()
            .await?;

        let (cancel_tx, cancel_rx) = watch::channel(false);

        tracing::info!("Receiver created, blob store at {}", data_dir.display());

        Ok(Self {
            endpoint,
            store,
            data_dir,
            cancel_tx,
            cancel_rx,
        })
    }

    /// Parse a ticket string into (root_hash, endpoint_addr).
    pub fn parse_ticket(ticket: &str) -> Result<(Hash, iroh::EndpointAddr)> {
        let (hash_str, addr_b64) = ticket
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid ticket format"))?;

        let hash = Hash::from_str(hash_str)?;
        let addr_bytes = STANDARD_NO_PAD.decode(addr_b64)?;
        let addr: iroh::EndpointAddr = serde_json::from_slice(&addr_bytes)?;

        tracing::info!("Parsed ticket: hash={}, node_id={}", hash, addr.id);
        tracing::info!(
            "Ticket direct addrs: {:?}",
            addr.ip_addrs().collect::<Vec<_>>()
        );

        Ok((hash, addr))
    }

    /// Fetch the collection metadata from the provider.
    ///
    /// Connects to the provider, downloads blobs into the local store using
    /// the same streaming approach as alt-sendme, then loads metadata.
    pub async fn fetch_collection(
        &self,
        ticket: &str,
    ) -> Result<(LocalSendCollection, Vec<(String, Hash)>)> {
        let (root_hash, addr) = Self::parse_ticket(ticket)?;
        let hash_and_format = HashAndFormat::hash_seq(root_hash);

        let connection = self
            .endpoint
            .connect(addr, iroh_blobs::protocol::ALPN)
            .await?;

        tracing::info!("Connected to provider, fetching collection");

        // Check what we already have locally.
        let local = self.store.remote().local(hash_and_format).await
            .map_err(|e| anyhow::anyhow!("local check failed: {}", e.to_string()))?;

        if !local.is_complete() {
            // Download using execute_get with streaming (same as alt-sendme).
            let get = self.store.remote().execute_get(connection, local.missing());
            let mut stream = get.stream();

            while let Some(item) = stream.next().await {
                match item {
                    GetProgressItem::Progress(offset) => {
                        tracing::debug!("Download progress: {} bytes", offset);
                    }
                    GetProgressItem::Done(stats) => {
                        tracing::info!("Download complete: {:?}", stats);
                        break;
                    }
                    GetProgressItem::Error(cause) => {
                        anyhow::bail!("Download error: {:?}", cause);
                    }
                }
            }
        } else {
            tracing::info!("Collection already complete locally");
        }

        // Load the iroh Collection from our local store.
        let collection = Collection::load(root_hash, &*self.store).await?;

        let entries: Vec<(String, Hash)> = collection.iter().cloned().collect();
        if entries.is_empty() {
            anyhow::bail!("empty collection");
        }

        tracing::info!("Collection has {} entries", entries.len());
        for (i, (name, hash)) in entries.iter().enumerate() {
            tracing::info!("  [{}] {} -> {}", i, name, hash);
        }

        // First entry is "collection.json" — read it.
        let (_meta_name, meta_hash) = &entries[0];
        let meta_bytes = self.store.get_bytes(*meta_hash).await
            .map_err(|e| anyhow::anyhow!("get_bytes failed: {}", e.to_string()))?;

        tracing::info!("Meta blob size: {} bytes", meta_bytes.len());

        let localsend_collection: LocalSendCollection = serde_json::from_slice(&meta_bytes)?;

        // Remaining entries are file blobs.
        let file_entries: Vec<(String, Hash)> = entries.into_iter().skip(1).collect();

        tracing::info!(
            "Fetched collection: {} files from {}",
            localsend_collection.files.len(),
            localsend_collection.sender_alias
        );

        Ok((localsend_collection, file_entries))
    }

    /// Download selected files from the provider and export them to `output_dir`.
    ///
    /// Uses the same approach as alt-sendme: connects, streams download via
    /// `execute_get`, then exports from the local blob store.
    ///
    /// `file_indices` are 0-based indices into the `LocalSendCollection.files` vector.
    /// Returns total bytes exported.
    pub async fn download_files(
        &self,
        ticket: &str,
        file_indices: &[usize],
        output_dir: &Path,
        progress_tx: tokio::sync::mpsc::Sender<(usize, u64)>,
    ) -> Result<u64> {
        tracing::info!(
            "download_files called: {} files to {}",
            file_indices.len(),
            output_dir.display()
        );

        let (root_hash, addr) = Self::parse_ticket(ticket)?;
        let hash_and_format = HashAndFormat::hash_seq(root_hash);

        // Connect and download everything.
        let connection = self
            .endpoint
            .connect(addr, iroh_blobs::protocol::ALPN)
            .await?;

        tracing::info!("Connected to provider for download");

        let local = self.store.remote().local(hash_and_format).await
            .map_err(|e| anyhow::anyhow!("local check failed: {}", e.to_string()))?;

        if !local.is_complete() {
            let get = self.store.remote().execute_get(connection, local.missing());
            let mut stream = get.stream();

            while let Some(item) = stream.next().await {
                match item {
                    GetProgressItem::Progress(offset) => {
                        tracing::debug!("Download progress: {} bytes", offset);
                    }
                    GetProgressItem::Done(stats) => {
                        tracing::info!("Download complete: {:?}", stats);
                        break;
                    }
                    GetProgressItem::Error(cause) => {
                        anyhow::bail!("Download error: {:?}", cause);
                    }
                }
            }
        } else {
            tracing::info!("All blobs already complete locally");
        }

        // Load the iroh Collection to get name→hash mappings.
        let iroh_collection = Collection::load(root_hash, &*self.store).await?;
        let all_entries: Vec<(String, Hash)> = iroh_collection.iter().cloned().collect();

        tracing::info!("Collection entries: {}", all_entries.len());

        if all_entries.len() < 2 {
            anyhow::bail!("collection has no file entries");
        }

        // Entry 0 is "collection.json" (metadata). Entries 1..N are file blobs.
        let file_entries = &all_entries[1..];

        // Read the metadata blob.
        let meta_hash = all_entries[0].1;
        let meta_bytes = self.store.get_bytes(meta_hash).await
            .map_err(|e| anyhow::anyhow!("get_bytes for meta failed: {}", e.to_string()))?;
        let collection: LocalSendCollection = serde_json::from_slice(&meta_bytes)?;

        // Ensure output directory exists.
        tokio::fs::create_dir_all(output_dir).await?;

        let mut total_bytes: u64 = 0;

        for &idx in file_indices {
            if *self.cancel_rx.borrow() {
                tracing::info!("Download cancelled");
                break;
            }

            let file_meta = collection.files.get(idx).ok_or_else(|| {
                anyhow::anyhow!(
                    "file index {} out of range (max {})",
                    idx,
                    collection.files.len().saturating_sub(1)
                )
            })?;

            let (name, hash) = file_entries.get(idx).ok_or_else(|| {
                anyhow::anyhow!(
                    "no hash entry for file index {} (max {})",
                    idx,
                    file_entries.len().saturating_sub(1)
                )
            })?;

            tracing::info!(
                "Exporting blob {} ({}) -> {}/{}",
                name,
                hash,
                output_dir.display(),
                file_meta.file_name
            );

            // Export from local blob store to output directory.
            let output_path = output_dir.join(&file_meta.file_name);

            // Use streaming export (same as alt-sendme) for robustness.
            let mut export_stream = self
                .store
                .export_with_opts(iroh_blobs::api::blobs::ExportOptions {
                    hash: *hash,
                    target: output_path.clone(),
                    mode: iroh_blobs::api::blobs::ExportMode::Copy,
                })
                .stream()
                .await;

            let mut bytes_written: u64 = 0;
            while let Some(item) = export_stream.next().await {
                match item {
                    ExportProgressItem::Size(s) => {
                        bytes_written = s;
                        tracing::info!("Export size for {}: {} bytes", file_meta.file_name, s);
                    }
                    ExportProgressItem::Done => {
                        break;
                    }
                    ExportProgressItem::Error(cause) => {
                        anyhow::bail!("export {} failed: {:?}", file_meta.file_name, cause);
                    }
                    _ => {}
                }
            }

            tracing::info!(
                "Exported {} ({} bytes)",
                file_meta.file_name,
                bytes_written
            );

            total_bytes += bytes_written;
            let _ = progress_tx.send((idx, bytes_written)).await;
        }

        tracing::info!("download_files complete: {} total bytes", total_bytes);
        Ok(total_bytes)
    }

    /// Signal cancellation.
    pub fn cancel(&self) {
        let _ = self.cancel_tx.send(true);
    }

    /// Close the endpoint gracefully.
    pub async fn close(&self) {
        self.cancel();
        self.endpoint.close().await;
    }

    /// Shut down and clean up (consumes self).
    pub async fn shutdown(self) -> Result<()> {
        self.endpoint.close().await;
        self.store.shutdown().await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let _ = tokio::fs::remove_dir_all(self.data_dir).await;
        Ok(())
    }
}
