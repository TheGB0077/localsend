use anyhow::Result;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode, SecretKey};
use iroh_blobs::format::collection::Collection;
use iroh_blobs::get::request::get_blob;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::{BlobFormat, Hash, HashAndFormat};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tokio::sync::watch;
use tracing;

use super::LocalSendCollection;

/// Handles the receiver side: connects to a provider, downloads blobs,
/// and exports them to the user's chosen directory.
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

        let endpoint = Endpoint::builder(presets::N0)
            .alpns(vec![])
            .secret_key(secret_key)
            .relay_mode(relay)
            .bind()
            .await?;

        let (cancel_tx, cancel_rx) = watch::channel(false);

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

        Ok((hash, addr))
    }

    /// Fetch the collection metadata from the provider.
    ///
    /// Connects to the provider, downloads the root hash into the local store,
    /// then loads the iroh Collection and reads the first blob (metadata).
    pub async fn fetch_collection(
        &self,
        ticket: &str,
    ) -> Result<(LocalSendCollection, Vec<(String, Hash)>)> {
        let (root_hash, addr) = Self::parse_ticket(ticket)?;

        let connection = self
            .endpoint
            .connect(addr, iroh_blobs::protocol::ALPN)
            .await?;

        tracing::info!("Connected to provider, fetching collection");

        // Fetch the root hash (HashSeq / Collection) into our local store
        let _stats = self
            .store
            .remote()
            .fetch(connection.clone(), HashAndFormat::raw(root_hash))
            .await
            .map_err(|e| anyhow::anyhow!("fetch failed: {}", e.to_string()))?;

        // Load the iroh Collection from our local store
        let collection = Collection::load(root_hash, &*self.store).await?;

        let entries: Vec<(String, Hash)> = collection.iter().cloned().collect();
        if entries.is_empty() {
            anyhow::bail!("empty collection");
        }

        // First entry is "collection.json" — fetch and parse it
        let (_meta_name, meta_hash) = &entries[0];
        let _meta_stats = self
            .store
            .remote()
            .fetch(connection.clone(), HashAndFormat::raw(*meta_hash))
            .await
            .map_err(|e| anyhow::anyhow!("fetch meta failed: {}", e.to_string()))?;

        let meta_bytes = self.store.get_bytes(*meta_hash).await
            .map_err(|e| anyhow::anyhow!("get_bytes failed: {}", e.to_string()))?;

        let localsend_collection: LocalSendCollection = serde_json::from_slice(&meta_bytes)?;

        // Remaining entries are file blobs
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
    /// `file_indices` are 0-based indices into the collection's `files` vector.
    /// Returns total bytes downloaded.
    pub async fn download_files(
        &self,
        ticket: &str,
        file_indices: &[usize],
        output_dir: &Path,
        progress_tx: tokio::sync::mpsc::Sender<(usize, u64)>,
    ) -> Result<u64> {
        let (_root_hash, addr) = Self::parse_ticket(ticket)?;
        let connection = self
            .endpoint
            .connect(addr, iroh_blobs::protocol::ALPN)
            .await?;

        // Re-fetch collection to get hashes
        let (collection, file_entries) = self.fetch_collection(ticket).await?;

        let mut total_bytes: u64 = 0;

        for &idx in file_indices {
            if *self.cancel_rx.borrow() {
                tracing::info!("Download cancelled");
                break;
            }

            let file_meta = collection.files.get(idx).ok_or_else(|| {
                anyhow::anyhow!("file index {} out of range", idx)
            })?;

            let (hash_hex, hash) = file_entries.get(idx).ok_or_else(|| {
                anyhow::anyhow!("no hash entry for file index {}", idx)
            })?;

            // Fetch the blob into our store
            self.store
                .remote()
                .fetch(connection.clone(), HashAndFormat::raw(*hash))
                .await
                .map_err(|e| anyhow::anyhow!("fetch file failed: {}", e.to_string()))?;

            // Export to output directory
            let output_path = output_dir.join(&file_meta.file_name);
            let bytes_written = self
                .store
                .export(*hash, &output_path)
                .await
                .map_err(|e| anyhow::anyhow!("export failed: {}", e.to_string()))?;

            total_bytes += bytes_written;

            let _ = progress_tx.send((idx, bytes_written)).await;

            tracing::info!(
                "Downloaded {} ({} bytes)",
                file_meta.file_name,
                bytes_written
            );
        }

        Ok(total_bytes)
    }

    /// Signal cancellation.
    pub fn cancel(&self) {
        let _ = self.cancel_tx.send(true);
    }

    /// Shut down and clean up.
    pub async fn shutdown(self) -> Result<()> {
        self.cancel();
        self.store.shutdown().await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let _ = tokio::fs::remove_dir_all(self.data_dir).await;
        Ok(())
    }
}
