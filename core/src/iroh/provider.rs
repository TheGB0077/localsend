use anyhow::Result;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode, SecretKey};
use iroh_blobs::api::blobs::{AddPathOptions, BlobStatus, ImportMode};
use iroh_blobs::api::TempTag;
use iroh_blobs::format::collection::Collection;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::{BlobFormat, BlobsProtocol, Hash};
use std::path::PathBuf;
use std::str::FromStr;
use tokio::sync::watch;
use tracing;

use super::LocalSendCollection;

/// Handles the sender side: imports files into a blob store and serves them
/// to a receiver over iroh's QUIC transport (with NAT traversal).
pub struct LocalSendProvider {
    /// The iroh networking endpoint (handles hole punching, relay, etc.).
    endpoint: Endpoint,
    /// On-disk content-addressed blob store.
    store: FsStore,
    /// Directory for the blob store data (kept so we can clean up).
    data_dir: PathBuf,
    /// Tags holding imported blobs alive.
    temp_tags: Vec<TempTag>,
    /// Cancel channel.
    cancel_tx: watch::Sender<bool>,
    cancel_rx: watch::Receiver<bool>,
}

impl LocalSendProvider {
    /// Create a new provider. The blob store is created at `data_dir`.
    /// If `use_relay` is false, no relay servers are used (LAN only).
    pub async fn new(data_dir: PathBuf, use_relay: bool) -> Result<Self> {
        let store = FsStore::load(&data_dir).await?;
        let secret_key = SecretKey::generate();

        let relay = if use_relay {
            RelayMode::Default
        } else {
            RelayMode::Disabled
        };

        let endpoint = Endpoint::builder(presets::N0)
            .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
            .secret_key(secret_key)
            .relay_mode(relay)
            .bind()
            .await?;

        let (cancel_tx, cancel_rx) = watch::channel(false);

        Ok(Self {
            endpoint,
            store,
            data_dir,
            temp_tags: Vec::new(),
            cancel_tx,
            cancel_rx,
        })
    }

    /// Import a single file into the blob store. Returns its BLAKE3 hash (hex)
    /// and file size.
    pub async fn import_file(&mut self, path: &PathBuf) -> Result<(String, u64)> {
        let tt = self
            .store
            .add_path_with_opts(AddPathOptions {
                path: path.clone(),
                format: BlobFormat::Raw,
                mode: ImportMode::TryReference,
            })
            .temp_tag()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

        let hash = tt.hash();
        let hash_hex = hash.to_hex().to_string();

        let size = match self.store.status(hash).await {
            Ok(BlobStatus::Complete { size }) => size,
            other => {
                anyhow::bail!("blob not complete after import: {:?}", other)
            }
        };

        tracing::info!("Imported {} -> {} ({} bytes)", path.display(), hash_hex, size);
        self.temp_tags.push(tt);
        Ok((hash_hex, size))
    }

    /// Import raw bytes into the blob store. Used for Android SAF content://
    /// URIs where we have the data in memory but no filesystem path.
    /// Returns the BLAKE3 hash (hex) and size.
    pub async fn import_bytes(&mut self, data: bytes::Bytes) -> Result<(String, u64)> {
        let size = data.len() as u64;
        let tt = self
            .store
            .add_bytes(data)
            .temp_tag()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

        let hash = tt.hash();
        let hash_hex = hash.to_hex().to_string();

        tracing::info!("Imported bytes -> {} ({} bytes)", hash_hex, size);
        self.temp_tags.push(tt);
        Ok((hash_hex, size))
    }

    /// Create an iroh Collection from the imported files and generate a ticket
    /// that the receiver can use to connect and download.
    ///
    /// The collection stores: collection metadata (JSON) + file content blobs,
    /// all indexed by BLAKE3 hash.
    pub async fn create_ticket(&self, collection: &LocalSendCollection) -> Result<String> {
        // Serialize the collection metadata and store it as a blob
        let collection_bytes: bytes::Bytes = serde_json::to_vec(collection)?.into();
        let meta_tag = self
            .store
            .add_bytes(collection_bytes)
            .temp_tag()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let meta_hash = meta_tag.hash();

        // Build the iroh Collection: maps "collection.json" -> meta_hash,
        // then "<hash_hex>" -> file_hash for each file.
        let mut iroh_collection = Collection::default();
        iroh_collection.push("collection.json".to_string(), meta_hash);

        for file_meta in &collection.files {
            let hash = Hash::from_str(&file_meta.hash)?;
            iroh_collection.push(file_meta.hash.clone(), hash);
        }

        // Store the collection in the blob store — returns a TempTag for the root
        let root_tag = iroh_collection.store(&*self.store).await?;
        let root_hash = root_tag.hash();

        // Encode ticket: "root_hash_hex:base64(EndpointAddr JSON)"
        let addr = self.endpoint.addr();
        let addr_json = serde_json::to_vec(&addr)?;
        let addr_b64 = base64::engine::general_purpose::STANDARD_NO_PAD.encode(&addr_json);

        let ticket = format!("{}:{}", root_hash.to_hex(), addr_b64);

        tracing::info!(
            "Created ticket for collection with {} files",
            collection.files.len()
        );
        Ok(ticket)
    }

    /// Start serving blobs. This spawns the iroh protocol router and blocks
    /// until cancelled.
    pub async fn serve(&self) -> Result<()> {
        let blobs = BlobsProtocol::new(&*self.store, None);
        let router = iroh::protocol::Router::builder(self.endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs)
            .spawn();

        // Wait for cancellation signal
        let mut cancel_rx = self.cancel_rx.clone();
        cancel_rx.changed().await?;
        tracing::info!("Provider shutting down");

        tokio::time::timeout(std::time::Duration::from_secs(2), router.shutdown())
            .await
            .map_err(|e| anyhow::anyhow!("shutdown timeout: {e}"))?
            .map_err(|e| anyhow::anyhow!("shutdown error: {e}"))?;
        Ok(())
    }

    /// Signal the provider to stop serving.
    pub fn cancel(&self) {
        let _ = self.cancel_tx.send(true);
    }

    /// Get the endpoint address for ticket sharing / discovery.
    pub fn endpoint_addr(&self) -> iroh::EndpointAddr {
        self.endpoint.addr()
    }

    /// Shut down and clean up the blob store.
    pub async fn shutdown(self) -> Result<()> {
        self.cancel();
        self.store.shutdown().await?;
        Ok(())
    }
}
