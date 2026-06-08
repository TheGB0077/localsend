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
    /// The iroh networking endpoint.
    endpoint: Endpoint,
    /// On-disk content-addressed blob store.
    store: FsStore,
    /// Tags holding imported blobs alive.
    temp_tags: Vec<TempTag>,
    /// The iroh Router that serves blobs. Must stay alive for the provider to work.
    _router: iroh::protocol::Router,
    /// Cancel channel.
    cancel_tx: watch::Sender<bool>,
    cancel_rx: watch::Receiver<bool>,
}

impl LocalSendProvider {
    /// Create a new provider. The blob store is created at `data_dir`.
    /// Spawns the iroh Router immediately so it's ready to serve blobs
    /// when the receiver connects.
    /// If `use_relay` is false, no relay servers are used (LAN only).
    pub async fn new(data_dir: PathBuf, use_relay: bool) -> Result<Self> {
        let store = FsStore::load(&data_dir).await?;
        let secret_key = SecretKey::generate();

        let relay = if use_relay {
            RelayMode::Default
        } else {
            RelayMode::Disabled
        };

        let endpoint = Endpoint::builder(presets::Minimal)
            .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
            .secret_key(secret_key)
            .relay_mode(relay)
            .bind()
            .await?;

        // Spawn the Router immediately — it starts accepting connections
        // right away, so the receiver can connect as soon as it gets the ticket.
        let blobs = BlobsProtocol::new(&*store, None);
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs)
            .spawn();

        tracing::info!(
            "Provider ready, endpoint addr has {} direct addresses",
            endpoint.addr().ip_addrs().count()
        );

        let (cancel_tx, cancel_rx) = watch::channel(false);

        Ok(Self {
            endpoint,
            store,
            temp_tags: Vec::new(),
            _router: router,
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

    /// Import raw bytes into the blob store.
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
    pub async fn create_ticket(&self, collection: &LocalSendCollection) -> Result<String> {
        let collection_bytes: bytes::Bytes = serde_json::to_vec(collection)?.into();
        let meta_tag = self
            .store
            .add_bytes(collection_bytes)
            .temp_tag()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let meta_hash = meta_tag.hash();

        let mut iroh_collection = Collection::default();
        iroh_collection.push("collection.json".to_string(), meta_hash);

        for file_meta in &collection.files {
            let hash = Hash::from_str(&file_meta.hash)?;
            iroh_collection.push(file_meta.hash.clone(), hash);
        }

        let root_tag = iroh_collection.store(&*self.store).await?;
        let root_hash = root_tag.hash();

        // Build a filtered EndpointAddr — only include LAN-reachable addresses.
        // Skip Docker bridges (172.16-31.x.x), CGNAT (100.64-127.x.x), etc.
        let full_addr = self.endpoint.addr();
        let filtered_addrs: Vec<iroh::TransportAddr> = full_addr
            .ip_addrs()
            .filter(|addr| {
                let ip = addr.ip();
                // Keep private LAN (192.168.x.x, 10.x.x.x)
                // Keep IPv6 link-local and unique local
                // Skip Docker bridges (172.16-31.x.x)
                // Skip CGNAT (100.64-127.x.x)
                // Skip loopback (127.x.x.x)
                if ip.is_loopback() {
                    return false;
                }
                if let std::net::IpAddr::V4(v4) = ip {
                    let octets = v4.octets();
                    // Skip CGNAT: 100.64.0.0/10
                    if octets[0] == 100 && (octets[1] & 0xc0) == 0x40 {
                        return false;
                    }
                    // Skip Docker bridges: 172.16.0.0/12
                    if octets[0] == 172 && (octets[1] & 0xf0) == 0x10 {
                        return false;
                    }
                }
                true
            })
            .map(|addr| iroh::TransportAddr::Ip(*addr))
            .collect();

        let filtered_addr = iroh::EndpointAddr::from_parts(full_addr.id, filtered_addrs);

        let addr_json = serde_json::to_vec(&filtered_addr)?;
        let addr_b64 = base64::engine::general_purpose::STANDARD_NO_PAD.encode(&addr_json);

        let ticket = format!("{}:{}", root_hash.to_hex(), addr_b64);

        tracing::info!(
            "Created ticket for collection with {} files ({} addresses)",
            collection.files.len(),
            filtered_addr.ip_addrs().count()
        );
        Ok(ticket)
    }

    /// Wait until cancelled. The Router is already running (spawned in `new()`).
    pub async fn serve(&self) -> Result<()> {
        let mut cancel_rx = self.cancel_rx.clone();
        cancel_rx.changed().await?;
        tracing::info!("Provider shutting down");
        Ok(())
    }

    /// Signal the provider to stop serving.
    pub fn cancel(&self) {
        let _ = self.cancel_tx.send(true);
    }

    /// Get the endpoint address.
    pub fn endpoint_addr(&self) -> iroh::EndpointAddr {
        self.endpoint.addr()
    }

    /// Shut down and clean up.
    pub async fn shutdown(self) -> Result<()> {
        self.cancel();
        self.endpoint.close().await;
        self.store.shutdown().await?;
        Ok(())
    }
}
