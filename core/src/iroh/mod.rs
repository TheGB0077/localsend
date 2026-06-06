pub mod provider;
pub mod receiver;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Metadata for a single file in a LocalSend collection.
/// Stored as the first blob in an iroh Collection, followed by file content blobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalSendFileMeta {
    /// Original file name (e.g. "photo.jpg").
    pub file_name: String,
    /// File size in bytes.
    pub size: u64,
    /// MIME-style file type (e.g. "image", "video", "text").
    pub file_type: String,
    /// BLAKE3 hash of the file content (hex-encoded).
    pub hash: String,
    /// Optional file modification timestamp (millis since epoch).
    pub last_modified: Option<i64>,
}

/// A LocalSend collection — the metadata envelope that tells the receiver
/// what files are available, their names, sizes, and hashes.
///
/// This is serialized as JSON and stored as the first entry in an iroh
/// Collection (which maps names to BLAKE3 hashes). The subsequent entries
/// in the Collection are the actual file contents, named by their hashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalSendCollection {
    /// Human-readable sender alias.
    pub sender_alias: String,
    /// Sender's fingerprint for identity verification.
    pub sender_fingerprint: String,
    /// Protocol version.
    pub version: String,
    /// Files in this transfer.
    pub files: Vec<LocalSendFileMeta>,
}
