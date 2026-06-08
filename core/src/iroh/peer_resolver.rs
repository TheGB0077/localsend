//! Peer resolver for iroh transport.
//!
//! Broadcasts and discovers iroh tickets over the same UDP multicast channel
//! that LocalSend uses for HTTP discovery (224.0.0.167:53317).
//!
//! The multicast message is an extended JSON payload that includes an optional
//! `iroh_ticket` field. HTTP-only peers ignore this field; iroh-capable peers
//! extract the ticket and use it to connect.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::watch;

/// Default multicast group (same as LocalSend HTTP discovery).
pub const DEFAULT_MULTICAST_GROUP: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 167);
/// Default multicast port (same as LocalSend HTTP discovery).
pub const DEFAULT_MULTICAST_PORT: u16 = 53317;

/// Multicast message payload sent over UDP.
/// Extends the existing LocalSend multicast format with an iroh ticket field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IrohMulticastMessage {
    /// Sender display name.
    pub alias: String,
    /// Sender fingerprint for identity.
    pub fingerprint: String,
    /// Protocol version (e.g. "2.1").
    pub version: String,
    /// Device model (e.g. "Samsung Galaxy S24").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_model: Option<String>,
    /// Device type: "mobile", "desktop", etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_type: Option<String>,
    /// HTTP port for backward compatibility.
    pub port: u16,
    /// "http" or "https".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    /// Whether this is an announcement (triggers response from listeners).
    pub announce: bool,
    /// The iroh ticket string (present when sender is ready for iroh transfer).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iroh_ticket: Option<String>,
}

/// A discovered peer offering an iroh transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredIrohPeer {
    /// Sender display name.
    pub alias: String,
    /// Sender fingerprint.
    pub fingerprint: String,
    /// Protocol version.
    pub version: String,
    /// Device model.
    pub device_model: Option<String>,
    /// Device type.
    pub device_type: Option<String>,
    /// The iroh ticket for connecting.
    pub iroh_ticket: String,
    /// Source IP address of the multicast message.
    pub source_ip: String,
}

/// Peer resolver: broadcasts iroh tickets over UDP multicast and listens
/// for peers offering iroh transfers.
pub struct PeerResolver {
    socket: Arc<UdpSocket>,
    multicast_addr: SocketAddrV4,
    cancel_tx: watch::Sender<bool>,
    cancel_rx: watch::Receiver<bool>,
}

impl PeerResolver {
    /// Create a new peer resolver bound to the default multicast group/port.
    pub async fn new() -> Result<Self> {
        Self::with_addr(DEFAULT_MULTICAST_GROUP, DEFAULT_MULTICAST_PORT).await
    }

    /// Create with custom multicast address and port.
    pub async fn with_addr(group: Ipv4Addr, port: u16) -> Result<Self> {
        let addr = SocketAddrV4::new(group, port);

        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.set_multicast_loop_v4(true)?;
        socket.set_multicast_ttl_v4(4)?;
        // Join the multicast group on all interfaces
        socket.join_multicast_v4(group, Ipv4Addr::UNSPECIFIED)?;

        let (cancel_tx, cancel_rx) = watch::channel(false);

        Ok(Self {
            socket: Arc::new(socket),
            multicast_addr: addr,
            cancel_tx,
            cancel_rx,
        })
    }

    /// Broadcast an iroh ticket announcement to the multicast group.
    /// This should be called after the sender has created a ticket and started serving.
    pub async fn announce(&self, msg: &IrohMulticastMessage) -> Result<()> {
        let payload = serde_json::to_vec(msg)?;
        self.socket.send_to(&payload, self.multicast_addr).await?;
        tracing::info!(
            "Announced iroh ticket to {} ({} bytes)",
            self.multicast_addr,
            payload.len()
        );
        Ok(())
    }

    /// Send multiple announcement bursts with delays for reliability.
    pub async fn announce_burst(
        &self,
        msg: &IrohMulticastMessage,
        delays: &[Duration],
    ) -> Result<()> {
        for (i, delay) in delays.iter().enumerate() {
            if *self.cancel_rx.borrow() {
                break;
            }
            self.announce(msg).await?;
            if i < delays.len() - 1 {
                tokio::time::sleep(*delay).await;
            }
        }
        Ok(())
    }

    /// Listen for iroh multicast announcements and return discovered peers.
    ///
    /// This binds to the multicast port and joins the group. Callers should
    /// consume the returned stream to receive peer discoveries.
    /// Filters out messages from our own fingerprint.
    ///
    /// Blocks until cancelled via `cancel()`.
    pub async fn listen(&self, own_fingerprint: &str) -> Result<Vec<DiscoveredIrohPeer>> {
        // Bind a separate socket for listening on the multicast port
        let listen_socket = UdpSocket::bind(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            self.multicast_addr.port(),
        ))
        .await?;
        listen_socket.join_multicast_v4(*self.multicast_addr.ip(), Ipv4Addr::UNSPECIFIED)?;

        let mut cancel_rx = self.cancel_rx.clone();
        let mut buf = [0u8; 65535];
        let mut peers = Vec::new();

        loop {
            if *cancel_rx.borrow() {
                break;
            }

            tokio::select! {
                result = listen_socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, src)) => {
                            if let Some(peer) = Self::parse_message(
                                &buf[..len],
                                src,
                                own_fingerprint,
                            ) {
                                tracing::info!(
                                    "Discovered iroh peer: {} ({})",
                                    peer.alias,
                                    peer.source_ip
                                );
                                peers.push(peer);
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Multicast recv error: {e}");
                        }
                    }
                }
                _ = cancel_rx.changed() => {
                    break;
                }
            }
        }

        Ok(peers)
    }

    /// Parse a single UDP datagram into a DiscoveredIrohPeer.
    /// Returns None if the message doesn't contain an iroh ticket or is from ourselves.
    fn parse_message(
        data: &[u8],
        src: std::net::SocketAddr,
        own_fingerprint: &str,
    ) -> Option<DiscoveredIrohPeer> {
        let msg: IrohMulticastMessage = serde_json::from_slice(data).ok()?;

        // Filter out our own announcements
        if msg.fingerprint == own_fingerprint {
            return None;
        }

        // Only process messages that have an iroh ticket
        let iroh_ticket = msg.iroh_ticket?;

        Some(DiscoveredIrohPeer {
            alias: msg.alias,
            fingerprint: msg.fingerprint,
            version: msg.version,
            device_model: msg.device_model,
            device_type: msg.device_type,
            iroh_ticket,
            source_ip: src.ip().to_string(),
        })
    }

    /// Signal cancellation to stop listening/announcing.
    pub fn cancel(&self) {
        let _ = self.cancel_tx.send(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_multicast_message_with_ticket() {
        let msg = IrohMulticastMessage {
            alias: "Test Device".to_string(),
            fingerprint: "abc123".to_string(),
            version: "2.1".to_string(),
            device_model: Some("TestModel".to_string()),
            device_type: Some("mobile".to_string()),
            port: 53317,
            protocol: Some("https".to_string()),
            announce: true,
            iroh_ticket: Some("abc123def456:base64data".to_string()),
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"irohTicket\":\"abc123def456:base64data\""));
        assert!(json.contains("\"announce\":true"));

        // Round-trip
        let parsed: IrohMulticastMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.iroh_ticket, Some("abc123def456:base64data".to_string()));
    }

    #[test]
    fn test_serialize_multicast_message_without_ticket() {
        let msg = IrohMulticastMessage {
            alias: "Test Device".to_string(),
            fingerprint: "abc123".to_string(),
            version: "2.1".to_string(),
            device_model: None,
            device_type: None,
            port: 53317,
            protocol: None,
            announce: true,
            iroh_ticket: None,
        };

        let json = serde_json::to_string(&msg).unwrap();
        // skip_serializing_if should omit these
        assert!(!json.contains("irohTicket"));
        assert!(!json.contains("deviceModel"));
    }

    #[test]
    fn test_parse_message_filters_own_fingerprint() {
        let msg = IrohMulticastMessage {
            alias: "Me".to_string(),
            fingerprint: "myfp".to_string(),
            version: "2.1".to_string(),
            device_model: None,
            device_type: None,
            port: 53317,
            protocol: None,
            announce: true,
            iroh_ticket: Some("ticket123".to_string()),
        };
        let data = serde_json::to_vec(&msg).unwrap();
        let src: std::net::SocketAddr = "192.168.1.1:53317".parse().unwrap();

        assert!(PeerResolver::parse_message(&data, src, "myfp").is_none());
        assert!(PeerResolver::parse_message(&data, src, "otherfp").is_some());
    }

    #[test]
    fn test_parse_message_requires_ticket() {
        let msg = IrohMulticastMessage {
            alias: "Other".to_string(),
            fingerprint: "otherfp".to_string(),
            version: "2.1".to_string(),
            device_model: None,
            device_type: None,
            port: 53317,
            protocol: None,
            announce: true,
            iroh_ticket: None,
        };
        let data = serde_json::to_vec(&msg).unwrap();
        let src: std::net::SocketAddr = "192.168.1.2:53317".parse().unwrap();

        assert!(PeerResolver::parse_message(&data, src, "myfp").is_none());
    }
}
