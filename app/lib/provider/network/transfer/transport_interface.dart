// Transport-agnostic interface for file transfer sessions.
//
// Both HTTP v2 and iroh transports implement this interface so the UI
// can drive any transfer without knowing the underlying protocol.
//
// The UI observes state changes through the provider's state map
// (keyed by session ID) and calls actions through this interface.

import 'package:common/model/device.dart';
import 'package:localsend_app/model/cross_file.dart';
import 'package:localsend_app/model/state/server/receive_session_state.dart';

/// Which transport to use for a transfer.
enum TransportType {
  http,
  iroh,
}

/// Result of starting a send session.
class SendSessionResult {
  final String sessionId;
  final TransportType transport;

  const SendSessionResult({
    required this.sessionId,
    required this.transport,
  });
}

/// A discovered peer that offers an iroh transfer.
class DiscoveredIrohPeer {
  final String alias;
  final String fingerprint;
  final String version;
  final String? deviceModel;
  final String? deviceType;
  final String irohTicket;
  final String sourceIp;

  const DiscoveredIrohPeer({
    required this.alias,
    required this.fingerprint,
    required this.version,
    this.deviceModel,
    this.deviceType,
    required this.irohTicket,
    required this.sourceIp,
  });
}

/// Abstract send-side transport.
///
/// Implemented by both HTTP send provider and iroh send provider.
/// The UI calls these methods to initiate and control outgoing transfers.
abstract class SendTransport {
  /// The transport type.
  TransportType get transportType;

  /// Start a send session to [target] with the given [files].
  ///
  /// If [background] is true, the session auto-closes on success and
  /// no pages are pushed.
  ///
  /// Returns the session ID.
  Future<String> startSession({
    required Device target,
    required List<CrossFile> files,
    required bool background,
  });

  /// Retry sending a single failed file.
  Future<bool> retryFile({
    required String sessionId,
    required String fileId,
  });

  /// Cancel the session (notifies remote side if possible).
  Future<void> cancelSession(String sessionId);

  /// Close the session locally (no remote notification).
  void closeSession(String sessionId);

  /// Clean up all sessions.
  void clearAllSessions();

  /// Set the session background flag.
  void setBackground(String sessionId, bool background);
}

/// Abstract receive-side transport.
///
/// Implemented by both HTTP server provider and iroh receive provider.
/// The UI calls these methods to accept/decline incoming transfers.
abstract class ReceiveTransport {
  /// The transport type.
  TransportType get transportType;

  /// The current receive session, if any.
  ReceiveSessionState? get currentSession;

  /// Accept the incoming file request with the given file name map.
  /// Key: file ID, value: desired file name.
  void acceptFileRequest(Map<String, String> fileNameMap);

  /// Decline the incoming file request.
  void declineFileRequest();

  /// Update the destination directory for the current session.
  void setSessionDestinationDir(String destinationDirectory);

  /// Update the save-to-gallery setting for the current session.
  void setSessionSaveToGallery(bool saveToGallery);

  /// Cancel the session (notifies sender if possible).
  void cancelSession();

  /// Close the session locally.
  void closeSession();
}
