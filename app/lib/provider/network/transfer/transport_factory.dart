// Transport factory and router.
//
// Decides whether to use HTTP v2 or iroh for a given transfer,
// and provides a unified interface for the UI.

import 'package:common/model/device.dart';
import 'package:localsend_app/model/cross_file.dart';
import 'package:localsend_app/model/state/send/send_session_state.dart';
import 'package:localsend_app/model/state/server/receive_session_state.dart';
import 'package:localsend_app/provider/device_info_provider.dart';
import 'package:localsend_app/provider/network/send_provider.dart';
import 'package:localsend_app/provider/network/transfer/iroh_receive_provider.dart';
import 'package:localsend_app/provider/network/transfer/iroh_send_provider.dart';
import 'package:localsend_app/provider/network/transfer/transport_interface.dart';
import 'package:refena_flutter/refena_flutter.dart';

/// Provider that determines which transport to use for sending.
///
/// Decision logic:
/// 1. If the target device supports iroh (has irohTicket in discovery),
///    prefer iroh for the transfer.
/// 2. Otherwise, fall back to HTTP v2.
///
/// The iroh transport is currently opt-in via settings. Once stable,
/// it can become the default for LAN transfers.
final transportDecisionProvider = Provider<TransportType>((ref) {
  // For now, default to HTTP. Iroh is activated when a discovered peer
  // has an iroh ticket. The send flow checks this per-device.
  return TransportType.http;
});

/// The iroh send session state, exposed alongside HTTP send state
/// so the UI can observe both.
final irohSendSessionsProvider = ViewProvider<Map<String, SendSessionState>>((ref) {
  return ref.watch(irohSendProvider);
});

/// The iroh receive session state, exposed alongside HTTP receive state
/// so the UI can observe both.
final irohReceiveSessionProvider = ViewProvider<ReceiveSessionState?>((ref) {
  return ref.watch(irohReceiveProvider).session;
});

/// Utility: check if a discovered device offers iroh transport.
/// Currently, this checks if the multicast announcement contained
/// an iroh ticket field.
bool deviceSupportsIroh(Device device) {
  // Devices discovered via multicast that include an iroh ticket will
  // have their info stored with an iroh ticket marker.
  // For now, this is determined at discovery time in the peer resolver.
  // TODO: extend the Device model with an `irohTicket` field, or use
  // a separate iroh device registry.
  return false; // Will be updated when discovery integration is complete.
}

/// Start a send session using the appropriate transport.
///
/// This is the main entry point for the UI — replaces direct calls
/// to `sendProvider.startSession()`.
Future<String> startSendSession({
  required Ref ref,
  required Device target,
  required List<CrossFile> files,
  required bool background,
  TransportType? forceTransport,
}) async {
  final transport = forceTransport ?? ref.read(transportDecisionProvider);

  if (transport == TransportType.iroh) {
    final deviceInfo = ref.read(deviceFullInfoProvider);
    return ref
        .notifier(irohSendProvider)
        .startSession(
          target: target,
          files: files,
          background: background,
          senderAlias: deviceInfo.alias,
          senderFingerprint: deviceInfo.fingerprint,
          version: deviceInfo.version,
        );
  }

  // HTTP v2 (default)
  await ref
      .notifier(sendProvider)
      .startSession(
        target: target,
        files: files,
        background: background,
      );
  // The HTTP provider manages its own session IDs internally.
  // Find the latest session from the state.
  final httpState = ref.read(sendProvider);
  return httpState.keys.last;
}

/// Get the active send session state regardless of transport.
SendSessionState? getSendSession(
  Map<String, SendSessionState> httpSessions,
  Map<String, SendSessionState> irohSessions,
  String sessionId,
) {
  return httpSessions[sessionId] ?? irohSessions[sessionId];
}

/// Get the active receive session state regardless of transport.
ReceiveSessionState? getReceiveSession(
  ReceiveSessionState? httpSession,
  ReceiveSessionState? irohSession,
) {
  // HTTP takes priority if both exist (shouldn't happen).
  return httpSession ?? irohSession;
}
