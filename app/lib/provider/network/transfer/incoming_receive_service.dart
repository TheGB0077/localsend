// Unified incoming receive flow.
//
// Handles the common UI logic for incoming file requests regardless of
// transport (HTTP POST or iroh multicast ticket). Both transports produce
// a [ReceiveSessionState] and are driven through [ReceiveTransport].
//
// The service:
// 1. Starts the iroh receive session when a ticket arrives via multicast
// 2. Applies quick-save logic (shared between HTTP and iroh)
// 3. Pushes the appropriate page (ReceivePage or ProgressPage)

import 'package:common/model/device.dart';
import 'package:localsend_app/model/state/server/receive_session_state.dart';
import 'package:localsend_app/pages/progress_page.dart';
import 'package:localsend_app/pages/receive_page.dart';
import 'package:localsend_app/provider/favorites_provider.dart';
import 'package:localsend_app/provider/network/transfer/iroh_receive_provider.dart';
import 'package:localsend_app/provider/selection/selected_receiving_files_provider.dart';
import 'package:localsend_app/provider/settings_provider.dart';
import 'package:localsend_app/util/native/directories.dart';
import 'package:logging/logging.dart';
import 'package:refena_flutter/refena_flutter.dart';
import 'package:routerino/routerino.dart';

final _logger = Logger('IncomingReceive');

/// Provider for the incoming receive service.
/// Has `ref` access (extends Notifier) so it can read/write other providers.
final incomingReceiveServiceProvider = NotifierProvider<IncomingReceiveService, void>((ref) {
  return IncomingReceiveService();
});

/// Manages the common incoming receive flow for both HTTP and iroh transports.
///
/// HTTP: the receive_controller creates the session and directly handles
/// page navigation (because it needs to block the HTTP handler on a
/// StreamController while waiting for user decision).
///
/// Iroh: [handleIrohTicket] is called when the multicast listener detects
/// a non-empty iroh ticket. It starts the receive session, fetches the
/// collection, applies quick-save logic, and pushes the appropriate page.
class IncomingReceiveService extends Notifier<void> {
  @override
  void init() {}

  /// Handle an iroh ticket received from the multicast listener.
  ///
  /// Starts the iroh receive session, fetches the collection from the sender,
  /// applies quick-save logic, and pushes the appropriate page.
  Future<void> handleIrohTicket(String ticket) async {
    try {
      final settings = ref.read(settingsProvider);
      final destinationDir = settings.destination ?? await getDefaultDestinationDirectory();

      // Start the iroh receive session (fetches collection metadata from sender).
      await ref
          .notifier(irohReceiveProvider)
          .startSession(
            ticket: ticket,
            destinationDirectory: destinationDir,
          );

      final session = ref.read(irohReceiveProvider).session;
      if (session == null) {
        _logger.warning('Iroh receive session is null after startSession');
        return;
      }

      // Determine quick-save.
      bool quickSave = settings.quickSave;
      if (settings.quickSaveFromFavorites && !quickSave) {
        final isFavorite = ref
            .read(favoritesProvider)
            .any(
              (e) => e.fingerprint == session.sender.fingerprint,
            );
        if (isFavorite) {
          quickSave = true;
        }
      }

      if (quickSave) {
        _quickSave(session);
      } else {
        _showReceivePage(session);
      }
    } catch (e, st) {
      _logger.warning('Failed to handle iroh ticket', e, st);
    }
  }

  /// Auto-accept all files, start download, push ProgressPage.
  void _quickSave(ReceiveSessionState session) {
    final fileNameMap = <String, String>{};
    for (final entry in session.files.entries) {
      fileNameMap[entry.key] = entry.value.file.fileName;
    }
    ref.notifier(irohReceiveProvider).acceptFileRequest(fileNameMap);

    // ignore: discarded_futures
    Routerino.context.pushAndRemoveUntilImmediately(
      removeUntil: ReceivePage,
      builder: () => ProgressPage(
        showAppBar: false,
        closeSessionOnClose: true,
        sessionId: session.sessionId,
      ),
    );
  }

  /// Show ReceivePage for the user to accept/decline, then ProgressPage.
  void _showReceivePage(ReceiveSessionState session) {
    final receiveProvider = ViewProvider((ref) {
      final irohSession = ref.watch(irohReceiveProvider).session;
      return ReceivePageVm(
        status: irohSession?.status,
        sender: irohSession?.sender ?? Device.empty,
        showSenderInfo: true,
        files: irohSession?.files.values.map((f) => f.file).toList() ?? [],
        message: null,
        onAccept: () async {
          final selectedFiles = ref.read(selectedReceivingFilesProvider);
          ref.notifier(irohReceiveProvider).acceptFileRequest(selectedFiles);

          await Routerino.context.pushAndRemoveUntilImmediately(
            removeUntil: ReceivePage,
            builder: () => ProgressPage(
              showAppBar: false,
              closeSessionOnClose: true,
              sessionId: irohSession?.sessionId ?? '',
            ),
          );
        },
        onDecline: () {
          ref.notifier(irohReceiveProvider).declineFileRequest();
        },
        onClose: () {
          ref.notifier(irohReceiveProvider).closeSession();
        },
      );
    });

    // ignore: discarded_futures
    Routerino.context.push(() => ReceivePage(receiveProvider));
  }
}
