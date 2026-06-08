// Iroh send transport provider.
//
// Manages outgoing file transfers using the iroh blob protocol.
// Produces [SendSessionState] so the existing UI pages work as-is.

import 'dart:convert';
import 'dart:io';

import 'package:common/model/device.dart';
import 'package:common/model/dto/file_dto.dart';
import 'package:common/model/file_status.dart';
import 'package:common/model/session_status.dart';
import 'package:localsend_app/model/cross_file.dart';
import 'package:localsend_app/model/state/send/send_session_state.dart';
import 'package:localsend_app/model/state/send/sending_file.dart';
import 'package:localsend_app/provider/progress_provider.dart';
import 'package:localsend_app/rust/api/iroh_transfer.dart' as rust;
import 'package:logging/logging.dart';
import 'package:refena_flutter/refena_flutter.dart';
import 'package:uuid/uuid.dart';

const _uuid = Uuid();
final _logger = Logger('IrohSend');

/// Provider for iroh send sessions.
/// State is a map of session ID → session state (same shape as HTTP sendProvider).
final irohSendProvider = NotifierProvider<IrohSendNotifier, Map<String, SendSessionState>>((ref) {
  return IrohSendNotifier();
});

class IrohSendNotifier extends Notifier<Map<String, SendSessionState>> {
  /// Active Rust sender handles (session ID → handle).
  final Map<String, rust.RsIrohSender> _senders = {};

  @override
  Map<String, SendSessionState> init() => {};

  /// Create a new iroh send session, import files, start serving, and
  /// broadcast the ticket.
  Future<String> startSession({
    required Device target,
    required List<CrossFile> files,
    required bool background,
    required String senderAlias,
    required String senderFingerprint,
    required String version,
  }) async {
    final sessionId = _uuid.v4();

    // Build the initial session state (same shape as HTTP send provider).
    final filesMap = <String, SendingFile>{};
    final fileIds = <String>{}; // preserve order

    for (final file in files) {
      final id = _uuid.v4();
      fileIds.add(id);
      filesMap[id] = SendingFile(
        file: FileDto(
          id: id,
          fileName: file.name,
          size: file.size,
          fileType: file.fileType,
          hash: null,
          preview: null,
          metadata: null,
        ),
        status: FileStatus.queue,
        token: null,
        thumbnail: file.thumbnail,
        asset: file.asset,
        path: file.path,
        bytes: file.bytes,
        errorMessage: null,
      );
    }

    state = {
      ...state,
      sessionId: SendSessionState(
        sessionId: sessionId,
        remoteSessionId: null,
        background: background,
        status: SessionStatus.waiting,
        target: target,
        files: filesMap,
        startTime: null,
        endTime: null,
        sendingTasks: null,
        errorMessage: null,
      ),
    };

    try {
      // 1. Create the Rust sender
      final dataDir = Directory.systemTemp.createTempSync('localsend-iroh-send-');
      final sender = await rust.irohSenderNew(
        dataDir: dataDir.path,
        useRelay: false, // LAN-only for now
      );
      _senders[sessionId] = sender;

      // 2. Import files one by one
      final importedHashes = <String, String>{}; // file id → hash_hex
      for (final entry in filesMap.entries) {
        final file = entry.value;
        if (file.path != null) {
          final result = await rust.irohSenderImportFile(
            sender: sender,
            filePath: file.path!,
          );
          final parts = result.split(':');
          final hashHex = parts[0];
          importedHashes[entry.key] = hashHex;

          // Update file with hash — FileDto has no copyWith, rebuild it.
          final updatedFile = FileDto(
            id: file.file.id,
            fileName: file.file.fileName,
            size: file.file.size,
            fileType: file.file.fileType,
            hash: hashHex,
            preview: file.file.preview,
            metadata: file.file.metadata,
          );
          state = {
            ...state,
            sessionId: state[sessionId]!.copyWith(
              files: {
                ...state[sessionId]!.files,
                entry.key: file.copyWith(file: updatedFile),
              },
            ),
          };
        }
      }

      // 3. Build files metadata JSON for ticket creation
      final filesMetaJson = <Map<String, dynamic>>[];
      for (final id in fileIds) {
        final file = filesMap[id]!;
        filesMetaJson.add({
          'fileName': file.file.fileName,
          'size': file.file.size,
          'fileType': file.file.fileType.name,
          'hash': importedHashes[id] ?? '',
          'lastModified': null,
        });
      }

      // 4. Start serving + create ticket + announce
      final senderInfoJson = jsonEncode({
        'alias': senderAlias,
        'fingerprint': senderFingerprint,
        'version': version,
        'port': 53317,
        'protocol': 'http',
        'deviceModel': null,
        'deviceType': null,
      });

      final ticket = await rust.irohSenderStart(
        sender: sender,
        senderInfoJson: senderInfoJson,
        filesJson: jsonEncode(filesMetaJson),
      );

      _logger.info('Iroh sender started, ticket: ${ticket.substring(0, 20)}...');

      // 5. Update state to "sending" — files are now being served.
      state = {
        ...state,
        sessionId: state[sessionId]!.copyWith(
          status: SessionStatus.sending,
          startTime: DateTime.now().millisecondsSinceEpoch,
        ),
      };

      // 6. Mark all files as "sending" — iroh serves them on demand.
      state = {
        ...state,
        sessionId: state[sessionId]!.copyWith(
          files: {
            for (final e in state[sessionId]!.files.entries) e.key: e.value.copyWith(status: FileStatus.sending),
          },
        ),
      };

      // TODO: Monitor for completion. Currently iroh has no receiver → sender
      // confirmation signal. The sender just serves until cancelled.
      // A follow-up mechanism (e.g., receiver sends an HTTP confirmation,
      // or we add a confirmation blob to the collection) is needed.
    } catch (e, st) {
      _logger.warning('Iroh send failed', e, st);
      state = {
        ...state,
        sessionId: state[sessionId]!.copyWith(
          status: SessionStatus.finishedWithErrors,
          errorMessage: e.toString(),
          endTime: DateTime.now().millisecondsSinceEpoch,
        ),
      };
    }

    return sessionId;
  }

  /// Cancel a session.
  Future<void> cancelSession(String sessionId) async {
    final sender = _senders.remove(sessionId);
    if (sender != null) {
      try {
        await rust.irohSenderCancel(sender: sender);
      } catch (e) {
        _logger.warning('Error cancelling iroh sender', e);
      }
    }

    final session = state[sessionId];
    if (session != null) {
      state = {
        ...state,
        sessionId: session.copyWith(
          status: SessionStatus.canceledBySender,
          endTime: DateTime.now().millisecondsSinceEpoch,
        ),
      };
    }
  }

  /// Close a session locally.
  void closeSession(String sessionId) {
    _senders.remove(sessionId);
    ref.notifier(progressProvider).removeSession(sessionId);
    state = {...state}..remove(sessionId);
  }

  /// Clear all sessions.
  void clearAllSessions() {
    for (final sender in _senders.values) {
      try {
        // Fire and forget — can't await in a loop with non-async method.
        rust.irohSenderCancel(sender: sender);
      } catch (_) {}
    }
    _senders.clear();
    ref.notifier(progressProvider).removeAllSessions();
    state = {};
  }

  void setBackground(String sessionId, bool background) {
    final session = state[sessionId];
    if (session != null) {
      state = {
        ...state,
        sessionId: session.copyWith(background: background),
      };
    }
  }
}
