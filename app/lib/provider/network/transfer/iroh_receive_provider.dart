// Iroh receive transport provider.
//
// Manages incoming file transfers from iroh-capable peers.
// The ticket comes from the Device model (IrohDiscovery), populated
// by the existing Dart multicast listener — no separate Rust listener needed.
// Produces [ReceiveSessionState] so the existing UI pages work as-is.

import 'dart:convert';

import 'package:common/model/device.dart';
import 'package:common/model/dto/file_dto.dart';
import 'package:common/model/file_status.dart';
import 'package:common/model/file_type.dart';
import 'package:common/model/session_status.dart';
import 'package:localsend_app/model/state/server/receive_session_state.dart';
import 'package:localsend_app/model/state/server/receiving_file.dart';
import 'package:localsend_app/provider/progress_provider.dart';
import 'package:localsend_app/rust/api/iroh_transfer.dart' as rust;
import 'package:logging/logging.dart';
import 'package:refena_flutter/refena_flutter.dart';
import 'package:uuid/uuid.dart';

const _uuid = Uuid();
final _logger = Logger('IrohReceive');

/// Provider for the current iroh receive session.
final irohReceiveProvider = NotifierProvider<IrohReceiveNotifier, IrohReceiveState>((ref) {
  return IrohReceiveNotifier();
});

/// State for the iroh receive flow.
class IrohReceiveState {
  /// The current receive session, if any.
  final ReceiveSessionState? session;

  /// The Rust receiver handle, kept alive during the session.
  final rust.RsIrohReceiver? receiver;

  /// The iroh ticket for the current session.
  final String? ticket;

  const IrohReceiveState({
    this.session,
    this.receiver,
    this.ticket,
  });
}

class IrohReceiveNotifier extends Notifier<IrohReceiveState> {
  /// Guard flag: true while a ticket is being processed (from first
  /// handleIrohTicket call until startSession completes). Prevents
  /// burst multicast announcements from creating multiple sessions.
  bool _processing = false;

  @override
  IrohReceiveState init() => const IrohReceiveState();

  /// Whether a ticket is currently being processed.
  bool get isProcessing => _processing;

  /// Start a receive session from a discovered iroh-capable device.
  ///
  /// The [ticket] comes from `Device.irohTicket` (populated by the Dart
  /// multicast listener when it receives a message with an `irohTicket` field).
  /// [destinationDirectory] is where files will be saved.
  Future<void> startSession({
    required String ticket,
    required String destinationDirectory,
  }) async {
    _processing = true;
    try {
      final receiver = await rust.irohReceiverNew(useRelay: false);

      state = IrohReceiveState(receiver: receiver, ticket: ticket);

      // Fetch the collection metadata from the provider.
      final collectionJson = await rust.irohReceiverFetchCollection(
        receiver: receiver,
        ticket: ticket,
      );

      final collection = jsonDecode(collectionJson) as Map<String, dynamic>;
      final files = (collection['files'] as List).cast<Map<String, dynamic>>();

      // Build a Device from the collection metadata.
      final sender = Device(
        signalingId: null,
        ip: null,
        version: collection['version'] as String? ?? '',
        port: 53317,
        https: false,
        fingerprint: collection['senderFingerprint'] as String? ?? '',
        alias: collection['senderAlias'] as String? ?? '',
        deviceModel: null,
        deviceType: DeviceType.desktop,
        download: false,
        discoveryMethods: {},
      );

      // Build ReceivingFile entries.
      final receivingFiles = <String, ReceivingFile>{};
      for (int i = 0; i < files.length; i++) {
        final f = files[i];
        final id = _uuid.v4();
        final fileTypeStr = f['fileType'] as String? ?? 'other';
        receivingFiles[id] = ReceivingFile(
          file: FileDto(
            id: id,
            fileName: f['fileName'] as String? ?? 'unknown',
            size: (f['size'] as num?)?.toInt() ?? 0,
            fileType: _parseFileType(fileTypeStr),
            hash: f['hash'] as String?,
            preview: null,
            metadata: null,
          ),
          status: FileStatus.queue,
          token: null,
          desiredName: null,
          path: null,
          savedToGallery: false,
          errorMessage: null,
        );
      }

      final sessionId = _uuid.v4();
      final sessionState = ReceiveSessionState(
        sessionId: sessionId,
        status: SessionStatus.waiting,
        sender: sender,
        senderAlias: sender.alias,
        files: receivingFiles,
        startTime: null,
        endTime: null,
        destinationDirectory: destinationDirectory,
        cacheDirectory: '',
        saveToGallery: false,
        createdDirectories: {},
        responseHandler: null,
      );

      state = IrohReceiveState(
        session: sessionState,
        receiver: receiver,
        ticket: ticket,
      );

      _logger.info('Iroh receive session started: ${sender.alias} with ${files.length} files');
    } catch (e, st) {
      _logger.warning('Iroh receive start failed', e, st);
      state = const IrohReceiveState();
    } finally {
      _processing = false;
    }
  }

  /// Accept the file request and start downloading.
  /// [fileNameMap]: file ID → desired output name.
  Future<void> acceptFileRequest(Map<String, String> fileNameMap) async {
    final s = state;
    if (s.session == null || s.receiver == null || s.ticket == null) return;

    // Update files: mark selected as queue with desiredName, others as skipped.
    final updatedFiles = <String, ReceivingFile>{};
    final selectedIndices = <int>[];
    final fileEntries = s.session!.files.entries.toList();

    for (int i = 0; i < fileEntries.length; i++) {
      final entry = fileEntries[i];
      final desiredName = fileNameMap[entry.key];
      if (desiredName != null) {
        selectedIndices.add(i);
        updatedFiles[entry.key] = entry.value.copyWith(
          status: FileStatus.queue,
          desiredName: desiredName,
        );
      } else {
        updatedFiles[entry.key] = entry.value.copyWith(
          status: FileStatus.skipped,
        );
      }
    }

    state = IrohReceiveState(
      session: s.session!.copyWith(
        status: SessionStatus.sending,
        files: updatedFiles,
        startTime: DateTime.now().millisecondsSinceEpoch,
      ),
      receiver: s.receiver,
      ticket: s.ticket,
    );

    // Download in background.
    _downloadFiles(selectedIndices);
  }

  /// Decline the file request.
  void declineFileRequest() {
    final s = state;
    if (s.session == null) return;

    state = IrohReceiveState(
      session: s.session!.copyWith(
        status: SessionStatus.declined,
        endTime: DateTime.now().millisecondsSinceEpoch,
      ),
      receiver: s.receiver,
      ticket: s.ticket,
    );
  }

  /// Cancel the session.
  void cancelSession() {
    final s = state;
    if (s.session == null) return;

    if (s.receiver != null) {
      try {
        rust.irohReceiverCancel(receiver: s.receiver!);
      } catch (_) {}
    }

    state = IrohReceiveState(
      session: s.session!.copyWith(
        status: SessionStatus.canceledByReceiver,
        endTime: DateTime.now().millisecondsSinceEpoch,
      ),
      receiver: s.receiver,
      ticket: s.ticket,
    );
  }

  /// Close the session locally.
  void closeSession() {
    ref.notifier(progressProvider).removeSession(state.session?.sessionId ?? '');
    state = const IrohReceiveState();
  }

  void setSessionDestinationDir(String dir) {
    final s = state;
    if (s.session == null) return;
    state = IrohReceiveState(
      session: s.session!.copyWith(destinationDirectory: dir),
      receiver: s.receiver,
      ticket: s.ticket,
    );
  }

  void setSessionSaveToGallery(bool save) {
    final s = state;
    if (s.session == null) return;
    state = IrohReceiveState(
      session: s.session!.copyWith(saveToGallery: save),
      receiver: s.receiver,
      ticket: s.ticket,
    );
  }

  /// Internal: download selected files from the provider.
  Future<void> _downloadFiles(List<int> indices) async {
    final s = state;
    if (s.session == null || s.receiver == null || s.ticket == null) return;

    try {
      final totalBytes = await rust.irohReceiverDownload(
        receiver: s.receiver!,
        ticket: s.ticket!,
        fileIndicesJson: jsonEncode(indices),
        outputDir: s.session!.destinationDirectory,
      );

      _logger.info('Download complete: $totalBytes bytes');

      // Mark all selected files as finished.
      final updatedFiles = <String, ReceivingFile>{};
      for (final entry in s.session!.files.entries) {
        if (entry.value.status == FileStatus.queue || entry.value.status == FileStatus.sending) {
          updatedFiles[entry.key] = entry.value.copyWith(
            status: FileStatus.finished,
            path: '${s.session!.destinationDirectory}/${entry.value.desiredName}',
          );
        } else {
          updatedFiles[entry.key] = entry.value;
        }
      }

      state = IrohReceiveState(
        session: s.session!.copyWith(
          status: SessionStatus.finished,
          files: updatedFiles,
          endTime: DateTime.now().millisecondsSinceEpoch,
        ),
        receiver: s.receiver,
        ticket: s.ticket,
      );
    } catch (e, st) {
      _logger.warning('Download failed', e, st);

      final updatedFiles = <String, ReceivingFile>{};
      for (final entry in s.session!.files.entries) {
        if (entry.value.status == FileStatus.queue || entry.value.status == FileStatus.sending) {
          updatedFiles[entry.key] = entry.value.copyWith(
            status: FileStatus.failed,
            errorMessage: e.toString(),
          );
        } else {
          updatedFiles[entry.key] = entry.value;
        }
      }

      state = IrohReceiveState(
        session: s.session!.copyWith(
          status: SessionStatus.finishedWithErrors,
          files: updatedFiles,
          endTime: DateTime.now().millisecondsSinceEpoch,
        ),
        receiver: s.receiver,
        ticket: s.ticket,
      );
    }
  }
}

DeviceType _parseDeviceType(String? type) {
  if (type == null) return DeviceType.desktop;
  return DeviceType.values.firstWhere(
    (e) => e.name.toLowerCase() == type.toLowerCase(),
    orElse: () => DeviceType.desktop,
  );
}

FileType _parseFileType(String type) {
  return FileType.values.firstWhere(
    (e) => e.name.toLowerCase() == type.toLowerCase(),
    orElse: () => FileType.other,
  );
}
