import 'package:collection/collection.dart';
import 'package:common/model/device.dart';
import 'package:common/model/session_status.dart';
import 'package:flutter/material.dart';
import 'package:localsend_app/model/cross_file.dart';
import 'package:localsend_app/model/persistence/favorite_device.dart';
import 'package:localsend_app/model/send_mode.dart';
import 'package:localsend_app/pages/progress_page.dart';
import 'package:localsend_app/pages/send_page.dart';
import 'package:localsend_app/pages/web_send_page.dart';
import 'package:localsend_app/provider/favorites_provider.dart';
import 'package:localsend_app/provider/device_info_provider.dart';
import 'package:localsend_app/provider/local_ip_provider.dart';
import 'package:localsend_app/provider/network/nearby_devices_provider.dart';
import 'package:localsend_app/provider/network/scan_facade.dart';
import 'package:localsend_app/provider/network/send_provider.dart';
import 'package:localsend_app/provider/network/transfer/iroh_send_provider.dart';
import 'package:localsend_app/provider/network/transfer/transport_factory.dart';
import 'package:localsend_app/provider/network/transfer/transport_interface.dart';
import 'package:localsend_app/provider/selection/selected_sending_files_provider.dart';
import 'package:localsend_app/provider/settings_provider.dart';
import 'package:localsend_app/util/favorites.dart';
import 'package:localsend_app/widget/dialogs/address_input_dialog.dart';
import 'package:localsend_app/widget/dialogs/favorite_delete_dialog.dart';
import 'package:localsend_app/widget/dialogs/favorite_dialog.dart';
import 'package:localsend_app/widget/dialogs/favorite_edit_dialog.dart';
import 'package:localsend_app/widget/dialogs/no_files_dialog.dart';
import 'package:refena_flutter/refena_flutter.dart';
import 'package:routerino/routerino.dart';

class SendTabVm {
  final SendMode sendMode;
  final List<CrossFile> selectedFiles;
  final List<String> localIps;
  final Iterable<Device> nearbyDevices;
  final List<FavoriteDevice> favoriteDevices;
  final Future<void> Function(BuildContext context) onTapAddress;
  final Future<void> Function(BuildContext context) onTapFavorite;
  final Future<void> Function(BuildContext context, SendMode mode) onTapSendMode;
  final Future<void> Function(BuildContext context, Device device) onToggleFavorite;
  final Future<void> Function(BuildContext context, Device device) onTapDevice;
  final Future<void> Function(BuildContext context, Device device) onTapDeviceMultiSend;

  const SendTabVm({
    required this.sendMode,
    required this.selectedFiles,
    required this.localIps,
    required this.nearbyDevices,
    required this.favoriteDevices,
    required this.onTapAddress,
    required this.onTapFavorite,
    required this.onTapSendMode,
    required this.onToggleFavorite,
    required this.onTapDevice,
    required this.onTapDeviceMultiSend,
  });
}

final sendTabVmProvider = ViewProvider((ref) {
  final sendMode = ref.watch(settingsProvider.select((s) => s.sendMode));
  final selectedFiles = ref.watch(selectedSendingFilesProvider);
  final localIps = ref.watch(localIpProvider).localIps;
  final nearbyDevices = ref.watch(nearbyDevicesProvider).allDevices.values;
  final favoriteDevices = ref.watch(favoritesProvider);

  return SendTabVm(
    sendMode: sendMode,
    selectedFiles: selectedFiles,
    localIps: localIps,
    nearbyDevices: nearbyDevices,
    favoriteDevices: favoriteDevices,
    onTapAddress: (context) async {
      final files = ref.read(selectedSendingFilesProvider);
      if (files.isEmpty) {
        await context.pushBottomSheet(() => const NoFilesDialog());
        return;
      }
      final device = await showDialog<Device?>(
        context: context,
        builder: (_) => const AddressInputDialog(),
      );
      if (device != null && context.mounted) {
        // Manual address entry always uses HTTP (no iroh discovery)
        await ref
            .notifier(sendProvider)
            .startSession(
              target: device,
              files: files,
              background: false,
            );
      }
    },
    onTapFavorite: (context) async {
      final device = await showDialog<Device?>(
        context: context,
        builder: (_) => const FavoritesDialog(),
      );
      if (device != null && context.mounted) {
        final files = ref.read(selectedSendingFilesProvider);
        if (files.isEmpty) {
          await context.pushBottomSheet(() => const NoFilesDialog());
          return;
        }

        await _startSessionWithTransport(
          ref: ref,
          target: device,
          files: files,
          background: false,
        );
      }
    },
    onTapSendMode: (context, mode) async {
      if (mode == SendMode.link) {
        final files = ref.read(selectedSendingFilesProvider);
        if (files.isEmpty) {
          await context.pushBottomSheet(() => const NoFilesDialog());
          return;
        }
        await context.push(() => WebSendPage(files));
        return;
      }

      await ref.notifier(settingsProvider).setSendMode(mode);
      if (mode != SendMode.multiple) {
        ref.notifier(sendProvider).clearAllSessions();
      }
    },
    onToggleFavorite: (context, device) async {
      final favoriteDevice = favoriteDevices.findDevice(device);
      if (favoriteDevice != null) {
        final result = await showDialog<bool>(
          context: context,
          builder: (_) => FavoriteDeleteDialog(favoriteDevice),
        );
        if (result == true) {
          await ref.redux(favoritesProvider).dispatchAsync(RemoveFavoriteAction(deviceFingerprint: device.fingerprint));
        }
      } else {
        await showDialog(
          context: context,
          builder: (_) => FavoriteEditDialog(prefilledDevice: device),
        );
      }
    },
    onTapDevice: (context, device) async {
      if (selectedFiles.isEmpty) {
        await context.pushBottomSheet(() => const NoFilesDialog());
        return;
      }

      await _startSessionWithTransport(
        ref: ref,
        target: device,
        files: selectedFiles,
        background: false,
      );
    },
    onTapDeviceMultiSend: (context, device) async {
      // Check for existing sessions in either HTTP or iroh provider
      final httpSession = ref.read(sendProvider).values.firstWhereOrNull((s) => s.target.ip == device.ip);
      final irohSession = ref.read(irohSendProvider).values.firstWhereOrNull((s) => s.target.ip == device.ip);
      final session = httpSession ?? irohSession;
      final isIroh = irohSession != null;

      if (session != null) {
        if (session.status == SessionStatus.waiting) {
          _setBackground(ref, session.sessionId, isIroh, false);
          await context.push(
            () => SendPage(showAppBar: true, closeSessionOnClose: false, sessionId: session.sessionId),
            transition: RouterinoTransition.fade(),
          );
          _setBackground(ref, session.sessionId, isIroh, true);
          return;
        } else if (session.status == SessionStatus.sending || session.status == SessionStatus.finishedWithErrors) {
          _setBackground(ref, session.sessionId, isIroh, false);
          await context.push(() => ProgressPage(showAppBar: true, closeSessionOnClose: false, sessionId: session.sessionId));
          _setBackground(ref, session.sessionId, isIroh, true);
          return;
        }
      }

      final files = ref.read(selectedSendingFilesProvider);
      if (files.isEmpty) {
        await context.pushBottomSheet(() => const NoFilesDialog());
        return;
      }

      if (session != null) {
        // close old session
        _closeSession(ref, session.sessionId, isIroh);
      }

      await _startSessionWithTransport(
        ref: ref,
        target: device,
        files: files,
        background: true,
      );
    },
  );
});

class SendTabInitAction extends AsyncGlobalAction {
  final BuildContext context;

  SendTabInitAction(this.context);

  @override
  Future<void> reduce() async {
    final devices = ref.read(nearbyDevicesProvider).devices;
    if (devices.isEmpty) {
      await dispatchAsync(StartSmartScan(forceLegacy: false));
    }
  }
}

/// Start a send session using the appropriate transport.
///
/// If the target device supports iroh (discovered via multicast with an
/// iroh ticket), use iroh transport. Otherwise fall back to HTTP v2.
Future<void> _startSessionWithTransport({
  required Ref ref,
  required Device target,
  required List<CrossFile> files,
  required bool background,
}) async {
  if (target.supportsIroh) {
    final deviceInfo = ref.read(deviceFullInfoProvider);
    await ref
        .notifier(irohSendProvider)
        .startSession(
          target: target,
          files: files,
          background: background,
          senderAlias: deviceInfo.alias,
          senderFingerprint: deviceInfo.fingerprint,
          version: deviceInfo.version,
        );
  } else {
    await ref
        .notifier(sendProvider)
        .startSession(
          target: target,
          files: files,
          background: background,
        );
  }
}

void _setBackground(Ref ref, String sessionId, bool isIroh, bool background) {
  if (isIroh) {
    ref.notifier(irohSendProvider).setBackground(sessionId, background);
  } else {
    ref.notifier(sendProvider).setBackground(sessionId, background);
  }
}

void _closeSession(Ref ref, String sessionId, bool isIroh) {
  if (isIroh) {
    ref.notifier(irohSendProvider).closeSession(sessionId);
  } else {
    ref.notifier(sendProvider).closeSession(sessionId);
  }
}
