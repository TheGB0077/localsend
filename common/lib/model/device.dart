import 'package:dart_mappable/dart_mappable.dart';

part 'device.mapper.dart';

@MappableEnum(defaultValue: DeviceType.desktop)
enum DeviceType {
  mobile,
  desktop,
  web,
  headless,
  server,
}

@MappableClass()
sealed class DiscoveryMethod with DiscoveryMethodMappable {
  const DiscoveryMethod();
}

@MappableClass()
class MulticastDiscovery extends DiscoveryMethod with MulticastDiscoveryMappable {
  const MulticastDiscovery();
}

@MappableClass()
class HttpDiscovery extends DiscoveryMethod with HttpDiscoveryMappable {
  final String ip;

  const HttpDiscovery({required this.ip});
}

@MappableClass()
class IrohDiscovery extends DiscoveryMethod with IrohDiscoveryMappable {
  /// The iroh ticket string for connecting to this device.
  final String irohTicket;

  const IrohDiscovery({required this.irohTicket});
}

enum TransmissionMethod {
  http('HTTP'),
  iroh('Iroh');

  final String label;

  const TransmissionMethod(this.label);
}

/// Internal device model.
/// It gets not serialized.
@MappableClass()
class Device with DeviceMappable {
  /// A unique ID provided by the signaling server.
  final String? signalingId;

  /// The IP address of the device.
  /// Is null when found via signaling.
  final String? ip;

  final String version;
  final int port;
  final bool https;
  final String fingerprint;
  final String alias;
  final String? deviceModel;
  final DeviceType deviceType;
  final bool download;
  final Set<DiscoveryMethod> discoveryMethods;

  Set<TransmissionMethod> get transmissionMethods {
    bool http = false;
    bool iroh = false;

    for (final method in discoveryMethods) {
      if (method is IrohDiscovery) {
        iroh = true;
      } else {
        http = true;
      }
    }

    final methods = <TransmissionMethod>{};
    if (http) {
      methods.add(TransmissionMethod.http);
    }
    if (iroh) {
      methods.add(TransmissionMethod.iroh);
    }

    return methods;
  }

  /// Whether this device supports iroh transport.
  bool get supportsIroh => discoveryMethods.any((m) => m is IrohDiscovery);

  /// The iroh ticket for this device, if available and non-empty.
  /// During discovery this may be empty (capability-only) — the ticket
  /// is generated at send time when both devices support iroh.
  String? get irohTicket {
    for (final method in discoveryMethods) {
      if (method is IrohDiscovery) {
        final ticket = method.irohTicket;
        if (ticket.isNotEmpty) return ticket;
      }
    }
    return null;
  }

  const Device({
    required this.signalingId,
    required this.ip,
    required this.version,
    required this.port,
    required this.https,
    required this.fingerprint,
    required this.alias,
    required this.deviceModel,
    required this.deviceType,
    required this.download,
    required this.discoveryMethods,
  });

  static const empty = Device(
    signalingId: null,
    ip: null,
    version: '',
    port: -1,
    https: false,
    fingerprint: '',
    alias: '',
    deviceModel: null,
    deviceType: DeviceType.desktop,
    download: false,
    discoveryMethods: {},
  );
}
