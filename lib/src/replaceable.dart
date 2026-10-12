import 'package:ndk/ndk.dart';

/// NIP-01 address shared by every version of a replaceable or addressable
/// [event], or `null` for any other kind.
String? replaceableAddress(Nip01Event event) {
  final kind = event.kind;
  if (kind == 0 || kind == 3 || (kind >= 10000 && kind < 20000)) {
    return '$kind:${event.pubKey}:';
  }
  if (kind >= 30000 && kind < 40000) {
    return '$kind:${event.pubKey}:${event.getDtag() ?? ''}';
  }
  return null;
}

/// Whether relays keep [a] over [b], two versions sharing an address: the
/// latest `created_at` wins, then the lowest id (NIP-01).
bool replaces(Nip01Event a, Nip01Event b) => a.createdAt != b.createdAt
    ? a.createdAt > b.createdAt
    : a.id.compareTo(b.id) < 0;
