import 'dart:convert';

import 'package:ndk/entities.dart' show Nip65, ReadWriteMarker;
import 'package:ndk/ndk.dart' hide RelaySet;

import 'indexer_relays.dart';
import 'relay_set.dart';

const _kPrivateRelays = 10013;

/// Builds a [RelayListFn] that reads kind 10002, 10050 and 10013 through
/// [ndk], cache included, so a list seen before resolves offline.
///
/// Without an event, the lookup is `notFound` when at least one relay sent
/// EOSE, and `unavailable` otherwise. The lists are queried on
/// [discoveryRelays], not on NDK's bootstrap relays, plus the pubkey's write
/// relays for a DM or private relay list. [queryTimeout] bounds each query and
/// defaults to NDK's query timeout.
///
/// A private relay list is decrypted through `ndk.decryptedEventPayloads`, so
/// a given list event is decrypted once and read from NDK's cache afterwards,
/// account or not. Otherwise the signer of the matching account in
/// `ndk.accounts` decrypts it, with no timeout. A missing account or a failed
/// decryption is `unavailable`, so the lookup is retried.
RelayListFn ndkRelayListFn(
  Ndk ndk, {
  Iterable<String> discoveryRelays = defaultIndexerRelays,
  Duration? queryTimeout,
}) {
  return (pubkey, kind, outboxRelays) async {
    final response = ndk.requests.query(
      filter: Filter(authors: [pubkey], kinds: [_eventKind(kind)]),
      explicitRelays: {...outboxRelays, ...discoveryRelays},
      timeout: queryTimeout,
    );
    final events = await response.future;
    if (events.isNotEmpty) {
      final latest = events.reduce((a, b) => b.createdAt > a.createdAt ? b : a);
      if (kind != RelayListKind.private) {
        return RelayLookup.found(relaysOfList(latest, kind));
      }
      return _decryptPrivateRelays(ndk, pubkey, latest);
    }
    final outcomes = await response.relayOutcomesDone;
    if (outcomes.values.any((o) => o.status == RelayRequestStatus.eose)) {
      return const RelayLookup.notFound();
    }
    return RelayLookup.unavailable(
      outcomes.isEmpty
          ? 'no relay reached'
          : outcomes.entries.map((e) => '${e.key}: ${e.value}').join(', '),
    );
  };
}

int _eventKind(RelayListKind kind) => switch (kind) {
  RelayListKind.dm => Nip51List.kDmRelays,
  RelayListKind.private => _kPrivateRelays,
  RelayListKind.outbox ||
  RelayListKind.inbox ||
  RelayListKind.nip65 => Nip65.kKind,
};

Future<RelayLookup> _decryptPrivateRelays(
  Ndk ndk,
  String owner,
  Nip01Event event,
) async {
  if (event.content.isEmpty) return const RelayLookup.found([]);
  final payloads = ndk.decryptedEventPayloads;
  final cached = await payloads.loadCachedPlaintext(
    eventId: event.id,
    viewerPubKey: owner,
  );
  if (cached != null) return RelayLookup.found(relaysOfPrivateTags(cached));

  final account = ndk.accounts.accounts[owner];
  if (account == null) {
    return RelayLookup.unavailable('account $owner is not in NDK');
  }
  final String? plaintext;
  try {
    plaintext = await payloads.loadOrDecrypt(
      event: event,
      viewerPubKey: owner,
      scheme: DecryptedPayloadScheme.nip44,
      decrypt: () => account.signer.decryptNip44(
        ciphertext: event.content,
        senderPubKey: event.pubKey,
      ),
    );
  } catch (e) {
    return RelayLookup.unavailable('private relay list not decrypted: $e');
  }
  if (plaintext == null) {
    return const RelayLookup.unavailable('private relay list not decrypted');
  }
  return RelayLookup.found(relaysOfPrivateTags(plaintext));
}

/// Relay URLs of the decrypted private tags of a kind 10013. Anything that is
/// not a JSON list of tags yields no relay.
List<String> relaysOfPrivateTags(String plaintext) {
  final Object? tags;
  try {
    tags = jsonDecode(plaintext);
  } on FormatException {
    return const [];
  }
  if (tags is! List) return const [];
  return [
    for (final tag in tags)
      if (tag is List &&
          tag.length >= 2 &&
          tag[0] == 'relay' &&
          tag[1] is String)
        tag[1] as String,
  ];
}

/// Relay URLs of a public relay list [event] for [kind]. A private list must
/// be decrypted first, see [relaysOfPrivateTags].
List<String> relaysOfList(Nip01Event event, RelayListKind kind) {
  switch (kind) {
    case RelayListKind.dm:
      return [
        for (final tag in event.tags)
          if (tag.length >= 2 && tag[0] == Nip51List.kRelay) tag[1],
      ];
    case RelayListKind.private:
      throw ArgumentError.value(kind, 'kind', 'is encrypted');
    case RelayListKind.nip65:
      return Nip65.fromEvent(event).relays.keys.toList();
    case RelayListKind.outbox:
    case RelayListKind.inbox:
      final wanted = kind == RelayListKind.outbox
          ? (ReadWriteMarker m) => m.isWrite
          : (ReadWriteMarker m) => m.isRead;
      return [
        for (final entry in Nip65.fromEvent(event).relays.entries)
          if (wanted(entry.value)) entry.key,
      ];
  }
}
