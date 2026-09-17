/// Which relay list of an account a [RelaySet] leaf reads.
enum RelayListKind {
  /// NIP-65 (kind 10002) relays marked for writing, or unmarked.
  outbox,

  /// NIP-65 (kind 10002) relays marked for reading, or unmarked.
  inbox,

  /// Every NIP-65 (kind 10002) relay, whatever its marker.
  nip65,

  /// NIP-17 DM relays (kind 10050).
  dm,

  /// NIP-37 private relays (kind 10013), encrypted to their owner.
  private;

  /// Whether the list is published on the owner's NIP-65 write relays.
  bool get onOutbox => this == dm || this == private;
}

/// Looks up one relay list of [pubkey]. Called by the worker, never by
/// `broadcast`, so it may hit the network.
///
/// [outboxRelays] are the NIP-65 write relays of [pubkey], where it publishes
/// its own lists. They are given when [RelayListKind.onOutbox] (empty when
/// [pubkey] has no NIP-65) and should be queried along with any discovery
/// relays. They are always empty for the NIP-65 kinds.
///
/// The shim never times a lookup out: bound the network part, but let a signer
/// take as long as it needs.
typedef RelayListFn =
    Future<RelayLookup> Function(
      String pubkey,
      RelayListKind kind,
      List<String> outboxRelays,
    );

/// Outcome of a single relay list lookup.
sealed class RelayLookup {
  const RelayLookup();

  /// The list exists. [relays] may be empty.
  const factory RelayLookup.found(List<String> relays) = RelayListFound;

  /// Relays answered (EOSE) and none of them holds the list.
  const factory RelayLookup.notFound() = RelayListNotFound;

  /// No relay answered, so whether the list exists is unknown.
  const factory RelayLookup.unavailable([String? reason]) =
      RelayListUnavailable;
}

/// See [RelayLookup.found].
final class RelayListFound extends RelayLookup {
  /// Relay URLs of the list, in the order the list gives them.
  final List<String> relays;

  /// Creates a found lookup.
  const RelayListFound(this.relays);
}

/// See [RelayLookup.notFound].
final class RelayListNotFound extends RelayLookup {
  /// Creates a not-found lookup.
  const RelayListNotFound();
}

/// See [RelayLookup.unavailable].
final class RelayListUnavailable extends RelayLookup {
  /// Why the lookup could not conclude, if known.
  final String? reason;

  /// Creates an unavailable lookup.
  const RelayListUnavailable([this.reason]);
}

/// Where a queued event should go, described rather than listed so it can be
/// persisted offline and resolved later by the worker.
///
/// Resolution runs once: the first time every lookup in the tree concludes,
/// the resulting relays are frozen into the queued record.
///
/// `package:ndk/ndk.dart` also exports a `RelaySet`. In a file importing both,
/// add `hide RelaySet` to the ndk import.
sealed class RelaySet {
  const RelaySet();

  /// A fixed list of relay URLs. Needs no lookup.
  const factory RelaySet.explicit(List<String> relays) = ExplicitRelays;

  /// NIP-65 write relays of [pubkey].
  const factory RelaySet.outbox(String pubkey) = OutboxRelays;

  /// Every relay of the NIP-65 of [pubkey], read and write alike.
  const factory RelaySet.nip65(String pubkey) = Nip65Relays;

  /// NIP-65 read relays of every pubkey in [pubkeys], merged.
  const factory RelaySet.inbox(List<String> pubkeys) = InboxRelays;

  /// NIP-17 DM relays (kind 10050) of every pubkey in [pubkeys], merged. Use
  /// this for gift wraps, not [RelaySet.inbox].
  const factory RelaySet.dm(List<String> pubkeys) = DmRelays;

  /// NIP-37 private relays (kind 10013) of [pubkey]. The list is encrypted to
  /// its owner, so [pubkey] must be an account able to decrypt it locally.
  const factory RelaySet.private(String pubkey) = PrivateRelays;

  /// Every relay of every set in [sets].
  const factory RelaySet.union(List<RelaySet> sets) = UnionRelays;

  /// The relays of the first set in [sets] that resolves to a non-empty list.
  /// A list that is not found counts as empty. An unavailable one stops
  /// resolution, so an offline lookup never falls through to the next set.
  const factory RelaySet.fallback(List<RelaySet> sets) = FallbackRelays;

  /// Whether resolving this set calls a [RelayListFn].
  bool get needsLookup;

  /// Serializes the set for storage.
  Map<String, Object?> toMap();

  /// Inverse of [toMap].
  static RelaySet fromMap(Map map) {
    List<String> strings(String field) => (map[field] as List).cast<String>();
    List<RelaySet> sets() => (map['sets'] as List)
        .map((s) => RelaySet.fromMap(s as Map))
        .toList(growable: false);
    return switch (map['type']) {
      'explicit' => ExplicitRelays(strings('relays')),
      'outbox' => OutboxRelays(map['pubkey'] as String),
      'nip65' => Nip65Relays(map['pubkey'] as String),
      'inbox' => InboxRelays(strings('pubkeys')),
      'dm' => DmRelays(strings('pubkeys')),
      'private' => PrivateRelays(map['pubkey'] as String),
      'union' => UnionRelays(sets()),
      'fallback' => FallbackRelays(sets()),
      final type => throw FormatException('unknown relay set type: $type'),
    };
  }
}

/// See [RelaySet.explicit].
final class ExplicitRelays extends RelaySet {
  /// Relay URLs.
  final List<String> relays;

  /// Creates an explicit set.
  const ExplicitRelays(this.relays);

  @override
  bool get needsLookup => false;

  @override
  Map<String, Object?> toMap() => {'type': 'explicit', 'relays': relays};
}

/// See [RelaySet.outbox].
final class OutboxRelays extends RelaySet {
  /// Account whose write relays are targeted.
  final String pubkey;

  /// Creates an outbox set.
  const OutboxRelays(this.pubkey);

  @override
  bool get needsLookup => true;

  @override
  Map<String, Object?> toMap() => {'type': 'outbox', 'pubkey': pubkey};
}

/// See [RelaySet.nip65].
final class Nip65Relays extends RelaySet {
  /// Account whose NIP-65 relays are targeted.
  final String pubkey;

  /// Creates a NIP-65 set.
  const Nip65Relays(this.pubkey);

  @override
  bool get needsLookup => true;

  @override
  Map<String, Object?> toMap() => {'type': 'nip65', 'pubkey': pubkey};
}

/// See [RelaySet.inbox].
final class InboxRelays extends RelaySet {
  /// Accounts whose read relays are targeted.
  final List<String> pubkeys;

  /// Creates an inbox set.
  const InboxRelays(this.pubkeys);

  @override
  bool get needsLookup => pubkeys.isNotEmpty;

  @override
  Map<String, Object?> toMap() => {'type': 'inbox', 'pubkeys': pubkeys};
}

/// See [RelaySet.dm].
final class DmRelays extends RelaySet {
  /// Accounts whose DM relays are targeted.
  final List<String> pubkeys;

  /// Creates a DM set.
  const DmRelays(this.pubkeys);

  @override
  bool get needsLookup => pubkeys.isNotEmpty;

  @override
  Map<String, Object?> toMap() => {'type': 'dm', 'pubkeys': pubkeys};
}

/// See [RelaySet.private].
final class PrivateRelays extends RelaySet {
  /// Account whose private relays are targeted.
  final String pubkey;

  /// Creates a private set.
  const PrivateRelays(this.pubkey);

  @override
  bool get needsLookup => true;

  @override
  Map<String, Object?> toMap() => {'type': 'private', 'pubkey': pubkey};
}

/// See [RelaySet.union].
final class UnionRelays extends RelaySet {
  /// Sets to merge.
  final List<RelaySet> sets;

  /// Creates a union set.
  const UnionRelays(this.sets);

  @override
  bool get needsLookup => sets.any((s) => s.needsLookup);

  @override
  Map<String, Object?> toMap() => {
    'type': 'union',
    'sets': [for (final s in sets) s.toMap()],
  };
}

/// See [RelaySet.fallback].
final class FallbackRelays extends RelaySet {
  /// Sets to try in order.
  final List<RelaySet> sets;

  /// Creates a fallback set.
  const FallbackRelays(this.sets);

  @override
  bool get needsLookup => sets.any((s) => s.needsLookup);

  @override
  Map<String, Object?> toMap() => {
    'type': 'fallback',
    'sets': [for (final s in sets) s.toMap()],
  };
}

/// Result of resolving a whole [RelaySet] tree.
sealed class RelayResolution {
  const RelayResolution();
}

/// Every lookup concluded. [relays] may be empty and is not deduplicated.
final class RelaysResolved extends RelayResolution {
  final List<String> relays;
  const RelaysResolved(this.relays);
}

/// At least one lookup that the result depends on was unavailable.
final class RelaysUnavailable extends RelayResolution {
  final String reason;
  const RelaysUnavailable(this.reason);
}

/// Resolves [set] with [lookup]. Each `(pubkey, kind)` pair is looked up at
/// most once per call, and [RelaySet.fallback] never looks up the sets after
/// the first non-empty one. A DM or private relay list is looked up after the
/// NIP-65 of the same pubkey, on its write relays.
Future<RelayResolution> resolveRelaySet(RelaySet set, RelayListFn lookup) {
  final memo = <String, Future<RelayLookup>>{};
  return _resolve(
    set,
    (pubkey, kind, outboxRelays) => memo.putIfAbsent(
      '${kind.name}|$pubkey',
      () => lookup(pubkey, kind, outboxRelays),
    ),
  );
}

Future<RelayResolution> _resolve(RelaySet set, RelayListFn lookup) async {
  switch (set) {
    case ExplicitRelays(:final relays):
      return RelaysResolved(relays);
    case OutboxRelays(:final pubkey):
      return _resolveLists([pubkey], RelayListKind.outbox, lookup);
    case Nip65Relays(:final pubkey):
      return _resolveLists([pubkey], RelayListKind.nip65, lookup);
    case InboxRelays(:final pubkeys):
      return _resolveLists(pubkeys, RelayListKind.inbox, lookup);
    case DmRelays(:final pubkeys):
      return _resolveLists(pubkeys, RelayListKind.dm, lookup);
    case PrivateRelays(:final pubkey):
      return _resolveLists([pubkey], RelayListKind.private, lookup);
    case UnionRelays(:final sets):
      return _merge(
        await Future.wait([for (final s in sets) _resolve(s, lookup)]),
      );
    case FallbackRelays(:final sets):
      for (final s in sets) {
        final resolution = await _resolve(s, lookup);
        if (resolution is RelaysUnavailable) return resolution;
        if ((resolution as RelaysResolved).relays.isNotEmpty) return resolution;
      }
      return const RelaysResolved([]);
  }
}

Future<RelayResolution> _resolveLists(
  List<String> pubkeys,
  RelayListKind kind,
  RelayListFn lookup,
) async {
  return _merge(
    await Future.wait([
      for (final pubkey in pubkeys) _resolveList(pubkey, kind, lookup),
    ]),
  );
}

Future<RelayResolution> _resolveList(
  String pubkey,
  RelayListKind kind,
  RelayListFn lookup,
) async {
  RelayResolution unavailable(String? reason) => RelaysUnavailable(
    '${kind.name} relays of $pubkey unavailable'
    '${reason == null ? '' : ': $reason'}',
  );

  var outboxRelays = const <String>[];
  if (kind.onOutbox) {
    switch (await lookup(pubkey, RelayListKind.outbox, const [])) {
      case RelayListFound(:final relays):
        outboxRelays = relays;
      case RelayListNotFound():
        break;
      case RelayListUnavailable(:final reason):
        return unavailable('NIP-65 ${reason ?? 'unavailable'}');
    }
  }

  return switch (await lookup(pubkey, kind, outboxRelays)) {
    RelayListFound(:final relays) => RelaysResolved(relays),
    RelayListNotFound() => const RelaysResolved([]),
    RelayListUnavailable(:final reason) => unavailable(reason),
  };
}

RelayResolution _merge(List<RelayResolution> parts) {
  final unavailable = parts.whereType<RelaysUnavailable>();
  if (unavailable.isNotEmpty) {
    return RelaysUnavailable(unavailable.map((u) => u.reason).join('; '));
  }
  return RelaysResolved([
    for (final part in parts.cast<RelaysResolved>()) ...part.relays,
  ]);
}
