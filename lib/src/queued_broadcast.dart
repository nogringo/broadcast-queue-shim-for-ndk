import 'package:ndk/ndk.dart';

/// Status of a queued broadcast.
enum BroadcastStatus {
  /// At least one targeted relay has not acknowledged the event yet.
  pending,

  /// Every relay has either acknowledged the event or rejected it with a
  /// terminal NIP-01 OK prefix.
  failed,

  /// Every relay in `relays` has acknowledged the event at least once.
  /// Monotonic: once a record reaches this state the shim never demotes it
  /// back to `pending` on its own.
  delivered,
}

/// A single broadcast tracked by [OfflineBroadcast].
///
/// Immutable from the caller's perspective: every mutation goes through the
/// store and yields a fresh instance.
class QueuedBroadcast {
  /// Nostr event id. Combined with [pubkey] it forms the sembast record [key].
  final String id;

  /// Pubkey of the account this broadcast was queued under, or `null` if the
  /// caller did not attribute it. This is NOT `event.pubKey`: an event can be
  /// queued under an account that did not sign it (rebroadcasting someone
  /// else's note), and for a gift wrap (kind 1059) `event.pubKey` is an
  /// ephemeral throwaway key. It is part of the record's identity, so the same
  /// event queued under two pubkeys is two independent records.
  /// [OfflineBroadcast.clearLocalAccountData] matches on this field.
  final String? pubkey;

  /// The full event as it will be (re-)broadcast.
  final Nip01Event event;

  /// The list of relays this event must reach. Fixed at creation, but may grow
  /// if `rebroadcast(id, relay: ...)` introduces a new relay.
  final List<String> relays;

  /// Subset of [relays] that have returned `broadcastSuccessful: true` at
  /// least once across all attempts.
  final List<String> ackedRelays;

  /// Last error message seen per relay still pending. Cleared on ack.
  final Map<String, String> lastErrors;

  /// Terminal NIP-01 OK false messages per relay. These relays are no longer
  /// retried automatically for this event.
  final Map<String, String> terminalErrors;

  /// Consecutive online attempts where a relay was unreachable. Reset when the
  /// relay responds or a forced rebroadcast later succeeds.
  final Map<String, int> inaccessibleAttempts;

  /// Number of delivery attempts that have completed (success or failure)
  /// since the record was created.
  final int attempts;

  /// Wall-clock millis (since epoch) of the first attempt for this record,
  /// or null if none has run yet.
  final int? firstAttemptAt;

  /// Wall-clock millis (since epoch) of the most recent attempt, or null if
  /// none has run yet.
  final int? lastAttemptAt;

  /// Wall-clock millis (since epoch) at which the worker should attempt this
  /// record next. The periodic tick picks up records whose value is <= now.
  final int nextAttemptAt;

  /// Wall-clock millis (since epoch) of the first time every relay in
  /// [relays] had acknowledged the event. Monotonic: once set, never
  /// cleared by an attempt; only `rebroadcast(id, relay: r)` with a brand
  /// new relay (or `broadcast()` merging in an unacked relay) clears it.
  final int? deliveredAt;

  /// Wall-clock millis (since epoch) of the first time the entry became
  /// terminally failed: all relays were either acked or terminally rejected,
  /// with at least one terminal rejection.
  final int? failedAt;

  /// Wall-clock millis (since epoch) when this record was first persisted.
  final int createdAt;

  /// Override for the *next* attempt only: when non-null, the worker pushes
  /// the event to exactly this list of relays, even if some of them already
  /// have acks. Set by [OfflineBroadcast.rebroadcast]; cleared by the
  /// attempt itself once it runs. Existence is what makes a delivered entry
  /// eligible for one more push without rewriting its history.
  final List<String>? forcedRelays;

  /// Creates a record. Most callers should not invoke this directly; records
  /// are produced by [OfflineBroadcast].
  QueuedBroadcast({
    required this.id,
    this.pubkey,
    required this.event,
    required this.relays,
    required this.ackedRelays,
    required this.lastErrors,
    required this.terminalErrors,
    required this.inaccessibleAttempts,
    required this.attempts,
    required this.firstAttemptAt,
    required this.lastAttemptAt,
    required this.nextAttemptAt,
    required this.deliveredAt,
    required this.failedAt,
    required this.createdAt,
    this.forcedRelays,
  });

  /// The sembast record key for this entry.
  String get key => keyFor(eventId: id, pubkey: pubkey);

  /// Builds the sembast record key for an (event, account) pair: `pubkey|eventId`
  /// when bound to an account, the bare event id otherwise. Account-less entries
  /// keep their pre-0.4.0 key so existing databases keep working.
  static String keyFor({required String eventId, String? pubkey}) =>
      pubkey == null ? eventId : '$pubkey|$eventId';

  /// `pending` while any relay is still retryable, otherwise terminal.
  BroadcastStatus get status {
    if (deliveredAt != null) return BroadcastStatus.delivered;
    if (failedAt != null) return BroadcastStatus.failed;
    return BroadcastStatus.pending;
  }

  /// Relays still retryable, i.e. [relays] minus acks and terminal failures.
  List<String> get remainingRelays {
    final acked = ackedRelays.toSet();
    final terminal = terminalErrors.keys.toSet();
    return relays
        .where((r) => !acked.contains(r) && !terminal.contains(r))
        .toList(growable: false);
  }

  /// Returns a copy of this record with the given fields replaced.
  ///
  /// Use `clearDelivered: true` to force-clear [deliveredAt] (`null` arg
  /// alone is ambiguous with "leave as-is" for nullable fields). Same idea
  /// for `clearForcedRelays`.
  QueuedBroadcast copyWith({
    List<String>? relays,
    List<String>? ackedRelays,
    Map<String, String>? lastErrors,
    Map<String, String>? terminalErrors,
    Map<String, int>? inaccessibleAttempts,
    int? attempts,
    int? firstAttemptAt,
    int? lastAttemptAt,
    int? nextAttemptAt,
    int? deliveredAt,
    int? failedAt,
    List<String>? forcedRelays,
    bool clearDelivered = false,
    bool clearFailed = false,
    bool clearForcedRelays = false,
  }) {
    return QueuedBroadcast(
      id: id,
      pubkey: pubkey,
      event: event,
      relays: relays ?? this.relays,
      ackedRelays: ackedRelays ?? this.ackedRelays,
      lastErrors: lastErrors ?? this.lastErrors,
      terminalErrors: terminalErrors ?? this.terminalErrors,
      inaccessibleAttempts: inaccessibleAttempts ?? this.inaccessibleAttempts,
      attempts: attempts ?? this.attempts,
      firstAttemptAt: firstAttemptAt ?? this.firstAttemptAt,
      lastAttemptAt: lastAttemptAt ?? this.lastAttemptAt,
      nextAttemptAt: nextAttemptAt ?? this.nextAttemptAt,
      deliveredAt: clearDelivered ? null : (deliveredAt ?? this.deliveredAt),
      failedAt: clearFailed ? null : (failedAt ?? this.failedAt),
      createdAt: createdAt,
      forcedRelays: clearForcedRelays
          ? null
          : (forcedRelays ?? this.forcedRelays),
    );
  }

  /// Serializes the record for sembast storage.
  Map<String, dynamic> toMap() {
    return {
      'id': id,
      'pubkey': pubkey,
      'event': Nip01EventModel.fromEntity(event).toJson(),
      'relays': relays,
      'ackedRelays': ackedRelays,
      'lastErrors': lastErrors,
      'terminalErrors': terminalErrors,
      'inaccessibleAttempts': inaccessibleAttempts,
      'attempts': attempts,
      'firstAttemptAt': firstAttemptAt,
      'lastAttemptAt': lastAttemptAt,
      'nextAttemptAt': nextAttemptAt,
      'deliveredAt': deliveredAt,
      'failedAt': failedAt,
      'createdAt': createdAt,
      'forcedRelays': forcedRelays,
    };
  }

  /// Inverse of [toMap].
  static QueuedBroadcast fromMap(Map<String, dynamic> map) {
    return QueuedBroadcast(
      id: map['id'] as String,
      pubkey: map['pubkey'] as String?,
      event: Nip01EventModel.fromJson(map['event'] as Map),
      relays: (map['relays'] as List).cast<String>(),
      ackedRelays: (map['ackedRelays'] as List).cast<String>(),
      lastErrors: (map['lastErrors'] as Map).map(
        (k, v) => MapEntry(k as String, v as String),
      ),
      terminalErrors: ((map['terminalErrors'] as Map?) ?? const {}).map(
        (k, v) => MapEntry(k as String, v as String),
      ),
      inaccessibleAttempts: ((map['inaccessibleAttempts'] as Map?) ?? const {})
          .map((k, v) => MapEntry(k as String, v as int)),
      attempts: map['attempts'] as int,
      firstAttemptAt: map['firstAttemptAt'] as int?,
      lastAttemptAt: map['lastAttemptAt'] as int?,
      nextAttemptAt: map['nextAttemptAt'] as int,
      deliveredAt: map['deliveredAt'] as int?,
      failedAt: map['failedAt'] as int?,
      createdAt: map['createdAt'] as int,
      forcedRelays: (map['forcedRelays'] as List?)?.cast<String>(),
    );
  }
}
