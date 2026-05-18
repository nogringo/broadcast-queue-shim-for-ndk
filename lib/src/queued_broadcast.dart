import 'package:ndk/ndk.dart';

/// Status of a queued broadcast.
enum BroadcastStatus {
  /// At least one targeted relay has not acknowledged the event yet.
  pending,

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
  /// Nostr event id (also the sembast record key).
  final String id;

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
    required this.event,
    required this.relays,
    required this.ackedRelays,
    required this.lastErrors,
    required this.attempts,
    required this.firstAttemptAt,
    required this.lastAttemptAt,
    required this.nextAttemptAt,
    required this.deliveredAt,
    required this.createdAt,
    this.forcedRelays,
  });

  /// `pending` while any relay still owes an ack, otherwise `delivered`.
  BroadcastStatus get status =>
      deliveredAt != null ? BroadcastStatus.delivered : BroadcastStatus.pending;

  /// Relays still owed an ack, i.e. [relays] minus [ackedRelays].
  List<String> get remainingRelays {
    final acked = ackedRelays.toSet();
    return relays.where((r) => !acked.contains(r)).toList(growable: false);
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
    int? attempts,
    int? firstAttemptAt,
    int? lastAttemptAt,
    int? nextAttemptAt,
    int? deliveredAt,
    List<String>? forcedRelays,
    bool clearDelivered = false,
    bool clearForcedRelays = false,
  }) {
    return QueuedBroadcast(
      id: id,
      event: event,
      relays: relays ?? this.relays,
      ackedRelays: ackedRelays ?? this.ackedRelays,
      lastErrors: lastErrors ?? this.lastErrors,
      attempts: attempts ?? this.attempts,
      firstAttemptAt: firstAttemptAt ?? this.firstAttemptAt,
      lastAttemptAt: lastAttemptAt ?? this.lastAttemptAt,
      nextAttemptAt: nextAttemptAt ?? this.nextAttemptAt,
      deliveredAt: clearDelivered ? null : (deliveredAt ?? this.deliveredAt),
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
      'event': Nip01EventModel.fromEntity(event).toJson(),
      'relays': relays,
      'ackedRelays': ackedRelays,
      'lastErrors': lastErrors,
      'attempts': attempts,
      'firstAttemptAt': firstAttemptAt,
      'lastAttemptAt': lastAttemptAt,
      'nextAttemptAt': nextAttemptAt,
      'deliveredAt': deliveredAt,
      'createdAt': createdAt,
      'forcedRelays': forcedRelays,
    };
  }

  /// Inverse of [toMap].
  static QueuedBroadcast fromMap(Map<String, dynamic> map) {
    return QueuedBroadcast(
      id: map['id'] as String,
      event: Nip01EventModel.fromJson(map['event'] as Map),
      relays: (map['relays'] as List).cast<String>(),
      ackedRelays: (map['ackedRelays'] as List).cast<String>(),
      lastErrors: (map['lastErrors'] as Map).map(
        (k, v) => MapEntry(k as String, v as String),
      ),
      attempts: map['attempts'] as int,
      firstAttemptAt: map['firstAttemptAt'] as int?,
      lastAttemptAt: map['lastAttemptAt'] as int?,
      nextAttemptAt: map['nextAttemptAt'] as int,
      deliveredAt: map['deliveredAt'] as int?,
      createdAt: map['createdAt'] as int,
      forcedRelays: (map['forcedRelays'] as List?)?.cast<String>(),
    );
  }
}
