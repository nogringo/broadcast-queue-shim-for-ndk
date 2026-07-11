import 'dart:async';
import 'dart:math';

import 'package:ndk/entities.dart' show RelayBroadcastResponse;
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast.dart';

import 'backoff.dart';
import 'queue_store.dart';
import 'queued_broadcast.dart';
import 'relay_host_filter.dart';

/// Function that hands an event off to the network. Matches the call pattern
/// of `Ndk.broadcast.broadcast` with `specificRelays` always provided.
typedef BroadcastFn =
    NdkBroadcastResponse Function(Nip01Event event, List<String> relays);

/// Offline-first wrapper around NDK's broadcast.
///
/// Contract:
///  - `broadcast(event, relays: [...])` persists the event before returning.
///  - Delivery is guaranteed in the eventual sense: the shim keeps retrying
///    each `pending` entry until every relay in [relays] has acknowledged it
///    or returned a terminal NIP-01 rejection.
///  - Records are never auto-deleted. A terminal entry stays in the store for
///    manual `rebroadcast` or inspection.
class OfflineBroadcast {
  final BroadcastFn _broadcastFn;
  final QueueStore _store;
  final Duration _tickInterval;
  final Duration _initialBackoff;
  final Duration _maxBackoff;
  final Duration _perAttemptTimeout;
  final int _maxInaccessibleAttemptsPerRelay;
  final Random _random;
  final int Function() _now;
  final Stream<bool>? _onlineSignal;

  Timer? _tickTimer;
  StreamSubscription<bool>? _onlineSub;
  bool _isOnline = true;
  final Map<String, Future<void>> _inFlight = <String, Future<void>>{};
  bool _disposed = false;

  OfflineBroadcast._({
    required BroadcastFn broadcastFn,
    required Database db,
    required String storeName,
    required Duration tickInterval,
    required Duration initialBackoff,
    required Duration maxBackoff,
    required Duration perAttemptTimeout,
    required int maxInaccessibleAttemptsPerRelay,
    Stream<bool>? onlineSignal,
    Random? random,
    int Function()? now,
  }) : _broadcastFn = broadcastFn,
       _store = QueueStore(db: db, storeName: storeName),
       _tickInterval = tickInterval,
       _initialBackoff = initialBackoff,
       _maxBackoff = maxBackoff,
       _perAttemptTimeout = perAttemptTimeout,
       _maxInaccessibleAttemptsPerRelay = maxInaccessibleAttemptsPerRelay,
       _onlineSignal = onlineSignal,
       _random = random ?? Random(),
       _now = now ?? (() => DateTime.now().millisecondsSinceEpoch);

  /// Default constructor: inject the broadcast function explicitly.
  /// Useful for tests or for callers who already wrap NDK.
  ///
  /// Pass [onlineSignal] to make the periodic retry loop connectivity-aware:
  /// while the latest emission is `false`, periodic ticks are no-ops, and the
  /// `false -> true` edge triggers an immediate retry pass. `retryNow()`
  /// always runs regardless of this signal. If [onlineSignal] is null the
  /// shim assumes it is always online.
  ///
  /// [maxInaccessibleAttemptsPerRelay] caps consecutive online attempts where
  /// a relay is unreachable for one event. Once reached, that relay is treated
  /// as terminally failed for the event. Attempts while [onlineSignal] is
  /// `false`, global broadcaster exceptions, and relay-level OK responses do
  /// not increment this counter.
  factory OfflineBroadcast({
    required BroadcastFn broadcastFn,
    required Database db,
    String storeName = 'broadcasts',
    Duration tickInterval = const Duration(seconds: 30),
    Duration initialBackoff = const Duration(seconds: 5),
    Duration maxBackoff = const Duration(minutes: 30),
    Duration perAttemptTimeout = const Duration(seconds: 10),
    int maxInaccessibleAttemptsPerRelay = 12,
    Stream<bool>? onlineSignal,
    Random? random,
    int Function()? now,
  }) {
    if (maxInaccessibleAttemptsPerRelay <= 0) {
      throw ArgumentError.value(
        maxInaccessibleAttemptsPerRelay,
        'maxInaccessibleAttemptsPerRelay',
        'must be greater than zero',
      );
    }
    return OfflineBroadcast._(
      broadcastFn: broadcastFn,
      db: db,
      storeName: storeName,
      tickInterval: tickInterval,
      initialBackoff: initialBackoff,
      maxBackoff: maxBackoff,
      perAttemptTimeout: perAttemptTimeout,
      maxInaccessibleAttemptsPerRelay: maxInaccessibleAttemptsPerRelay,
      onlineSignal: onlineSignal,
      random: random,
      now: now,
    );
  }

  /// Convenience constructor wired to an [Ndk] instance.
  ///
  /// Derives an `onlineSignal` from `ndk.connectivity.relayConnectivityChanges`:
  /// the shim is considered "online" when at least one connected relay sits on
  /// a public-internet host (loopback, private IPv4/IPv6, and `.local` names
  /// are filtered out, so a connected dev relay on localhost will not mask a
  /// real outage).
  ///
  /// [maxInaccessibleAttemptsPerRelay] has the same meaning as on the default
  /// constructor.
  factory OfflineBroadcast.withNdk(
    Ndk ndk, {
    required Database db,
    String storeName = 'broadcasts',
    Duration tickInterval = const Duration(seconds: 30),
    Duration initialBackoff = const Duration(seconds: 5),
    Duration maxBackoff = const Duration(minutes: 30),
    Duration perAttemptTimeout = const Duration(seconds: 10),
    int maxInaccessibleAttemptsPerRelay = 12,
  }) {
    final onlineSignal = ndk.connectivity.relayConnectivityChanges
        .map(
          (relays) => relays.values.any(
            (rc) => rc.isConnected && isPublicRelayHost(rc.url),
          ),
        )
        .distinct();
    return OfflineBroadcast(
      broadcastFn: (event, relays) =>
          ndk.broadcast.broadcast(nostrEvent: event, specificRelays: relays),
      db: db,
      storeName: storeName,
      tickInterval: tickInterval,
      initialBackoff: initialBackoff,
      maxBackoff: maxBackoff,
      perAttemptTimeout: perAttemptTimeout,
      maxInaccessibleAttemptsPerRelay: maxInaccessibleAttemptsPerRelay,
      onlineSignal: onlineSignal,
    );
  }

  /// Persists [event] for delivery to every URL in [relays], then fires the
  /// first attempt in the background. The returned [QueuedBroadcast] reflects
  /// the persisted state, not the attempt outcome.
  ///
  /// If a record with the same `event.id` already exists, its target relays
  /// are merged with [relays] and it is rescheduled for an immediate attempt.
  /// The event payload is *not* overwritten; the original event wins.
  Future<QueuedBroadcast> broadcast(
    Nip01Event event, {
    required List<String> relays,
  }) async {
    _ensureNotDisposed();
    if (relays.isEmpty) {
      throw ArgumentError.value(relays, 'relays', 'must not be empty');
    }
    final normalizedRelays = _dedupNormalized(relays);
    final now = _now();

    final existing = await _store.get(event.id);
    final QueuedBroadcast record;
    if (existing != null) {
      final mergedRelays = _dedupNormalized([
        ...existing.relays,
        ...normalizedRelays,
      ]);
      // Terminal timestamps are monotonic facts, but adding a relay that is
      // not already covered by that terminal state reopens the entry.
      final fullyAcked = mergedRelays.every(existing.ackedRelays.contains);
      final fullySettled = mergedRelays.every(
        (relay) =>
            existing.ackedRelays.contains(relay) ||
            existing.terminalErrors.containsKey(relay),
      );
      record = existing.copyWith(
        relays: mergedRelays,
        nextAttemptAt: now,
        clearDelivered: !fullyAcked,
        clearFailed: !fullyAcked && !fullySettled,
      );
    } else {
      record = QueuedBroadcast(
        id: event.id,
        event: event,
        relays: normalizedRelays,
        ackedRelays: const [],
        lastErrors: const {},
        terminalErrors: const {},
        inaccessibleAttempts: const {},
        attempts: 0,
        firstAttemptAt: null,
        lastAttemptAt: null,
        nextAttemptAt: now,
        deliveredAt: null,
        failedAt: null,
        createdAt: now,
      );
    }
    await _store.put(record);

    // Kick off the first attempt without blocking the caller.
    unawaited(_attempt(record.id));
    return record;
  }

  /// Re-pushes a queued event without rewriting its delivery history.
  ///
  /// `ackedRelays` is monotonic and append-only over an entry's lifetime:
  /// a relay that has confirmed receipt stays confirmed forever. `rebroadcast`
  /// never clears acks; it sets a one-shot `forcedRelays` override that the
  /// next attempt consumes.
  ///
  /// - `relay == null`: schedules an immediate attempt that pushes to every
  ///   relay in the entry's `relays` list, including those already acked.
  ///   `deliveredAt` is preserved.
  /// - `relay != null`: adds [relay] to the entry's `relays` list if absent,
  ///   schedules an immediate one-shot push to that single relay. If the
  ///   relay is new, `deliveredAt` is cleared (the entry can no longer claim
  ///   100% delivery until the new relay acks); otherwise it is preserved.
  Future<QueuedBroadcast?> rebroadcast(String eventId, {String? relay}) async {
    _ensureNotDisposed();
    final now = _now();
    final updated = await _store.update(eventId, (current) {
      if (relay == null) {
        return current.copyWith(
          forcedRelays: List<String>.from(current.relays),
          nextAttemptAt: now,
        );
      }
      final normalized = _normalizeRelay(relay);
      final isNew = !current.relays.contains(normalized);
      final relays = isNew ? [...current.relays, normalized] : current.relays;
      return current.copyWith(
        relays: relays,
        forcedRelays: [normalized],
        nextAttemptAt: now,
        clearDelivered: isNew,
        clearFailed: isNew,
      );
    });
    if (updated != null) {
      unawaited(_attempt(eventId));
    }
    return updated;
  }

  /// Triggers an immediate scan for due entries. Safe to call repeatedly;
  /// in-flight attempts are not duplicated.
  Future<void> retryNow() async {
    _ensureNotDisposed();
    await _tick();
  }

  /// Returns the currently persisted record for [eventId], or `null` if none
  /// exists.
  Future<QueuedBroadcast?> get(String eventId) => _store.get(eventId);

  /// Live snapshot of the record for [eventId]. Emits `null` if/while the
  /// record is absent.
  Stream<QueuedBroadcast?> watch(String eventId) => _store.watch(eventId);

  /// Live snapshot of every record that still has retryable relays.
  Stream<List<QueuedBroadcast>> watchPending() => _store.watchPending();

  /// One-shot read of every record in the store, delivered or not.
  Future<List<QueuedBroadcast>> listAll() => _store.findAll();

  /// Starts the periodic retry timer and replays anything already due.
  /// Also subscribes to `onlineSignal` if one was provided. Idempotent:
  /// calling it more than once is a no-op.
  void start() {
    _ensureNotDisposed();
    if (_tickTimer != null) return;
    _tickTimer = Timer.periodic(_tickInterval, (_) => _periodicTick());
    if (_onlineSignal != null && _onlineSub == null) {
      _onlineSub = _onlineSignal.listen(_handleOnlineChange);
    }
    // Replay anything already due (e.g. after a process restart), respecting
    // the current online state. If a signal is configured and hasn't emitted
    // yet, [_isOnline] defaults to true so behavior matches the no-signal
    // case until the first event arrives.
    _periodicTick();
  }

  /// Stops the retry timer, cancels the connectivity subscription, and waits
  /// for any in-flight attempt to finish so the caller can safely close the
  /// underlying sembast database.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _tickTimer?.cancel();
    _tickTimer = null;
    await _onlineSub?.cancel();
    _onlineSub = null;
    if (_inFlight.isNotEmpty) {
      await Future.wait(_inFlight.values);
    }
  }

  void _handleOnlineChange(bool online) {
    if (_disposed) return;
    final wasOnline = _isOnline;
    _isOnline = online;
    if (!wasOnline && online) {
      // Edge: offline -> online. Replay due entries right away rather than
      // waiting for the next periodic tick.
      unawaited(_tick());
    }
  }

  void _periodicTick() {
    if (_disposed) return;
    if (!_isOnline) return;
    unawaited(_tick());
  }

  // ---------------------------------------------------------------------------
  // internals
  // ---------------------------------------------------------------------------

  Future<void> _tick() async {
    if (_disposed) return;
    final due = await _store.findDue(now: _now());
    for (final record in due) {
      if (_disposed) return;
      // Fire-and-forget per entry; _attempt guards against duplicates.
      unawaited(_attempt(record.id));
    }
  }

  Future<void> _attempt(String id) async {
    if (_disposed) return;
    if (_inFlight.containsKey(id)) return; // already attempting
    final completer = Completer<void>();
    _inFlight[id] = completer.future;

    try {
      final record = await _store.get(id);
      if (record == null) return;
      // A terminal entry only re-enters _attempt if a force-push is queued.
      if (record.status != BroadcastStatus.pending &&
          record.forcedRelays == null) {
        return;
      }

      // forcedRelays (set by rebroadcast) wins for this single attempt;
      // otherwise we target only relays still owed an ack.
      final targets = record.forcedRelays ?? record.remainingRelays;
      if (targets.isEmpty) {
        // Nothing to push. Sync terminal timestamps if we somehow got here
        // with stale nulls.
        await _store.update(id, (current) {
          final terminal = _terminalState(
            current.relays,
            current.ackedRelays,
            current.terminalErrors.keys,
          );
          if (terminal == _AttemptTerminalState.delivered &&
              current.deliveredAt == null) {
            return current.copyWith(deliveredAt: _now(), clearFailed: true);
          }
          if (terminal == _AttemptTerminalState.failed &&
              current.failedAt == null) {
            return current.copyWith(failedAt: _now());
          }
          return null;
        });
        return;
      }

      final attemptStart = _now();
      final countInaccessibleAttempts = _isOnline;
      List<RelayBroadcastResponse> results;
      String? syncError;
      try {
        final response = _broadcastFn(record.event, targets);
        results = await response.broadcastDoneFuture.timeout(
          _perAttemptTimeout,
          onTimeout: () => const [],
        );
      } catch (e) {
        syncError = e.toString();
        results = const [];
      }

      await _store.update(id, (current) {
        final newAcked = Set<String>.from(current.ackedRelays);
        final newErrors = Map<String, String>.from(current.lastErrors);
        final newTerminalErrors = Map<String, String>.from(
          current.terminalErrors,
        );
        final newInaccessibleAttempts = Map<String, int>.from(
          current.inaccessibleAttempts,
        );

        final byUrl = <String, RelayBroadcastResponse>{};
        for (final r in results) {
          byUrl[_normalizeRelay(r.relayUrl)] = r;
        }

        for (final relay in targets) {
          final r = byUrl[relay];
          final alreadyAcked = current.ackedRelays.contains(relay);
          final alreadyTerminal = current.terminalErrors.containsKey(relay);
          if (r != null && r.broadcastSuccessful) {
            newAcked.add(relay);
            newErrors.remove(relay);
            newTerminalErrors.remove(relay);
            newInaccessibleAttempts.remove(relay);
          } else if (r != null && _isTerminalOkFalse(r)) {
            if (!alreadyAcked) {
              final msg = r.msg.isEmpty ? 'rejected' : r.msg;
              newTerminalErrors[relay] = msg;
              newErrors.remove(relay);
              newInaccessibleAttempts.remove(relay);
            }
          } else if (!alreadyAcked && !alreadyTerminal) {
            // Acks are monotonic: a transient failure does not invalidate
            // historical confirmation. We only surface errors for relays
            // that have never confirmed or terminally failed.
            final msg = r == null
                ? (syncError ?? 'no response (timeout or relay unreachable)')
                : (r.msg.isEmpty ? 'rejected' : r.msg);
            if (countInaccessibleAttempts &&
                _isRelayInaccessible(r, syncError)) {
              final count = (newInaccessibleAttempts[relay] ?? 0) + 1;
              if (count >= _maxInaccessibleAttemptsPerRelay) {
                newTerminalErrors[relay] = 'max-inaccessible-attempts: $msg';
                newErrors.remove(relay);
                newInaccessibleAttempts[relay] = count;
              } else {
                newInaccessibleAttempts[relay] = count;
                newErrors[relay] = msg;
              }
            } else {
              newInaccessibleAttempts.remove(relay);
              newErrors[relay] = msg;
            }
          }
        }
        for (final ok in newAcked) {
          newErrors.remove(ok);
          newTerminalErrors.remove(ok);
          newInaccessibleAttempts.remove(ok);
        }
        for (final terminal in newTerminalErrors.keys) {
          newErrors.remove(terminal);
        }

        final terminalState = _terminalState(
          current.relays,
          newAcked,
          newTerminalErrors.keys,
        );
        final delivered = terminalState == _AttemptTerminalState.delivered;
        final failed = terminalState == _AttemptTerminalState.failed;
        final attempts = current.attempts + 1;
        final nextDelay = computeBackoff(
          attempts: attempts,
          initial: _initialBackoff,
          max: _maxBackoff,
          random: _random,
        );

        return current.copyWith(
          ackedRelays: newAcked.toList(growable: false),
          lastErrors: newErrors,
          terminalErrors: newTerminalErrors,
          inaccessibleAttempts: newInaccessibleAttempts,
          attempts: attempts,
          firstAttemptAt: current.firstAttemptAt ?? attemptStart,
          lastAttemptAt: attemptStart,
          nextAttemptAt: delivered || failed
              ? attemptStart
              : _now() + nextDelay.inMilliseconds,
          // deliveredAt is monotonic: once set it stays set (the historical
          // 100% confirmation is preserved). Only set it the first time.
          deliveredAt: delivered
              ? (current.deliveredAt ?? _now())
              : current.deliveredAt,
          failedAt: failed ? (current.failedAt ?? _now()) : current.failedAt,
          clearFailed: delivered || !failed,
          clearForcedRelays: true,
        );
      });
    } finally {
      _inFlight.remove(id);
      completer.complete();
    }
  }

  void _ensureNotDisposed() {
    if (_disposed) {
      throw StateError('OfflineBroadcast has been disposed');
    }
  }

  String _normalizeRelay(String url) {
    var u = url.trim().toLowerCase();
    while (u.endsWith('/')) {
      u = u.substring(0, u.length - 1);
    }
    return u;
  }

  List<String> _dedupNormalized(Iterable<String> relays) {
    final seen = <String>{};
    final out = <String>[];
    for (final r in relays) {
      final n = _normalizeRelay(r);
      if (n.isEmpty) continue;
      if (seen.add(n)) out.add(n);
    }
    return out;
  }

  bool _isTerminalOkFalse(RelayBroadcastResponse response) {
    if (!response.okReceived || response.broadcastSuccessful) return false;
    final prefixEnd = response.msg.indexOf(':');
    if (prefixEnd <= 0) return false;
    final prefix = response.msg.substring(0, prefixEnd).trim().toLowerCase();
    return _terminalOkFalsePrefixes.contains(prefix);
  }

  bool _isRelayInaccessible(
    RelayBroadcastResponse? response,
    String? syncError,
  ) {
    if (response == null) return syncError == null;
    return !response.okReceived;
  }

  _AttemptTerminalState _terminalState(
    List<String> relays,
    Iterable<String> ackedRelays,
    Iterable<String> terminalRelays,
  ) {
    final acked = ackedRelays.toSet();
    final terminal = terminalRelays.toSet();
    final hasTerminal = relays.any(terminal.contains);
    final allAcked = relays.every(acked.contains);
    if (allAcked) return _AttemptTerminalState.delivered;
    final allSettled = relays.every(
      (relay) => acked.contains(relay) || terminal.contains(relay),
    );
    if (allSettled && hasTerminal) return _AttemptTerminalState.failed;
    return _AttemptTerminalState.pending;
  }
}

const _terminalOkFalsePrefixes = <String>{
  'blocked',
  'error',
  'invalid',
  'pow',
  'restricted',
};

enum _AttemptTerminalState { pending, failed, delivered }
