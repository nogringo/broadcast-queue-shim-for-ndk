import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';
import 'package:test/test.dart';

Nip01Event _event() =>
    Nip01Event(pubKey: 'a' * 64, kind: 1, tags: const [], content: 'hi');

void main() {
  group('QueuedBroadcast', () {
    test('remainingRelays excludes acked', () {
      final r = QueuedBroadcast(
        id: 'id',
        event: _event(),
        relays: const ['wss://a', 'wss://b', 'wss://c'],
        ackedRelays: const ['wss://b'],
        lastErrors: const {},
        terminalErrors: const {'wss://c': 'pow: too low'},
        inaccessibleAttempts: const {},
        attempts: 0,
        firstAttemptAt: null,
        lastAttemptAt: null,
        nextAttemptAt: 0,
        deliveredAt: null,
        failedAt: null,
        createdAt: 0,
      );
      expect(r.remainingRelays, ['wss://a']);
    });

    test('status reflects terminal timestamps', () {
      final base = QueuedBroadcast(
        id: 'id',
        event: _event(),
        relays: const ['wss://a'],
        ackedRelays: const ['wss://a'],
        lastErrors: const {},
        terminalErrors: const {},
        inaccessibleAttempts: const {},
        attempts: 1,
        firstAttemptAt: 0,
        lastAttemptAt: 0,
        nextAttemptAt: 0,
        deliveredAt: null,
        failedAt: null,
        createdAt: 0,
      );
      expect(base.status, BroadcastStatus.pending);
      expect(base.copyWith(failedAt: 1).status, BroadcastStatus.failed);
      expect(base.copyWith(deliveredAt: 1).status, BroadcastStatus.delivered);
    });

    test('toMap → fromMap roundtrip preserves event id', () {
      final event = _event();
      final original = QueuedBroadcast(
        id: event.id,
        event: event,
        relays: const ['wss://relay.example'],
        ackedRelays: const [],
        lastErrors: const {'wss://relay.example': 'timeout'},
        terminalErrors: const {'wss://other.example': 'blocked: banned'},
        inaccessibleAttempts: const {'wss://relay.example': 2},
        attempts: 2,
        firstAttemptAt: 100,
        lastAttemptAt: 200,
        nextAttemptAt: 300,
        deliveredAt: null,
        failedAt: 400,
        createdAt: 50,
      );
      final restored = QueuedBroadcast.fromMap(original.toMap());
      expect(restored.id, original.id);
      expect(restored.event.id, event.id);
      expect(restored.relays, original.relays);
      expect(restored.lastErrors, original.lastErrors);
      expect(restored.terminalErrors, original.terminalErrors);
      expect(restored.inaccessibleAttempts, original.inaccessibleAttempts);
      expect(restored.attempts, original.attempts);
      expect(restored.failedAt, original.failedAt);
    });

    test('fromMap defaults terminal fields for older records', () {
      final event = _event();
      final restored = QueuedBroadcast.fromMap({
        'id': event.id,
        'event': Nip01EventModel.fromEntity(event).toJson(),
        'relays': const ['wss://relay.example'],
        'ackedRelays': const [],
        'lastErrors': const {},
        'attempts': 0,
        'firstAttemptAt': null,
        'lastAttemptAt': null,
        'nextAttemptAt': 0,
        'deliveredAt': null,
        'createdAt': 0,
        'forcedRelays': null,
      });
      expect(restored.terminalErrors, isEmpty);
      expect(restored.inaccessibleAttempts, isEmpty);
      expect(restored.failedAt, isNull);
    });
  });
}
