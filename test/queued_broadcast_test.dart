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
        attempts: 0,
        firstAttemptAt: null,
        lastAttemptAt: null,
        nextAttemptAt: 0,
        deliveredAt: null,
        createdAt: 0,
      );
      expect(r.remainingRelays, ['wss://a', 'wss://c']);
    });

    test('status reflects deliveredAt', () {
      final base = QueuedBroadcast(
        id: 'id',
        event: _event(),
        relays: const ['wss://a'],
        ackedRelays: const ['wss://a'],
        lastErrors: const {},
        attempts: 1,
        firstAttemptAt: 0,
        lastAttemptAt: 0,
        nextAttemptAt: 0,
        deliveredAt: null,
        createdAt: 0,
      );
      expect(base.status, BroadcastStatus.pending);
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
        attempts: 2,
        firstAttemptAt: 100,
        lastAttemptAt: 200,
        nextAttemptAt: 300,
        deliveredAt: null,
        createdAt: 50,
      );
      final restored = QueuedBroadcast.fromMap(original.toMap());
      expect(restored.id, original.id);
      expect(restored.event.id, event.id);
      expect(restored.relays, original.relays);
      expect(restored.lastErrors, original.lastErrors);
      expect(restored.attempts, original.attempts);
    });
  });
}
