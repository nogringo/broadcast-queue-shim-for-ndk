import 'dart:async';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/entities.dart' show RelayBroadcastResponse;
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

/// Fake broadcaster that fails every call (so entries stay pending and get
/// rescheduled by the worker). Useful to count how many times the worker
/// actually tries.
class FailingCounter {
  int calls = 0;
  BroadcastFn get fn => (event, relays) {
    calls++;
    return NdkBroadcastResponse(
      publishEvent: event,
      broadcastDoneStream: Stream.value([
        for (final r in relays)
          RelayBroadcastResponse(
            relayUrl: r,
            okReceived: false,
            broadcastSuccessful: false,
            msg: 'offline test',
          ),
      ]),
    );
  };
}

Nip01Event _event() =>
    Nip01Event(pubKey: 'a' * 64, kind: 1, tags: const [], content: 'hi');

/// Returns once one microtask + one event-loop tick has elapsed, which is
/// enough for a stream listener attached this turn to receive a pending
/// emission.
Future<void> _flush() async {
  await Future<void>.delayed(Duration.zero);
}

void main() {
  late Database db;

  setUp(() async {
    db = await newDatabaseFactoryMemory().openDatabase('test.db');
  });

  tearDown(() async {
    await db.close();
  });

  test('periodic tick is a no-op while offline', () async {
    final fake = FailingCounter();
    final signal = StreamController<bool>();
    final outbox = OfflineBroadcast(
      broadcastFn: fake.fn,
      db: db,
      tickInterval: const Duration(milliseconds: 20),
      initialBackoff: const Duration(milliseconds: 5),
      onlineSignal: signal.stream,
    );

    // Seed the queue with a failing entry (so it stays pending and would
    // normally be picked up by every tick).
    await outbox.broadcast(_event(), relays: const ['wss://a']);
    outbox.start();
    signal.add(false);
    // Let broadcast()'s unconditional attempt + start()'s initial replay
    // land, plus the signal listener flip _isOnline to false.
    await Future<void>.delayed(const Duration(milliseconds: 10));
    final base = fake.calls;

    // Run through several tick intervals while offline.
    await Future<void>.delayed(const Duration(milliseconds: 120));
    expect(
      fake.calls,
      base,
      reason: 'no periodic attempt should fire while offline',
    );

    await outbox.dispose();
    await signal.close();
  });

  test('offline -> online edge triggers an immediate replay', () async {
    final fake = FailingCounter();
    final signal = StreamController<bool>();
    final outbox = OfflineBroadcast(
      broadcastFn: fake.fn,
      db: db,
      // Long interval so we know any replay came from the edge, not the timer.
      tickInterval: const Duration(seconds: 30),
      initialBackoff: const Duration(milliseconds: 5),
      onlineSignal: signal.stream,
    );

    await outbox.broadcast(_event(), relays: const ['wss://a']);
    await _flush();
    final base = fake.calls;
    expect(base, 1);

    outbox.start();
    signal.add(false);
    await _flush();

    // Wait long enough for backoff to expire — if anything were going to fire
    // from the timer it would have, but the timer is 30s away.
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(fake.calls, base, reason: 'still offline, no new attempt');

    signal.add(true);
    await _flush();
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(
      fake.calls,
      greaterThan(base),
      reason: 'flipping online must trigger an immediate replay',
    );

    await outbox.dispose();
    await signal.close();
  });

  test('retryNow() bypasses the online state', () async {
    final fake = FailingCounter();
    final signal = StreamController<bool>();
    final outbox = OfflineBroadcast(
      broadcastFn: fake.fn,
      db: db,
      tickInterval: const Duration(seconds: 30),
      initialBackoff: const Duration(milliseconds: 5),
      onlineSignal: signal.stream,
    );

    await outbox.broadcast(_event(), relays: const ['wss://a']);
    outbox.start();
    signal.add(false);
    // Let the initial attempt + start replay land and the entry's backoff
    // (5ms) elapse so it becomes due again.
    await Future<void>.delayed(const Duration(milliseconds: 30));
    final base = fake.calls;

    await outbox.retryNow();
    await _flush();
    expect(
      fake.calls,
      greaterThan(base),
      reason: 'retryNow() must run even while offline',
    );

    await outbox.dispose();
    await signal.close();
  });
}
