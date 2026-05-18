import 'dart:async';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/entities.dart' show RelayBroadcastResponse;
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

/// Records every call made to the fake broadcast function and lets the test
/// dictate what each relay should answer.
class FakeBroadcaster {
  /// Map from relay URL → result to return. Missing entries default to a
  /// no-response (timeout) by simulating an empty result list.
  final Map<String, RelayBroadcastResponse Function()> responders = {};

  /// Throws on every call until cleared. Use to simulate "NDK threw".
  Object? syncError;

  final List<({Nip01Event event, List<String> relays})> calls = [];

  BroadcastFn get fn => (event, relays) {
    calls.add((event: event, relays: List.of(relays)));
    if (syncError != null) throw syncError!;
    final responses = <RelayBroadcastResponse>[];
    for (final r in relays) {
      final responder = responders[r];
      if (responder != null) responses.add(responder());
    }
    return NdkBroadcastResponse(
      publishEvent: event,
      broadcastDoneStream: Stream.value(responses),
    );
  };

  void ackAll(List<String> relays) {
    for (final r in relays) {
      responders[r] = () => RelayBroadcastResponse(
        relayUrl: r,
        okReceived: true,
        broadcastSuccessful: true,
      );
    }
  }

  void fail(String relay, {String msg = 'connection refused'}) {
    responders[relay] = () => RelayBroadcastResponse(
      relayUrl: relay,
      okReceived: false,
      broadcastSuccessful: false,
      msg: msg,
    );
  }
}

Nip01Event _event({int seed = 1}) => Nip01Event(
  pubKey: 'a' * 64,
  kind: 1,
  tags: [
    ['t', 'seed-$seed'],
  ],
  content: 'hello $seed',
);

Future<QueuedBroadcast> _waitFor(
  OfflineBroadcast outbox,
  String id,
  bool Function(QueuedBroadcast r) predicate,
) async {
  for (var i = 0; i < 200; i++) {
    final r = await outbox.get(id);
    if (r != null && predicate(r)) return r;
    await Future.delayed(const Duration(milliseconds: 5));
  }
  throw TimeoutException('condition not met for $id');
}

void main() {
  late Database db;

  setUp(() async {
    db = await newDatabaseFactoryMemory().openDatabase('test.db');
  });

  tearDown(() async {
    await db.close();
  });

  test(
    'broadcast persists immediately and marks delivered once all ack',
    () async {
      final fake = FakeBroadcaster();
      fake.ackAll(['wss://a', 'wss://b']);
      final outbox = OfflineBroadcast(
        broadcastFn: fake.fn,
        db: db,
        initialBackoff: const Duration(milliseconds: 10),
      );

      final event = _event();
      final record = await outbox.broadcast(
        event,
        relays: const ['wss://a', 'wss://b'],
      );
      expect(record.status, BroadcastStatus.pending);

      final delivered = await _waitFor(
        outbox,
        event.id,
        (r) => r.status == BroadcastStatus.delivered,
      );
      expect(delivered.ackedRelays, unorderedEquals(['wss://a', 'wss://b']));
      expect(delivered.lastErrors, isEmpty);
      expect(fake.calls.length, 1);

      await outbox.dispose();
    },
  );

  test('partial success leaves remaining relays pending', () async {
    final fake = FakeBroadcaster();
    fake.ackAll(['wss://a']);
    fake.fail('wss://b', msg: 'pow too low');
    final outbox = OfflineBroadcast(
      broadcastFn: fake.fn,
      db: db,
      initialBackoff: const Duration(milliseconds: 5),
    );

    final event = _event();
    await outbox.broadcast(event, relays: const ['wss://a', 'wss://b']);

    final pending = await _waitFor(outbox, event.id, (r) => r.attempts >= 1);
    expect(pending.status, BroadcastStatus.pending);
    expect(pending.ackedRelays, ['wss://a']);
    expect(pending.remainingRelays, ['wss://b']);
    expect(pending.lastErrors['wss://b'], 'pow too low');

    await outbox.dispose();
  });

  test('retryNow only targets remaining relays', () async {
    final fake = FakeBroadcaster();
    fake.ackAll(['wss://a']);
    fake.fail('wss://b');
    final outbox = OfflineBroadcast(
      broadcastFn: fake.fn,
      db: db,
      initialBackoff: const Duration(milliseconds: 1),
    );

    final event = _event();
    await outbox.broadcast(event, relays: const ['wss://a', 'wss://b']);
    await _waitFor(outbox, event.id, (r) => r.attempts >= 1);

    // Now have wss://b ack.
    fake.ackAll(['wss://b']);
    await outbox.retryNow();
    final delivered = await _waitFor(
      outbox,
      event.id,
      (r) => r.status == BroadcastStatus.delivered,
    );

    expect(delivered.ackedRelays, unorderedEquals(['wss://a', 'wss://b']));
    // The retry call should have used only wss://b (the remaining relay).
    expect(fake.calls.last.relays, ['wss://b']);

    await outbox.dispose();
  });

  test(
    'rebroadcast() forces a push to every relay without losing acks or delivered status',
    () async {
      final fake = FakeBroadcaster();
      fake.ackAll(['wss://a', 'wss://b']);
      final outbox = OfflineBroadcast(
        broadcastFn: fake.fn,
        db: db,
        initialBackoff: const Duration(milliseconds: 1),
      );

      final event = _event();
      await outbox.broadcast(event, relays: const ['wss://a', 'wss://b']);
      final delivered = await _waitFor(
        outbox,
        event.id,
        (r) => r.status == BroadcastStatus.delivered,
      );
      final originalDeliveredAt = delivered.deliveredAt!;
      final attemptsBefore = delivered.attempts;

      // Simulate one relay being momentarily unreachable during the rebroadcast.
      // The historical ack must not be invalidated by this transient failure.
      fake.responders.clear();
      fake.ackAll(['wss://a']);
      fake.fail('wss://b', msg: 'temporary glitch');

      await outbox.rebroadcast(event.id);

      final after = await _waitFor(
        outbox,
        event.id,
        (r) => r.attempts > attemptsBefore,
      );
      expect(
        after.ackedRelays,
        unorderedEquals(['wss://a', 'wss://b']),
        reason: 'past acks must be monotonic',
      );
      expect(after.status, BroadcastStatus.delivered);
      expect(
        after.deliveredAt,
        originalDeliveredAt,
        reason: 'deliveredAt is a historical fact and must not move',
      );
      expect(
        after.lastErrors,
        isEmpty,
        reason: 'transient failures on already-acked relays are not surfaced',
      );
      // The force-push targeted ALL relays this time, not just remaining.
      expect(fake.calls.last.relays, unorderedEquals(['wss://a', 'wss://b']));

      await outbox.dispose();
    },
  );

  test(
    'rebroadcast(id, relay:) adds a new relay and only pushes to it',
    () async {
      final fake = FakeBroadcaster();
      fake.ackAll(['wss://a']);
      final outbox = OfflineBroadcast(
        broadcastFn: fake.fn,
        db: db,
        initialBackoff: const Duration(milliseconds: 1),
      );

      final event = _event();
      await outbox.broadcast(event, relays: const ['wss://a']);
      await _waitFor(
        outbox,
        event.id,
        (r) => r.status == BroadcastStatus.delivered,
      );

      fake.fail('wss://c');
      await outbox.rebroadcast(event.id, relay: 'wss://c');

      final withC = await _waitFor(
        outbox,
        event.id,
        (r) => r.relays.contains('wss://c') && r.attempts >= 2,
      );
      expect(withC.relays, containsAll(['wss://a', 'wss://c']));
      expect(
        withC.status,
        BroadcastStatus.pending,
        reason: 'a new unacked relay demotes the entry from delivered',
      );
      expect(withC.ackedRelays, [
        'wss://a',
      ], reason: 'the original ack stays monotonic');
      expect(withC.remainingRelays, ['wss://c']);
      // Force-push targeted only the named relay, not the whole list.
      expect(fake.calls.last.relays, ['wss://c']);

      await outbox.dispose();
    },
  );

  test(
    'rebroadcast(id, relay:) on an already-acked relay re-pushes without losing the ack',
    () async {
      final fake = FakeBroadcaster();
      fake.ackAll(['wss://a']);
      final outbox = OfflineBroadcast(
        broadcastFn: fake.fn,
        db: db,
        initialBackoff: const Duration(milliseconds: 1),
      );

      final event = _event();
      await outbox.broadcast(event, relays: const ['wss://a']);
      final delivered = await _waitFor(
        outbox,
        event.id,
        (r) => r.status == BroadcastStatus.delivered,
      );
      final attemptsBefore = delivered.attempts;
      final originalDeliveredAt = delivered.deliveredAt!;

      // Same relay, but failing now. The history must survive.
      fake.responders.clear();
      fake.fail('wss://a');
      await outbox.rebroadcast(event.id, relay: 'wss://a');

      final after = await _waitFor(
        outbox,
        event.id,
        (r) => r.attempts > attemptsBefore,
      );
      expect(after.ackedRelays, ['wss://a']);
      expect(after.status, BroadcastStatus.delivered);
      expect(after.deliveredAt, originalDeliveredAt);
      expect(after.lastErrors, isEmpty);
      expect(fake.calls.last.relays, ['wss://a']);

      await outbox.dispose();
    },
  );

  test('broadcast() throws on empty relays', () async {
    final outbox = OfflineBroadcast(broadcastFn: FakeBroadcaster().fn, db: db);
    expect(
      () => outbox.broadcast(_event(), relays: const []),
      throwsArgumentError,
    );
    await outbox.dispose();
  });

  test('duplicate broadcast() merges relays and rearms', () async {
    final fake = FakeBroadcaster();
    fake.ackAll(['wss://a']);
    fake.fail('wss://b');
    final outbox = OfflineBroadcast(
      broadcastFn: fake.fn,
      db: db,
      initialBackoff: const Duration(milliseconds: 1),
    );

    final event = _event();
    await outbox.broadcast(event, relays: const ['wss://a']);
    await _waitFor(outbox, event.id, (r) => r.attempts >= 1);

    await outbox.broadcast(event, relays: const ['wss://b']);
    final merged = await _waitFor(
      outbox,
      event.id,
      (r) => r.relays.length == 2,
    );
    expect(merged.relays, containsAll(['wss://a', 'wss://b']));

    await outbox.dispose();
  });

  test('relay URLs are normalized (case, trailing slash) on storage', () async {
    final fake = FakeBroadcaster();
    fake.ackAll(['wss://relay.example']);
    final outbox = OfflineBroadcast(
      broadcastFn: fake.fn,
      db: db,
      initialBackoff: const Duration(milliseconds: 1),
    );

    final event = _event();
    await outbox.broadcast(
      event,
      relays: const ['WSS://Relay.Example/', 'wss://relay.example'],
    );
    final r = await _waitFor(outbox, event.id, (r) => r.attempts >= 1);
    expect(r.relays, ['wss://relay.example']);

    await outbox.dispose();
  });

  test(
    'sync exception from broadcaster is recorded on remaining relays',
    () async {
      final fake = FakeBroadcaster()..syncError = StateError('no signer');
      final outbox = OfflineBroadcast(
        broadcastFn: fake.fn,
        db: db,
        initialBackoff: const Duration(milliseconds: 1),
      );

      final event = _event();
      await outbox.broadcast(event, relays: const ['wss://a']);
      final r = await _waitFor(outbox, event.id, (r) => r.attempts >= 1);
      expect(r.status, BroadcastStatus.pending);
      expect(r.lastErrors['wss://a'], contains('no signer'));

      await outbox.dispose();
    },
  );

  test('dispose() blocks further public calls', () async {
    final outbox = OfflineBroadcast(broadcastFn: FakeBroadcaster().fn, db: db);
    await outbox.dispose();
    expect(
      () => outbox.broadcast(_event(), relays: const ['wss://a']),
      throwsStateError,
    );
  });
}
