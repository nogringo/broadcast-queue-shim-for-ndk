import 'dart:async';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:broadcast_queue_shim_for_ndk/src/queue_store.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

import 'support/fake_broadcaster.dart';
import 'support/helpers.dart';

class FakeRelayLists {
  final Map<String, UrlListLookup> answers = {};
  final List<String> calls = [];
  final Map<String, List<String>> outboxRelaysSeen = {};
  Completer<void>? gate;

  void set(String pubkey, UrlListKind kind, UrlListLookup answer) =>
      answers['${kind.name}|$pubkey'] = answer;

  UrlListFn get fn => (pubkey, kind, outboxRelays) async {
    final id = '${kind.name}|$pubkey';
    calls.add(id);
    outboxRelaysSeen[id] = outboxRelays;
    await gate?.future;
    return answers[id] ?? const UrlListLookup.unavailable('offline');
  };
}

void main() {
  group('OfflineBroadcast with a relay set', () {
    late Database db;

    setUp(() async {
      db = await newDatabaseFactoryMemory().openDatabase('test.db');
    });

    tearDown(() async {
      await db.close();
    });

    OfflineBroadcast outboxFor(FakeBroadcaster fake, FakeRelayLists? lists) =>
        OfflineBroadcast(
          broadcastFn: fake.fn,
          relayListFn: lists?.fn,
          db: db,
          initialBackoff: const Duration(milliseconds: 5),
          maxBackoff: const Duration(milliseconds: 5),
        );

    test('persists unresolved without waiting for the lookup', () async {
      final fake = FakeBroadcaster()..ack('wss://w');
      final lists = FakeRelayLists()
        ..gate = Completer<void>()
        ..set('p', UrlListKind.outbox, const UrlListLookup.found(['wss://w/']));
      final outbox = outboxFor(fake, lists);

      final event = makeEvent();
      final record = await outbox.broadcast(
        event,
        relaySet: const UrlSet.outbox('p'),
      );
      expect(record.status, BroadcastStatus.pending);
      expect(record.relays, isEmpty);
      expect(record.pendingRelaySet, isA<OutboxUrls>());
      expect(fake.calls, isEmpty);

      lists.gate!.complete();
      final delivered = await waitFor(
        outbox,
        event.id,
        (r) => r.status == BroadcastStatus.delivered,
      );
      expect(delivered.relays, ['wss://w']);
      expect(delivered.pendingRelaySet, isNull);

      await outbox.dispose();
    });

    test('an unavailable lookup backs off and is retried', () async {
      final fake = FakeBroadcaster()..ack('wss://w');
      final lists = FakeRelayLists();
      final outbox = outboxFor(fake, lists);

      final event = makeEvent();
      await outbox.broadcast(event, relaySet: const UrlSet.outbox('p'));
      final waiting = await waitFor(
        outbox,
        event.id,
        (r) => r.resolutionAttempts == 1,
      );
      expect(waiting.status, BroadcastStatus.pending);
      expect(waiting.resolutionError, contains('offline'));
      expect(fake.calls, isEmpty);

      lists.set(
        'p',
        UrlListKind.outbox,
        const UrlListLookup.found(['wss://w']),
      );
      await Future.delayed(const Duration(milliseconds: 20));
      await outbox.retryNow();
      final delivered = await waitFor(
        outbox,
        event.id,
        (r) => r.status == BroadcastStatus.delivered,
      );
      expect(delivered.resolutionAttempts, 0);
      expect(delivered.resolutionError, isNull);

      await outbox.dispose();
    });

    test('freezes relays at the first successful resolution', () async {
      final fake = FakeBroadcaster()..fail('wss://w');
      final lists = FakeRelayLists()
        ..set('p', UrlListKind.outbox, const UrlListLookup.found(['wss://w']));
      final outbox = outboxFor(fake, lists);

      final event = makeEvent();
      await outbox.broadcast(event, relaySet: const UrlSet.outbox('p'));
      await waitFor(outbox, event.id, (r) => r.attempts >= 1);

      lists.set(
        'p',
        UrlListKind.outbox,
        const UrlListLookup.found(['wss://x']),
      );
      await Future.delayed(const Duration(milliseconds: 20));
      await outbox.retryNow();
      final retried = await waitFor(outbox, event.id, (r) => r.attempts >= 2);
      expect(retried.relays, ['wss://w']);
      expect(lists.calls, ['outbox|p']);

      await outbox.dispose();
    });

    test('a set resolving to no relay fails the entry', () async {
      final fake = FakeBroadcaster();
      final lists = FakeRelayLists()
        ..set('p', UrlListKind.outbox, const UrlListLookup.notFound());
      final outbox = outboxFor(fake, lists);

      final event = makeEvent();
      await outbox.broadcast(event, relaySet: const UrlSet.outbox('p'));
      final failed = await waitFor(
        outbox,
        event.id,
        (r) => r.status == BroadcastStatus.failed,
      );
      expect(failed.resolutionError, 'relay set resolved to no relay');
      expect(fake.calls, isEmpty);

      await outbox.dispose();
    });

    test('a new set on a delivered entry resolves and reopens it', () async {
      final fake = FakeBroadcaster()..ackAll(['wss://a', 'wss://b']);
      final lists = FakeRelayLists()
        ..set('p', UrlListKind.inbox, const UrlListLookup.found(['wss://b']));
      final outbox = outboxFor(fake, lists);

      final event = makeEvent();
      await outbox.broadcast(
        event,
        relaySet: const UrlSet.explicit(['wss://a']),
      );
      await waitFor(
        outbox,
        event.id,
        (r) => r.status == BroadcastStatus.delivered,
      );

      await outbox.broadcast(event, relaySet: const UrlSet.inbox('p'));
      final delivered = await waitFor(
        outbox,
        event.id,
        (r) =>
            r.status == BroadcastStatus.delivered &&
            r.ackedRelays.contains('wss://b'),
      );
      expect(delivered.relays, ['wss://a', 'wss://b']);

      await outbox.dispose();
    });

    test('an unresolved set survives a restart', () async {
      final fake = FakeBroadcaster()..ack('wss://w');
      final first = outboxFor(fake, FakeRelayLists());
      final event = makeEvent();
      await first.broadcast(event, relaySet: const UrlSet.dm('p'));
      await waitFor(first, event.id, (r) => r.resolutionAttempts >= 1);
      await first.dispose();

      final lists = FakeRelayLists()
        ..set('p', UrlListKind.outbox, const UrlListLookup.notFound())
        ..set('p', UrlListKind.dm, const UrlListLookup.found(['wss://w']));
      final second = outboxFor(fake, lists);
      await Future.delayed(const Duration(milliseconds: 20));
      await second.retryNow();
      await waitFor(
        second,
        event.id,
        (r) => r.status == BroadcastStatus.delivered,
      );

      await second.dispose();
    });

    test('dispose abandons a resolution waiting on a lookup', () async {
      final fake = FakeBroadcaster();
      final lists = FakeRelayLists()..gate = Completer<void>();
      final outbox = outboxFor(fake, lists);

      final event = makeEvent();
      await outbox.broadcast(event, relaySet: const UrlSet.private('p'));
      while (lists.calls.isEmpty) {
        await Future.delayed(const Duration(milliseconds: 5));
      }
      await outbox.dispose().timeout(const Duration(seconds: 1));

      final record = await QueueStore(
        db: db,
        storeName: 'broadcasts',
      ).get(event.id);
      expect(record!.pendingRelaySet, isA<PrivateUrls>());
      expect(record.resolutionAttempts, 0);
      lists.gate!.complete();
    });

    test('an explicit-only set resolves at enqueue time', () async {
      final fake = FakeBroadcaster()..ack('wss://b');
      final outbox = outboxFor(fake, null);

      final record = await outbox.broadcast(
        makeEvent(),
        relaySet: const UrlSet.fallback([
          UrlSet.explicit([]),
          UrlSet.explicit(['wss://b']),
        ]),
      );
      expect(record.relays, ['wss://b']);
      expect(record.pendingRelaySet, isNull);

      await outbox.dispose();
    });

    test('rejects invalid arguments', () async {
      final outbox = outboxFor(FakeBroadcaster(), null);
      final event = makeEvent();
      expect(
        () => outbox.broadcast(event, relaySet: const UrlSet.outbox('p')),
        throwsStateError,
      );
      for (final invalid in const [
        UrlSet.explicit([]),
        UrlSet.explicit(['', ' ']),
        UrlSet.fallback([UrlSet.explicit([]), UrlSet.union([])]),
        UrlSet.blossom('p'),
        UrlSet.union([
          UrlSet.explicit(['wss://a']),
          UrlSet.blossom('p'),
        ]),
      ]) {
        await expectLater(
          outbox.broadcast(event, relaySet: invalid),
          throwsArgumentError,
        );
      }
      expect(await outbox.listAll(), isEmpty);
      await outbox.dispose();
    });
  });
}
