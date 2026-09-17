import 'dart:async';
import 'dart:convert';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:broadcast_queue_shim_for_ndk/src/ndk_relay_lists.dart';
import 'package:broadcast_queue_shim_for_ndk/src/queue_store.dart';
import 'package:broadcast_queue_shim_for_ndk/src/relay_set.dart';
import 'package:ndk/entities.dart' show Nip65;
import 'package:ndk/ndk.dart' hide RelaySet;
import 'package:ndk/shared/nips/nip01/bip340.dart';
import 'package:ndk/shared/nips/nip01/key_pair.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

import 'support/fake_broadcaster.dart';
import 'support/helpers.dart';

class FakeRelayLists {
  final Map<String, RelayLookup> answers = {};
  final List<String> calls = [];
  final Map<String, List<String>> outboxRelaysSeen = {};
  Completer<void>? gate;

  void set(String pubkey, RelayListKind kind, RelayLookup answer) =>
      answers['${kind.name}|$pubkey'] = answer;

  RelayListFn get fn => (pubkey, kind, outboxRelays) async {
    final id = '${kind.name}|$pubkey';
    calls.add(id);
    outboxRelaysSeen[id] = outboxRelays;
    await gate?.future;
    return answers[id] ?? const RelayLookup.unavailable('offline');
  };
}

class CountingSigner extends Bip340EventSigner {
  int decryptions = 0;

  CountingSigner(KeyPair keys)
    : super(privateKey: keys.privateKey, publicKey: keys.publicKey);

  @override
  Future<String?> decryptNip44({
    required String ciphertext,
    required String senderPubKey,
  }) {
    decryptions++;
    return super.decryptNip44(
      ciphertext: ciphertext,
      senderPubKey: senderPubKey,
    );
  }
}

List<String>? resolvedRelays(RelayResolution r) =>
    r is RelaysResolved ? r.relays : null;

void main() {
  group('RelaySet serialization', () {
    test('round-trips a nested set', () {
      const set = RelaySet.fallback([
        RelaySet.union([
          RelaySet.outbox('p1'),
          RelaySet.nip65('p5'),
          RelaySet.inbox(['p2', 'p3']),
          RelaySet.explicit(['wss://a']),
        ]),
        RelaySet.dm(['p4']),
        RelaySet.private('p6'),
      ]);
      expect(RelaySet.fromMap(set.toMap()).toMap(), set.toMap());
    });

    test('only lookups need a relay list function', () {
      expect(const RelaySet.explicit(['wss://a']).needsLookup, isFalse);
      expect(
        const RelaySet.union([
          RelaySet.explicit(['wss://a']),
          RelaySet.inbox([]),
        ]).needsLookup,
        isFalse,
      );
      expect(
        const RelaySet.fallback([
          RelaySet.explicit([]),
          RelaySet.outbox('p'),
        ]).needsLookup,
        isTrue,
      );
    });
  });

  group('resolveRelaySet', () {
    test('union merges every list and treats notFound as empty', () async {
      final lists = FakeRelayLists()
        ..set('p1', RelayListKind.outbox, const RelayLookup.found(['wss://w']))
        ..set('p2', RelayListKind.inbox, const RelayLookup.found(['wss://r']))
        ..set('p3', RelayListKind.inbox, const RelayLookup.notFound());
      final r = await resolveRelaySet(
        const RelaySet.union([
          RelaySet.outbox('p1'),
          RelaySet.inbox(['p2', 'p3']),
          RelaySet.explicit(['wss://x']),
        ]),
        lists.fn,
      );
      expect(resolvedRelays(r), ['wss://w', 'wss://r', 'wss://x']);
    });

    test('union is unavailable if any lookup is', () async {
      final lists = FakeRelayLists()
        ..set('p1', RelayListKind.outbox, const RelayLookup.found(['wss://w']));
      final r = await resolveRelaySet(
        const RelaySet.union([
          RelaySet.outbox('p1'),
          RelaySet.dm(['p2']),
        ]),
        lists.fn,
      );
      expect(r, isA<RelaysUnavailable>());
      expect((r as RelaysUnavailable).reason, contains('dm relays of p2'));
    });

    test('nip65 looks up every relay of the list', () async {
      final lists = FakeRelayLists()
        ..set(
          'p',
          RelayListKind.nip65,
          const RelayLookup.found(['wss://r', 'wss://w']),
        );
      final r = await resolveRelaySet(const RelaySet.nip65('p'), lists.fn);
      expect(resolvedRelays(r), ['wss://r', 'wss://w']);
      expect(lists.calls, ['nip65|p']);
    });

    test('looks up DM relays on the NIP-65 write relays', () async {
      final lists = FakeRelayLists()
        ..set('p', RelayListKind.outbox, const RelayLookup.found(['wss://w']))
        ..set('p', RelayListKind.dm, const RelayLookup.found(['wss://dm']));
      final r = await resolveRelaySet(const RelaySet.dm(['p']), lists.fn);
      expect(resolvedRelays(r), ['wss://dm']);
      expect(lists.calls, ['outbox|p', 'dm|p']);
      expect(lists.outboxRelaysSeen['outbox|p'], isEmpty);
      expect(lists.outboxRelaysSeen['dm|p'], ['wss://w']);
    });

    test(
      'looks up DM relays without write relays when NIP-65 is not found',
      () async {
        final lists = FakeRelayLists()
          ..set('p', RelayListKind.outbox, const RelayLookup.notFound())
          ..set('p', RelayListKind.dm, const RelayLookup.found(['wss://dm']));
        final r = await resolveRelaySet(const RelaySet.dm(['p']), lists.fn);
        expect(resolvedRelays(r), ['wss://dm']);
        expect(lists.outboxRelaysSeen['dm|p'], isEmpty);
      },
    );

    test('looks up private relays on the NIP-65 write relays', () async {
      final lists = FakeRelayLists()
        ..set('p', RelayListKind.outbox, const RelayLookup.found(['wss://w']))
        ..set('p', RelayListKind.private, const RelayLookup.found(['wss://s']));
      final r = await resolveRelaySet(const RelaySet.private('p'), lists.fn);
      expect(resolvedRelays(r), ['wss://s']);
      expect(lists.calls, ['outbox|p', 'private|p']);
      expect(lists.outboxRelaysSeen['private|p'], ['wss://w']);
    });

    test('DM relays are unavailable while NIP-65 is', () async {
      final lists = FakeRelayLists()
        ..set('p', RelayListKind.dm, const RelayLookup.found(['wss://dm']));
      final r = await resolveRelaySet(const RelaySet.dm(['p']), lists.fn);
      expect(r, isA<RelaysUnavailable>());
      expect((r as RelaysUnavailable).reason, contains('NIP-65'));
      expect(lists.calls, ['outbox|p']);
    });

    test('shares the NIP-65 lookup between outbox and DM sets', () async {
      final lists = FakeRelayLists()
        ..set('p', RelayListKind.outbox, const RelayLookup.found(['wss://w']))
        ..set('p', RelayListKind.dm, const RelayLookup.found(['wss://dm']));
      final r = await resolveRelaySet(
        const RelaySet.union([
          RelaySet.outbox('p'),
          RelaySet.dm(['p']),
        ]),
        lists.fn,
      );
      expect(resolvedRelays(r), ['wss://w', 'wss://dm']);
      expect(lists.calls, ['outbox|p', 'dm|p']);
    });

    test('fallback skips empty and notFound sets', () async {
      final lists = FakeRelayLists()
        ..set('p1', RelayListKind.outbox, const RelayLookup.notFound())
        ..set('p2', RelayListKind.outbox, const RelayLookup.found([]))
        ..set('p3', RelayListKind.outbox, const RelayLookup.found(['wss://c']));
      final r = await resolveRelaySet(
        const RelaySet.fallback([
          RelaySet.outbox('p1'),
          RelaySet.outbox('p2'),
          RelaySet.outbox('p3'),
          RelaySet.outbox('p4'),
        ]),
        lists.fn,
      );
      expect(resolvedRelays(r), ['wss://c']);
      expect(lists.calls, isNot(contains('outbox|p4')));
    });

    test(
      'fallback stops on an unavailable set instead of falling through',
      () async {
        final lists = FakeRelayLists();
        final r = await resolveRelaySet(
          const RelaySet.fallback([
            RelaySet.outbox('p1'),
            RelaySet.explicit(['wss://local']),
          ]),
          lists.fn,
        );
        expect(r, isA<RelaysUnavailable>());
      },
    );

    test('looks up each pubkey and kind once', () async {
      final lists = FakeRelayLists()
        ..set('p1', RelayListKind.outbox, const RelayLookup.found(['wss://a']))
        ..set('p1', RelayListKind.inbox, const RelayLookup.found(['wss://b']));
      await resolveRelaySet(
        const RelaySet.union([
          RelaySet.outbox('p1'),
          RelaySet.outbox('p1'),
          RelaySet.inbox(['p1', 'p1']),
        ]),
        lists.fn,
      );
      expect(lists.calls, ['outbox|p1', 'inbox|p1']);
    });
  });

  group('relaysOfList', () {
    test('splits NIP-65 markers into outbox and inbox', () {
      final event = Nip01Event(
        pubKey: 'a' * 64,
        kind: Nip65.kKind,
        tags: [
          ['r', 'wss://both'],
          ['r', 'wss://read', 'read'],
          ['r', 'wss://write', 'write'],
        ],
        content: '',
      );
      expect(
        relaysOfList(event, RelayListKind.outbox),
        unorderedEquals(['wss://both', 'wss://write']),
      );
      expect(
        relaysOfList(event, RelayListKind.inbox),
        unorderedEquals(['wss://both', 'wss://read']),
      );
      expect(
        relaysOfList(event, RelayListKind.nip65),
        unorderedEquals(['wss://both', 'wss://read', 'wss://write']),
      );
    });

    test('reads relay tags of a kind 10050', () {
      final event = Nip01Event(
        pubKey: 'a' * 64,
        kind: Nip51List.kDmRelays,
        tags: [
          ['relay', 'wss://dm1'],
          ['r', 'wss://ignored'],
          ['relay', 'wss://dm2'],
        ],
        content: '',
      );
      expect(relaysOfList(event, RelayListKind.dm), ['wss://dm1', 'wss://dm2']);
    });
  });

  group('relaysOfPrivateTags', () {
    test('keeps relay tags only', () {
      expect(
        relaysOfPrivateTags(
          jsonEncode([
            ['relay', 'wss://s1'],
            ['p', 'abc'],
            ['relay'],
            ['relay', 'wss://s2'],
          ]),
        ),
        ['wss://s1', 'wss://s2'],
      );
    });

    test('yields no relay for content that is not a tag list', () {
      expect(relaysOfPrivateTags('not json'), isEmpty);
      expect(relaysOfPrivateTags('{"relay": "wss://s"}'), isEmpty);
    });
  });

  group('ndkRelayListFn private relays', () {
    late Ndk ndk;
    late KeyPair keys;

    setUp(() async {
      keys = Bip340.generatePrivateKey();
      final cache = MemCacheManager();
      ndk = Ndk(
        NdkConfig(
          eventVerifier: Bip340EventVerifier(),
          cache: cache,
          bootstrapRelays: const ['ws://127.0.0.1:9'],
        ),
      );
      final signer = Bip340EventSigner(
        privateKey: keys.privateKey,
        publicKey: keys.publicKey,
      );
      final content = await signer.encryptNip44(
        plaintext: jsonEncode([
          ['relay', 'wss://private.example'],
        ]),
        recipientPubKey: keys.publicKey,
      );
      await cache.saveEvent(
        await signer.sign(
          Nip01Event(
            pubKey: keys.publicKey,
            kind: 10013,
            tags: const [],
            content: content!,
          ),
        ),
      );
    });

    tearDown(() async {
      await ndk.destroy();
    });

    RelayListFn lookupFn() => ndkRelayListFn(
      ndk,
      discoveryRelays: const ['ws://127.0.0.1:9'],
      queryTimeout: const Duration(milliseconds: 200),
    );

    test('decrypts the list with the account signer', () async {
      ndk.accounts.loginPrivateKey(
        pubkey: keys.publicKey,
        privkey: keys.privateKey!,
      );
      final lookup = await lookupFn()(
        keys.publicKey,
        RelayListKind.private,
        const [],
      );
      expect(lookup, isA<RelayListFound>());
      expect((lookup as RelayListFound).relays, ['wss://private.example']);
    });

    test('asks the signer once for the same list event', () async {
      final signer = CountingSigner(keys);
      ndk.accounts.loginExternalSigner(signer: signer);

      final lookups = await Future.wait([
        lookupFn()(keys.publicKey, RelayListKind.private, const []),
        lookupFn()(keys.publicKey, RelayListKind.private, const []),
      ]);
      ndk.accounts.removeAccount(pubkey: keys.publicKey);
      final afterRemoval = await lookupFn()(
        keys.publicKey,
        RelayListKind.private,
        const [],
      );

      for (final lookup in [...lookups, afterRemoval]) {
        expect((lookup as RelayListFound).relays, ['wss://private.example']);
      }
      expect(signer.decryptions, 1);
    });

    test('is unavailable while the account is not in NDK', () async {
      final lookup = await lookupFn()(
        keys.publicKey,
        RelayListKind.private,
        const [],
      );
      expect(lookup, isA<RelayListUnavailable>());
    });
  });

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
        ..set('p', RelayListKind.outbox, const RelayLookup.found(['wss://w/']));
      final outbox = outboxFor(fake, lists);

      final event = makeEvent();
      final record = await outbox.broadcast(
        event,
        relaySet: const RelaySet.outbox('p'),
      );
      expect(record.status, BroadcastStatus.pending);
      expect(record.relays, isEmpty);
      expect(record.pendingRelaySet, isA<OutboxRelays>());
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
      await outbox.broadcast(event, relaySet: const RelaySet.outbox('p'));
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
        RelayListKind.outbox,
        const RelayLookup.found(['wss://w']),
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
        ..set('p', RelayListKind.outbox, const RelayLookup.found(['wss://w']));
      final outbox = outboxFor(fake, lists);

      final event = makeEvent();
      await outbox.broadcast(event, relaySet: const RelaySet.outbox('p'));
      await waitFor(outbox, event.id, (r) => r.attempts >= 1);

      lists.set(
        'p',
        RelayListKind.outbox,
        const RelayLookup.found(['wss://x']),
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
        ..set('p', RelayListKind.outbox, const RelayLookup.notFound());
      final outbox = outboxFor(fake, lists);

      final event = makeEvent();
      await outbox.broadcast(event, relaySet: const RelaySet.outbox('p'));
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
        ..set('p', RelayListKind.inbox, const RelayLookup.found(['wss://b']));
      final outbox = outboxFor(fake, lists);

      final event = makeEvent();
      await outbox.broadcast(
        event,
        relaySet: const RelaySet.explicit(['wss://a']),
      );
      await waitFor(
        outbox,
        event.id,
        (r) => r.status == BroadcastStatus.delivered,
      );

      await outbox.broadcast(event, relaySet: const RelaySet.inbox(['p']));
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
      await first.broadcast(event, relaySet: const RelaySet.dm(['p']));
      await waitFor(first, event.id, (r) => r.resolutionAttempts >= 1);
      await first.dispose();

      final lists = FakeRelayLists()
        ..set('p', RelayListKind.outbox, const RelayLookup.notFound())
        ..set('p', RelayListKind.dm, const RelayLookup.found(['wss://w']));
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
      await outbox.broadcast(event, relaySet: const RelaySet.private('p'));
      while (lists.calls.isEmpty) {
        await Future.delayed(const Duration(milliseconds: 5));
      }
      await outbox.dispose().timeout(const Duration(seconds: 1));

      final record = await QueueStore(
        db: db,
        storeName: 'broadcasts',
      ).get(event.id);
      expect(record!.pendingRelaySet, isA<PrivateRelays>());
      expect(record.resolutionAttempts, 0);
      lists.gate!.complete();
    });

    test('an explicit-only set resolves at enqueue time', () async {
      final fake = FakeBroadcaster()..ack('wss://b');
      final outbox = outboxFor(fake, null);

      final record = await outbox.broadcast(
        makeEvent(),
        relaySet: const RelaySet.fallback([
          RelaySet.explicit([]),
          RelaySet.explicit(['wss://b']),
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
        () => outbox.broadcast(event, relaySet: const RelaySet.outbox('p')),
        throwsStateError,
      );
      for (final empty in const [
        RelaySet.explicit([]),
        RelaySet.explicit(['', ' ']),
        RelaySet.fallback([RelaySet.explicit([]), RelaySet.inbox([])]),
      ]) {
        await expectLater(
          outbox.broadcast(event, relaySet: empty),
          throwsArgumentError,
        );
      }
      expect(await outbox.listAll(), isEmpty);
      await outbox.dispose();
    });
  });
}
