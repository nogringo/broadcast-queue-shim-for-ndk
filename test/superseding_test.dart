import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

import 'support/fake_broadcaster.dart';
import 'support/helpers.dart';

const _alice = 'alice';
const _bob = 'bob';
const _relays = UrlSet.explicit(['wss://a']);

Nip01Event _version({
  int kind = 10002,
  required int createdAt,
  String? d,
  String pubKey = 'a',
  String content = '',
}) => Nip01Event(
  pubKey: pubKey * 64,
  kind: kind,
  createdAt: createdAt,
  tags: [
    if (d != null) ['d', d],
    ['r', 'wss://r$createdAt'],
  ],
  content: content,
);

void main() {
  late Database db;
  late FakeBroadcaster fake;

  setUp(() async {
    db = await newDatabaseFactoryMemory().openDatabase('test.db');
    fake = FakeBroadcaster();
  });

  tearDown(() async {
    await db.close();
  });

  OfflineBroadcast newOutbox({UrlListFn? relayListFn}) => OfflineBroadcast(
    broadcastFn: fake.fn,
    relayListFn: relayListFn,
    db: db,
    tickInterval: const Duration(milliseconds: 5),
    initialBackoff: const Duration(milliseconds: 5),
    maxBackoff: const Duration(milliseconds: 5),
  )..start();

  int pushesOf(Nip01Event event) =>
      fake.calls.where((c) => c.event.id == event.id).length;

  test('a newer version stops the retries of a pending older one', () async {
    fake.fail('wss://a');
    final outbox = newOutbox();
    final v1 = _version(createdAt: 100);
    final v2 = _version(createdAt: 200);

    await outbox.broadcast(v1, relaySet: _relays);
    await waitFor(outbox, v1.id, (r) => r.attempts >= 1);
    final queued = await outbox.broadcast(v2, relaySet: _relays);
    expect(queued.status, BroadcastStatus.pending);

    final old = (await outbox.get(v1.id))!;
    expect(old.status, BroadcastStatus.superseded);
    expect(old.supersededBy, v2.id);

    await waitFor(outbox, v2.id, (r) => r.attempts >= 3);
    final pushes = pushesOf(v1);
    await waitFor(outbox, v2.id, (r) => r.attempts >= 6);
    expect(pushesOf(v1), pushes);
    expect((await outbox.watchPending().first).map((r) => r.id), [v2.id]);
    expect(await outbox.listAll(), hasLength(2));

    await outbox.dispose();
  });

  test('an older version queued after a newer one is never sent', () async {
    fake.ack('wss://a');
    final outbox = newOutbox();
    final v1 = _version(createdAt: 100);
    final v2 = _version(createdAt: 200);

    await outbox.broadcast(v2, relaySet: _relays);
    await waitFor(outbox, v2.id, (r) => r.status == BroadcastStatus.delivered);
    final queued = await outbox.broadcast(v1, relaySet: _relays);
    expect(queued.status, BroadcastStatus.superseded);
    expect(queued.supersededBy, v2.id);

    await outbox.retryNow();
    await Future.delayed(const Duration(milliseconds: 20));
    expect(pushesOf(v1), 0);
    expect((await outbox.get(v2.id))!.status, BroadcastStatus.delivered);

    await outbox.dispose();
  });

  test('a delivered older version stays delivered', () async {
    fake.ack('wss://a');
    final outbox = newOutbox();
    final v1 = _version(createdAt: 100);
    final v2 = _version(createdAt: 200);

    await outbox.broadcast(v1, relaySet: _relays);
    await waitFor(outbox, v1.id, (r) => r.status == BroadcastStatus.delivered);
    await outbox.broadcast(v2, relaySet: _relays);

    final old = (await outbox.get(v1.id))!;
    expect(old.status, BroadcastStatus.delivered);
    expect(old.supersededBy, isNull);

    await outbox.dispose();
  });

  test('an unresolved relay set of an older version is dropped', () async {
    final outbox = newOutbox(
      relayListFn: (_, _, _) async =>
          const UrlListLookup.unavailable('offline'),
    );
    final v1 = _version(createdAt: 100);
    final v2 = _version(createdAt: 200);

    await outbox.broadcast(v1, relaySet: UrlSet.outbox(v1.pubKey));
    await outbox.broadcast(v2, relaySet: UrlSet.outbox(v2.pubKey));
    var old = (await outbox.get(v1.id))!;
    expect(old.status, BroadcastStatus.superseded);
    expect(old.pendingRelaySet, isNull);

    old = await outbox.broadcast(v1, relaySet: UrlSet.outbox(v1.pubKey));
    expect(old.status, BroadcastStatus.superseded);
    expect(old.pendingRelaySet, isNull);
    expect((await outbox.get(v1.id))!.pendingRelaySet, isNull);

    await outbox.dispose();
  });

  test('addressable versions supersede each other per d tag', () async {
    fake.fail('wss://a');
    final outbox = newOutbox();
    final x1 = _version(kind: 30000, createdAt: 100, d: 'x');
    final y1 = _version(kind: 30000, createdAt: 150, d: 'y');
    final x2 = _version(kind: 30000, createdAt: 200, d: 'x');

    await outbox.broadcast(x1, relaySet: _relays);
    await outbox.broadcast(y1, relaySet: _relays);
    await outbox.broadcast(x2, relaySet: _relays);

    expect((await outbox.get(x1.id))!.supersededBy, x2.id);
    expect((await outbox.get(y1.id))!.status, BroadcastStatus.pending);
    expect((await outbox.get(x2.id))!.status, BroadcastStatus.pending);

    await outbox.dispose();
  });

  test('versions with the same created_at keep the lowest id', () async {
    fake.fail('wss://a');
    final outbox = newOutbox();
    final versions = [
      _version(createdAt: 100, content: 'one'),
      _version(createdAt: 100, content: 'two'),
    ]..sort((a, b) => a.id.compareTo(b.id));
    final [lowest, highest] = versions;

    await outbox.broadcast(lowest, relaySet: _relays);
    final queued = await outbox.broadcast(highest, relaySet: _relays);

    expect(queued.supersededBy, lowest.id);
    expect((await outbox.get(lowest.id))!.status, BroadcastStatus.pending);

    await outbox.dispose();
  });

  test('regular events and other authors are never superseded', () async {
    fake.fail('wss://a');
    final outbox = newOutbox();
    final notes = [
      _version(kind: 1, createdAt: 100),
      _version(kind: 1, createdAt: 200),
    ];
    final lists = [
      _version(createdAt: 100, pubKey: 'a'),
      _version(createdAt: 200, pubKey: 'b'),
    ];

    for (final event in [...notes, ...lists]) {
      await outbox.broadcast(event, relaySet: _relays);
    }

    for (final event in [...notes, ...lists]) {
      expect((await outbox.get(event.id))!.status, BroadcastStatus.pending);
    }

    await outbox.dispose();
  });

  test('versions supersede across accounts, the same event does not', () async {
    fake.fail('wss://a');
    final outbox = newOutbox();
    final v1 = _version(createdAt: 100);
    final v2 = _version(createdAt: 200);

    await outbox.broadcast(v1, relaySet: _relays, pubkey: _alice);
    await outbox.broadcast(v1, relaySet: _relays, pubkey: _bob);
    for (final account in [_alice, _bob]) {
      final r = (await outbox.get(v1.id, pubkey: account))!;
      expect(r.status, BroadcastStatus.pending);
    }

    await outbox.broadcast(v2, relaySet: _relays, pubkey: _bob);
    for (final account in [_alice, _bob]) {
      final r = (await outbox.get(v1.id, pubkey: account))!;
      expect(r.supersededBy, v2.id);
    }

    await outbox.dispose();
  });

  test('rebroadcast pushes a superseded record once and keeps it', () async {
    fake.fail('wss://a');
    final outbox = newOutbox();
    final v1 = _version(createdAt: 100);
    final v2 = _version(createdAt: 200);

    await outbox.broadcast(v1, relaySet: _relays);
    await waitFor(outbox, v1.id, (r) => r.attempts >= 1);
    await outbox.broadcast(v2, relaySet: _relays);
    final attempts = (await outbox.get(v1.id))!.attempts;

    await outbox.rebroadcast(v1.id);
    final pushed = await waitFor(
      outbox,
      v1.id,
      (r) => r.attempts == attempts + 1 && r.forcedRelays == null,
    );
    expect(pushed.status, BroadcastStatus.superseded);

    await waitFor(outbox, v2.id, (r) => r.attempts >= 4);
    expect((await outbox.get(v1.id))!.attempts, attempts + 1);

    await outbox.dispose();
  });
}
