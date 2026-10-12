import 'dart:async';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/entities.dart' show RelayBroadcastResponse;
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

import 'support/fake_broadcaster.dart';
import 'support/helpers.dart';

const _alice = 'alice';
const _bob = 'bob';
const _relays = RelaySet.explicit(['wss://a']);

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

  OfflineBroadcast newOutbox({RelayListFn? relayListFn}) => OfflineBroadcast(
    broadcastFn: fake.fn,
    relayListFn: relayListFn,
    db: db,
    tickInterval: const Duration(milliseconds: 5),
    initialBackoff: const Duration(milliseconds: 5),
    maxBackoff: const Duration(milliseconds: 5),
  )..start();

  Future<void> settle() => Future.delayed(const Duration(milliseconds: 30));

  test('cancel stops the retries of a pending record', () async {
    fake.fail('wss://a');
    final outbox = newOutbox();
    final event = makeEvent();

    await outbox.broadcast(event, relaySet: _relays);
    await waitFor(outbox, event.id, (r) => r.attempts >= 1);
    final cancelled = (await outbox.cancel(event.id))!;
    expect(cancelled.status, BroadcastStatus.cancelled);
    expect(cancelled.cancelledAt, isNotNull);

    await settle();
    final pushes = fake.calls.length;
    await outbox.retryNow();
    await settle();
    expect(fake.calls.length, pushes);
    expect(await outbox.watchPending().first, isEmpty);
    expect(await outbox.listAll(), hasLength(1));

    await outbox.dispose();
  });

  test('cancel drops an unresolved relay set', () async {
    var lookups = 0;
    final outbox = newOutbox(
      relayListFn: (_, _, _) async {
        lookups++;
        return const RelayLookup.unavailable('offline');
      },
    );
    final event = makeEvent();

    await outbox.broadcast(event, relaySet: RelaySet.outbox(event.pubKey));
    final cancelled = (await outbox.cancel(event.id))!;
    expect(cancelled.status, BroadcastStatus.cancelled);
    expect(cancelled.pendingRelaySet, isNull);

    await settle();
    final seen = lookups;
    await outbox.retryNow();
    await settle();
    expect(lookups, seen);
    expect(fake.calls, isEmpty);

    await outbox.dispose();
  });

  test('an attempt in flight completes and keeps its acks', () async {
    fake.gate = Completer<List<RelayBroadcastResponse>>();
    final outbox = newOutbox();
    final event = makeEvent();

    await outbox.broadcast(event, relaySet: _relays);
    await waitFor(outbox, event.id, (_) => fake.calls.isNotEmpty);
    await outbox.cancel(event.id);
    fake.gate!.complete([
      RelayBroadcastResponse(
        relayUrl: 'wss://a',
        okReceived: true,
        broadcastSuccessful: true,
      ),
    ]);

    final record = await waitFor(outbox, event.id, (r) => r.attempts == 1);
    expect(record.status, BroadcastStatus.cancelled);
    expect(record.ackedRelays, ['wss://a']);
    expect(record.deliveredAt, isNotNull);

    await outbox.dispose();
  });

  test('a settled record keeps its status', () async {
    fake.ack('wss://a');
    final outbox = newOutbox();
    final event = makeEvent();

    await outbox.broadcast(event, relaySet: _relays);
    await waitFor(outbox, event.id, (r) => r.deliveredAt != null);
    final record = (await outbox.cancel(event.id))!;
    expect(record.status, BroadcastStatus.delivered);
    expect(record.cancelledAt, isNull);

    await outbox.dispose();
  });

  test('broadcast reopens a cancelled record', () async {
    fake.fail('wss://a');
    final outbox = newOutbox();
    final event = makeEvent();

    await outbox.broadcast(event, relaySet: _relays);
    await outbox.cancel(event.id);
    fake.ack('wss://a');
    final reopened = await outbox.broadcast(event, relaySet: _relays);
    expect(reopened.status, BroadcastStatus.pending);

    final record = await waitFor(
      outbox,
      event.id,
      (r) => r.status == BroadcastStatus.delivered,
    );
    expect(record.cancelledAt, isNull);

    await outbox.dispose();
  });

  test('rebroadcast pushes a cancelled record once and keeps it', () async {
    fake.fail('wss://a');
    final outbox = newOutbox();
    final event = makeEvent();

    await outbox.broadcast(event, relaySet: _relays);
    await waitFor(outbox, event.id, (r) => r.attempts >= 1);
    await outbox.cancel(event.id);
    await settle();
    final attempts = (await outbox.get(event.id))!.attempts;

    await outbox.rebroadcast(event.id);
    final pushed = await waitFor(
      outbox,
      event.id,
      (r) => r.attempts == attempts + 1 && r.forcedRelays == null,
    );
    expect(pushed.status, BroadcastStatus.cancelled);

    await settle();
    expect((await outbox.get(event.id))!.attempts, attempts + 1);

    await outbox.dispose();
  });

  test('cancel selects the record by pubkey', () async {
    fake.fail('wss://a');
    final outbox = newOutbox();
    final event = makeEvent();

    await outbox.broadcast(event, relaySet: _relays, pubkey: _alice);
    await outbox.broadcast(event, relaySet: _relays, pubkey: _bob);
    await outbox.cancel(event.id, pubkey: _alice);

    final alice = (await outbox.get(event.id, pubkey: _alice))!;
    final bob = (await outbox.get(event.id, pubkey: _bob))!;
    expect(alice.status, BroadcastStatus.cancelled);
    expect(bob.status, BroadcastStatus.pending);
    expect(await outbox.cancel(event.id), isNull);

    await outbox.dispose();
  });
}
