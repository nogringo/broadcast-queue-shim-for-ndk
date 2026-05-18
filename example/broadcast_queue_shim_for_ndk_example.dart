import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast_io.dart';

Future<void> main() async {
  // 1. Open a sembast database. Use sembast_io on dart:io targets, or
  //    sembast_web for the browser. The shim works with either.
  final db = await databaseFactoryIo.openDatabase('broadcasts.db');

  // 2. Build NDK as usual. The signer is configured on NDK itself. The shim
  //    never signs.
  final ndk = Ndk(
    NdkConfig(eventVerifier: Bip340EventVerifier(), cache: MemCacheManager()),
  );

  // 3. Wrap the broadcast use case.
  final outbox = OfflineBroadcast.withNdk(ndk, db: db);
  outbox.start();

  // 4. Fire off events as you would with NDK. The list of relays is required.
  final event = Nip01Event(
    pubKey: 'deadbeef' * 8,
    kind: 1,
    tags: const [],
    content: 'hello, offline-first nostr',
  );
  await outbox.broadcast(
    event,
    relays: const ['wss://relay.damus.io', 'wss://nos.lol'],
  );

  // 5. When connectivity comes back, ask for an immediate retry pass.
  await outbox.retryNow();

  // 6. Inspect what's still pending.
  final pending = await outbox.listAll();
  for (final entry in pending) {
    print(
      'event ${entry.id} '
      'status=${entry.status.name} '
      'remaining=${entry.remainingRelays} '
      'attempts=${entry.attempts}',
    );
  }

  await outbox.dispose();
  await db.close();
}
