import 'dart:async';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';

/// A deterministic kind-1 event. [seed] varies the tags and content so distinct
/// seeds yield distinct event ids; [pubKey] is the repeated author-key char.
Nip01Event makeEvent({String pubKey = 'a', int seed = 1}) => Nip01Event(
  pubKey: pubKey * 64,
  kind: 1,
  tags: [
    ['t', 'seed-$seed'],
  ],
  content: 'hello $seed',
);

/// Polls until [predicate] holds for the record at `(id, pubkey)`, or throws.
Future<QueuedBroadcast> waitFor(
  OfflineBroadcast outbox,
  String id,
  bool Function(QueuedBroadcast r) predicate, {
  String? pubkey,
}) async {
  for (var i = 0; i < 400; i++) {
    final r = await outbox.get(id, pubkey: pubkey);
    if (r != null && predicate(r)) return r;
    await Future.delayed(const Duration(milliseconds: 5));
  }
  throw TimeoutException('condition not met for $id');
}
