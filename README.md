# broadcast_queue_shim_for_ndk

Offline-first wrapper around the [`ndk`](https://pub.dev/packages/ndk) package's
broadcast use case.

NDK's `broadcast` sends a Nostr event to a set of relays and reports per-relay
results. If every relay is unreachable (flaky network, app backgrounded,
process killed), the event is gone from the caller's perspective. This shim
sits in front of `ndk.broadcast` and adds:

- **Local persistence first.** The event is committed to a sembast store
  before any network attempt. `broadcast()` returns once persistence is durable;
  delivery happens in the background and survives restarts.
- **100 % delivery guarantee.** An entry is only marked `delivered` once
  *every* targeted relay has returned `broadcastSuccessful: true`. Partial
  success keeps the entry pending and retries the missing retryable relays.
- **Monotonic ack history.** A relay that has acked never un-acks. A delivered
  entry never silently flips back to pending due to a transient relay outage.
- **Terminal relay rejections.** NIP-01 `OK false` replies with `pow`,
  `blocked`, `invalid`, `restricted`, or `error` stop retries for that
  relay/event pair and are exposed through `BroadcastStatus.failed`.
- **Dead relay cutoff.** A relay that stays inaccessible for 12 consecutive
  online attempts is also stopped for that event.
- **No auto-deletion.** Delivered entries stay in the store and can be
  re-broadcast later, for instance to a freshly discovered relay.
- **Account-scoped clearing.** Attribute an entry to an account with the
  optional `pubkey` argument, then wipe just that account's queue on logout via
  `clearLocalAccountData`.

## Quick start

```dart
import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast_io.dart';

Future<void> main() async {
  final db = await databaseFactoryIo.openDatabase('broadcasts.db');

  final ndk = Ndk(
    NdkConfig(
      eventVerifier: Bip340EventVerifier(),
      cache: MemCacheManager(),
    ),
  );

  final outbox = OfflineBroadcast.withNdk(ndk, db: db);
  outbox.start();

  final event = Nip01Event(
    pubKey: myPubKey,
    kind: 1,
    tags: const [],
    content: 'hello from a flaky network',
  );

  // Returns as soon as the event is persisted. Delivery is now the shim's
  // responsibility.
  await outbox.broadcast(
    event,
    relays: const ['wss://relay.damus.io', 'wss://nos.lol'],
  );
}
```

## Semantics

### `broadcast(event, relays: [...], {String? pubkey})`

Persists `event` and schedules an immediate attempt to push it to every URL in
`relays`. The list is **required**: gossip-based relay selection is never
used. URLs are normalized (lowercased, trailing `/` stripped) before storage.

Records are keyed by the pair `(event.id, pubkey)`. If a record with the same
pair already exists, the relay lists are merged: `deliveredAt` is preserved if
every relay in the merged list is already in the entry's ack set, otherwise the
entry is demoted to pending so the missing retryable relays get pushed. The
same event queued under a different `pubkey` is a separate record.

`pubkey` is optional and defaults to `null` (unattributed). See
[Account-scoped clearing](#account-scoped-clearing) for what it buys you.

### Account-scoped clearing

`pubkey` labels the local account an entry was queued under, so its queue can
be dropped on logout:

```dart
await outbox.broadcast(event, relays: [...], pubkey: myPubkey);
...
await outbox.clearLocalAccountData(pubkey: myPubkey); // wipe this account
await outbox.clearAllLocalData();                     // wipe everything
```

- It is a plain label, **not** `event.pubKey`. An event may be queued under an
  account that did not sign it (rebroadcasting someone else's note), and for a
  gift wrap (kind 1059) `event.pubKey` is an ephemeral throwaway key. Pass the
  real sending account so NIP-17 DMs and other wraps can still be cleared.
- Leaving `pubkey` `null` leaves the entry **unattributed**: retried like any
  other, keyed by the bare `event.id`, but never removed by
  `clearLocalAccountData`. Entries written before 0.4.0 are unattributed.
- `clearLocalAccountData` removes matching entries for good, delivered or not;
  pending ones will never be retried again. It is safe to call while attempts
  are in flight (a concurrent attempt no-ops once its record is gone), but do
  not `broadcast` for an account you are clearing, as an overlapping enqueue can
  re-create its record.
- `get`, `watch`, and `rebroadcast` take the same optional `pubkey` to select
  which record they act on. `watchPending` and `listAll` span all accounts;
  filter on `QueuedBroadcast.pubkey` for one.
- `clearAllLocalData` empties the shim's store only; other sembast stores in the
  same database are untouched.

### `retryNow()`

Forces an immediate scan of due entries, bypassing the online check. Use it
as an explicit override (e.g. when the user pulls to refresh).

### Connectivity awareness

`OfflineBroadcast.withNdk()` subscribes to
`ndk.connectivity.relayConnectivityChanges` and pauses the periodic retry
timer while no public relay is connected. As soon as a public relay comes
online, the shim replays everything that's due. Loopback addresses, RFC1918
IPv4, ULA/link-local IPv6, and mDNS `.local` names are excluded from the
"is online" computation so a local dev relay cannot mask a real outage.

For non-NDK setups, pass any `Stream<bool> onlineSignal` to the default
constructor:

```dart
OfflineBroadcast(
  broadcastFn: ...,
  db: db,
  onlineSignal: yourConnectivityStream, // true while online, false otherwise
);
```

If you don't pass anything, the shim assumes it is always online and the
periodic timer runs unconditionally (pre-0.2 behavior).

### `rebroadcast(id, {String? pubkey, String? relay})`

`ackedRelays` and `deliveredAt` are monotonic. `rebroadcast` never rewrites
the past; it queues a one-shot push via a transient `forcedRelays` override
that the next attempt consumes. `pubkey` selects which record to act on, the
same value passed to `broadcast`; it returns `null` if no such record exists.

- `rebroadcast(id)`: schedules an immediate push to **every** relay in the
  entry's `relays` list, including those that already acked. Useful when you
  suspect a relay dropped your event. Acks and `deliveredAt` are preserved
  regardless of the new attempt's outcome.
- `rebroadcast(id, relay: r)`: pushes to that single relay. If `r` is new
  to the entry, it joins the target list and the entry is demoted to pending
  until `r` acks. If `r` was already there, the historical state is
  preserved.

### What "success" means

The full target set must ack. NDK's own `considerDonePercent` knob is *not*
used as a delivery threshold; it only governs when the underlying future
completes, which is a different question.

### Terminal failures

NIP-01 requires failed `OK` messages to start with a machine-readable prefix
followed by `:`. When a relay returns `OK false` with one of these prefixes,
the shim records the message in `QueuedBroadcast.terminalErrors` and stops
retrying that relay for the event:

- `blocked`
- `error`
- `invalid`
- `pow`
- `restricted`

If every target relay is either acked or terminally rejected, and at least one
relay is terminally rejected, the entry status becomes `BroadcastStatus.failed`
and it leaves `watchPending()`. Non-terminal failures, including
`rate-limited`, unknown prefixes, missing colons, and global broadcaster
exceptions remain retryable. Timeouts, no response, and transport-level relay
failures remain retryable until the relay has been inaccessible for 12
consecutive online attempts.

Manual `rebroadcast(...)` can still force another push. If a previously
terminally rejected relay later succeeds, its terminal error is cleared and the
entry can become `delivered`.

### What the shim does NOT do

- **It never signs.** Whatever event you pass is forwarded as-is to
  `ndk.broadcast.broadcast`. If the event is unsigned, NDK signs it using its
  configured `EventSigner`. The shim has no opinion on signing.
- **It never auto-deletes records.** Even after full delivery, an entry stays
  in the database until you remove it, via `clearLocalAccountData` /
  `clearAllLocalData` or by clearing sembast records directly.
- **It does not apply a max-attempts limit.** Retryable failures continue with
  exponential backoff until they ack, are manually rebroadcast, become a
  terminal NIP-01 rejection, or hit the inaccessible-relay cutoff.

## Tuning

```dart
OfflineBroadcast.withNdk(
  ndk,
  db: db,
  storeName: 'broadcasts',                       // sembast store name
  tickInterval: const Duration(seconds: 30),     // periodic retry scan
  initialBackoff: const Duration(seconds: 5),    // backoff floor
  maxBackoff: const Duration(minutes: 30),       // backoff ceiling
  perAttemptTimeout: const Duration(seconds: 10),// gives up on a single NDK call after this
  maxInaccessibleAttemptsPerRelay: 12,           // stops dead relays while online
);
```

## Architecture in one diagram

```
caller.broadcast(event, relays)
        │
        ▼
  sembast write ──── durable, returns to caller here
        │
        ▼
  ndk.broadcast.broadcast(event, specificRelays: remaining)
        │
        ▼
  await broadcastDoneFuture
        │
        ▼
  per-relay union into ackedRelays       ────►  delivered when ⊇ relays
  terminal OK false into terminalErrors  ────►  failed when all remaining are terminal
  inaccessible relay attempts            ────►  terminal after 12 online failures
  retryable error into lastErrors              (otherwise schedule backoff)
```

A `Timer.periodic` scans `findDue` every `tickInterval` and replays whatever
is overdue. `retryNow()` runs the same scan immediately.

## Testing your integration

`OfflineBroadcast` is fully unit-testable without NDK. Pass a custom
`BroadcastFn` to the default constructor:

```dart
final outbox = OfflineBroadcast(
  broadcastFn: (event, relays) => NdkBroadcastResponse(
    publishEvent: event,
    broadcastDoneStream: Stream.value([
      for (final r in relays)
        RelayBroadcastResponse(
          relayUrl: r,
          okReceived: true,
          broadcastSuccessful: true,
        ),
    ]),
  ),
  db: await newDatabaseFactoryMemory().openDatabase('test.db'),
);
```

The package's own test suite uses exactly this approach; see
[`test/offline_broadcast_test.dart`](https://github.com/nogringo/broadcast-queue-shim-for-ndk/blob/main/test/offline_broadcast_test.dart).

## License

MIT
