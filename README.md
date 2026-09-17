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
- **Relay sets resolved offline.** Target an account's NIP-65 outbox, its
  recipients' inboxes, NIP-17 DM relays or NIP-37 private relays without
  knowing the URLs yet. The
  set is stored as is and resolved by the worker once online, then frozen.
- **Account-scoped clearing.** Attribute an entry to an account with the
  optional `pubkey` argument, then wipe just that account's queue on logout via
  `clearLocalAccountData`.

## Quick start

```dart
import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart' hide RelaySet;
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
    relaySet: const RelaySet.explicit([
      'wss://relay.damus.io',
      'wss://nos.lol',
    ]),
  );

  // Or let the shim find the author's write relays later, offline included.
  await outbox.broadcast(event, relaySet: RelaySet.outbox(myPubKey));
}
```

`package:ndk/ndk.dart` also exports a `RelaySet`. In a file that imports both
packages, add `hide RelaySet` to the ndk import.

## Semantics

### `broadcast(event, {required RelaySet relaySet, String? pubkey})`

Persists `event` and schedules an immediate attempt to the relays of
`relaySet` (see [Relay sets](#relay-sets)). URLs are normalized (lowercased,
trailing `/` stripped) before storage. `broadcast` never waits on the network.
A set that needs no lookup and holds no relay throws an `ArgumentError`.

Records are keyed by the pair `(event.id, pubkey)`. If a record with the same
pair already exists, the relay lists are merged: `deliveredAt` is preserved if
every relay in the merged list is already in the entry's ack set, otherwise the
entry is demoted to pending so the missing retryable relays get pushed. A relay
set that needs a lookup is merged the same way once it resolves. The same event
queued under a different `pubkey` is a separate record.

`pubkey` is optional and defaults to `null` (unattributed). See
[Account-scoped clearing](#account-scoped-clearing) for what it buys you.

### Relay sets

A `RelaySet` describes where an event goes instead of listing URLs:

```dart
RelaySet.explicit(['wss://a', 'wss://b'])  // fixed URLs
RelaySet.outbox(pubkey)                    // NIP-65 write relays
RelaySet.nip65(pubkey)                     // every NIP-65 relay, read and write
RelaySet.inbox([pubkeys])                  // NIP-65 read relays of each pubkey
RelaySet.dm([pubkeys])                     // NIP-17 DM relays (kind 10050)
RelaySet.private(pubkey)                   // NIP-37 private relays (kind 10013)
RelaySet.union([a, b])                     // every relay of every set
RelaySet.fallback([a, b])                  // first set with at least one relay
```

Use `dm`, not `inbox`, for gift wraps. A user publishes their kind 10050 to
their NIP-65 write relays, so a `dm` lookup first resolves the pubkey's NIP-65,
then searches the kind 10050 on those write relays. If the NIP-65 is
unavailable, so is the DM list. If it is not found, the kind 10050 is still
searched on the discovery relays alone.

`private` follows the same chaining for the NIP-37 kind 10013, which must be
published on the NIP-65 write relays. Its relays are encrypted to their owner,
so `pubkey` must be an account the shim can decrypt for. With NDK, that is an
account in `ndk.accounts` whose signer supports NIP-44. While the account is
missing (for instance before the app has logged it in) or decryption fails,
the lookup is unavailable and retried.

Decryption goes through `ndk.decryptedEventPayloads`: a given kind 10013 event
is decrypted once, concurrent lookups share that decryption, and later lookups
read the plaintext from NDK's cache without the account or its signer. The
signer is asked again only when the user publishes a new list. That cache is as
persistent as the `CacheManager` given to NDK, and clearing it is up to the app
through NDK's cache API: `clearLocalAccountData` does not touch it. Once frozen,
the private relays are also stored in plain text in the queue's sembast
database.

The set is persisted with the entry, which stays `pending` with an empty
`relays` list and a non-null `pendingRelaySet`. The worker resolves it on its
next attempt and, **the first time every lookup concludes**, freezes the result
into `relays` and clears `pendingRelaySet`. Later retries push to those frozen
relays and never look the lists up again. A set made only of `explicit` sets is
resolved at enqueue time.

Each relay list lookup ends in one of three states:

- **found**: the list exists (it may hold no relay of the wanted kind).
- **not found**: at least one relay sent EOSE and none holds the list. It
  counts as an empty list.
- **unavailable**: no relay answered (offline, timeouts, disconnections,
  `CLOSED`). Whether the list exists is unknown.

If any lookup the result depends on is unavailable, nothing is frozen: the
entry records `resolutionError`, increments `resolutionAttempts` and retries
with backoff. This matters for `fallback`: an offline lookup never falls
through to the next set, so going offline cannot lock an entry onto its
fallback relays. `fallback` only moves on when a set resolves to no relay.

If the whole set resolves to no relay, the entry becomes `failed` with
`resolutionError: 'relay set resolved to no relay'`. Calling `broadcast` again
with a set queues a new resolution on the same entry.

`OfflineBroadcast.withNdk` reads kind 10002, 10050 and 10013 through
`ndk.requests.query`, NDK's cache first, so a list NDK has already seen resolves
offline. The lists are queried on `defaultIndexerRelays` (coracle, yabu.me,
purplepag.es, nos.social indexers), not on NDK's bootstrap relays, plus the
write relays for a kind 10050 or 10013. Pass `relayListDiscoveryRelays` to
query other indexers. With the default constructor, pass your own `relayListFn`; without
one, only explicit sets are accepted.

Only the relay query is timed out (`relayListQueryTimeout`, NDK's query timeout
by default). The shim never times out a lookup as a whole, so a remote signer
(NIP-46, NIP-55) can wait for user approval as long as it takes. `dispose` does
not wait for such a lookup: the resolution is abandoned without writing and
starts over on the next run.

A "not found" answer from a fast relay can arrive while a slower relay holding
the list times out.

### Account-scoped clearing

`pubkey` labels the local account an entry was queued under, so its queue can
be dropped on logout:

```dart
await outbox.broadcast(event, relaySet: ..., pubkey: myPubkey);
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
  relayListDiscoveryRelays: defaultIndexerRelays, // relays queried for relay lists
  relayListQueryTimeout: null,                   // relay list query timeout, NDK's by default
);
```

## Architecture in one diagram

```
caller.broadcast(event, relaySet)
        │
        ▼
  sembast write ──── durable, returns to caller here
        │
        ▼
  resolve pendingRelaySet (once)         ────►  retry with backoff while unavailable
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
