## 0.1.0

Initial release.

- `OfflineBroadcast` wraps `ndk.broadcast` and persists every event to a
  caller-provided sembast database before attempting to send it.
- Required `relays` list at broadcast time. No gossip-based auto-selection.
- Per-relay acks are tracked monotonically. An entry is considered
  `delivered` only when every targeted relay has acknowledged the event with
  `broadcastSuccessful: true`; once set, the delivery timestamp is never
  cleared by the shim.
- Records are kept after delivery. They are never auto-deleted.
- `rebroadcast(id)` queues a one-shot push to every relay without touching
  the historical ack set.
- `rebroadcast(id, relay: ...)` queues a one-shot push to a specific relay,
  adding it to the entry's target list if absent.
- Retries are driven by an internal periodic timer started via `start()`,
  plus a public `retryNow()` callers can wire to connectivity signals.
- Backoff is exponential with full jitter, bounded by `initialBackoff` and
  `maxBackoff`.
- The shim never signs events. Signing remains NDK's responsibility.
