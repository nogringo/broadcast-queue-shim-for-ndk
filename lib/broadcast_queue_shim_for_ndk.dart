/// Offline-first shim around the [ndk](https://pub.dev/packages/ndk) package's
/// broadcast use case.
///
/// Use [OfflineBroadcast.withNdk] to wrap an existing `Ndk` instance, persist
/// outgoing events in a sembast database, and retry until every targeted relay
/// has acknowledged each event or returned a terminal NIP-01 rejection.
library;

export 'src/offline_broadcast.dart' show BroadcastFn, OfflineBroadcast;
export 'src/queued_broadcast.dart' show BroadcastStatus, QueuedBroadcast;
