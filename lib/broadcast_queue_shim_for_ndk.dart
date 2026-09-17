/// Offline-first shim around the [ndk](https://pub.dev/packages/ndk) package's
/// broadcast use case.
///
/// Use [OfflineBroadcast.withNdk] to wrap an existing `Ndk` instance, persist
/// outgoing events in a sembast database, and retry until every targeted relay
/// has acknowledged each event or returned a terminal NIP-01 rejection. Target
/// relays can be given as a [RelaySet] resolved later, offline included.
library;

export 'src/offline_broadcast.dart' show BroadcastFn, OfflineBroadcast;
export 'src/indexer_relays.dart' show defaultIndexerRelays;
export 'src/ndk_relay_lists.dart' show ndkRelayListFn;
export 'src/queued_broadcast.dart' show BroadcastStatus, QueuedBroadcast;
export 'src/relay_set.dart'
    show
        DmRelays,
        ExplicitRelays,
        FallbackRelays,
        InboxRelays,
        Nip65Relays,
        OutboxRelays,
        PrivateRelays,
        RelayListFn,
        RelayListFound,
        RelayListKind,
        RelayListNotFound,
        RelayListUnavailable,
        RelayLookup,
        RelaySet,
        UnionRelays;
