import 'package:sembast/sembast.dart';

import 'queued_broadcast.dart';

/// Sembast-backed persistent store for [QueuedBroadcast] records.
///
/// All writes are serialized through sembast transactions. The store name is
/// caller-provided so multiple shims can coexist in the same database.
class QueueStore {
  final Database _db;
  final StoreRef<String, Map<String, Object?>> _store;

  QueueStore({required Database db, required String storeName})
    : _db = db,
      _store = stringMapStoreFactory.store(storeName);

  Future<QueuedBroadcast?> get(String id) async {
    final map = await _store.record(id).get(_db);
    if (map == null) return null;
    return QueuedBroadcast.fromMap(_normalize(map));
  }

  Future<void> put(QueuedBroadcast record) async {
    await _store.record(record.id).put(_db, record.toMap());
  }

  /// Atomically read-modify-write a record. The mutator runs inside a sembast
  /// transaction; returning null leaves the record unchanged.
  Future<QueuedBroadcast?> update(
    String id,
    QueuedBroadcast? Function(QueuedBroadcast current) mutate,
  ) async {
    return _db.transaction((txn) async {
      final raw = await _store.record(id).get(txn);
      if (raw == null) return null;
      final current = QueuedBroadcast.fromMap(_normalize(raw));
      final next = mutate(current);
      if (next == null) return current;
      await _store.record(id).put(txn, next.toMap());
      return next;
    });
  }

  /// Records eligible for an attempt right now: either still pending, or
  /// delivered but carrying a [QueuedBroadcast.forcedRelays] override that
  /// hasn't been consumed yet.
  Future<List<QueuedBroadcast>> findDue({required int now}) async {
    final finder = Finder(
      filter: Filter.custom((record) {
        final m = record.value as Map;
        final nextAttemptAt = m['nextAttemptAt'] as int;
        if (nextAttemptAt > now) return false;
        if (m['deliveredAt'] == null) return true;
        return m['forcedRelays'] != null;
      }),
      sortOrders: [SortOrder('nextAttemptAt')],
    );
    final records = await _store.find(_db, finder: finder);
    return records
        .map((r) => QueuedBroadcast.fromMap(_normalize(r.value)))
        .toList(growable: false);
  }

  Future<List<QueuedBroadcast>> findAll() async {
    final records = await _store.find(_db);
    return records
        .map((r) => QueuedBroadcast.fromMap(_normalize(r.value)))
        .toList(growable: false);
  }

  Stream<QueuedBroadcast?> watch(String id) {
    return _store
        .record(id)
        .onSnapshot(_db)
        .map(
          (snap) => snap == null
              ? null
              : QueuedBroadcast.fromMap(_normalize(snap.value)),
        );
  }

  Stream<List<QueuedBroadcast>> watchPending() {
    final finder = Finder(filter: Filter.equals('deliveredAt', null));
    return _store
        .query(finder: finder)
        .onSnapshots(_db)
        .map(
          (snaps) => snaps
              .map((s) => QueuedBroadcast.fromMap(_normalize(s.value)))
              .toList(growable: false),
        );
  }

  /// Sembast hands back `Map<String, Object?>`; QueuedBroadcast.fromMap expects
  /// `Map<String, dynamic>`. The cast is shallow on purpose: nested maps stay
  /// as `Map<dynamic, dynamic>` and the model handles them.
  Map<String, dynamic> _normalize(Map<String, Object?> raw) =>
      Map<String, dynamic>.from(raw);
}
