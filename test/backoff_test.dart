import 'dart:math';

import 'package:broadcast_queue_shim_for_ndk/src/backoff.dart';
import 'package:test/test.dart';

void main() {
  group('computeBackoff', () {
    final initial = const Duration(seconds: 1);
    final max = const Duration(seconds: 30);

    test('returns initial on attempts <= 0', () {
      expect(
        computeBackoff(attempts: 0, initial: initial, max: max),
        equals(initial),
      );
    });

    test('result is bounded by [initial, max]', () {
      final rng = Random(42);
      for (var i = 1; i < 20; i++) {
        final d = computeBackoff(
          attempts: i,
          initial: initial,
          max: max,
          random: rng,
        );
        expect(d.inMilliseconds, greaterThanOrEqualTo(initial.inMilliseconds));
        expect(d.inMilliseconds, lessThanOrEqualTo(max.inMilliseconds));
      }
    });
  });
}
