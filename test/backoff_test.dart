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

    test('does not overflow for very large attempt counts', () {
      // On native platforms int is 64-bit; 5000 * 2^51 overflows.
      final d = computeBackoff(
        attempts: 100,
        initial: const Duration(seconds: 5),
        max: const Duration(minutes: 30),
        random: Random(1),
      );
      expect(d.inMilliseconds, greaterThanOrEqualTo(5000));
      expect(d.inMilliseconds, lessThanOrEqualTo(30 * 60 * 1000));
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
