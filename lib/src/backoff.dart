import 'dart:math';

/// Exponential backoff with full jitter.
///
/// `attempts` is the number of failed attempts so far (0-indexed before the
/// first retry). The returned delay is clamped to [initial, max].
Duration computeBackoff({
  required int attempts,
  required Duration initial,
  required Duration max,
  Random? random,
}) {
  if (attempts <= 0) return initial;
  final r = random ?? Random();
  final exp = initial.inMilliseconds.toDouble() * pow(2.0, attempts - 1);
  final capped = min(exp, max.inMilliseconds.toDouble());
  final jittered = r.nextDouble() * capped;
  final ms = jittered.clamp(initial.inMilliseconds.toDouble(), capped).toInt();
  return Duration(milliseconds: ms);
}
