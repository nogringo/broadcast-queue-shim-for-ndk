import 'dart:async';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/entities.dart' show RelayBroadcastResponse;
import 'package:ndk/ndk.dart';

/// Test double for [BroadcastFn]. Records every call and lets a test dictate
/// each relay's answer, throw synchronously, or park an attempt mid-flight.
class FakeBroadcaster {
  /// Map from relay URL → result to return. Missing entries default to a
  /// no-response (timeout) by simulating an empty result list.
  final Map<String, RelayBroadcastResponse Function()> responders = {};

  final List<({Nip01Event event, List<String> relays})> calls = [];

  /// Throws on every call until cleared. Use to simulate "NDK threw".
  Object? syncError;

  /// When set, every call parks on this gate instead of answering, letting a
  /// test drive an attempt that is mid-flight.
  Completer<List<RelayBroadcastResponse>>? gate;

  BroadcastFn get fn => (event, relays) {
    calls.add((event: event, relays: List.of(relays)));
    if (syncError != null) throw syncError!;
    if (gate != null) {
      return NdkBroadcastResponse(
        publishEvent: event,
        broadcastDoneStream: gate!.future.asStream(),
      );
    }
    final responses = <RelayBroadcastResponse>[];
    for (final r in relays) {
      final responder = responders[r];
      if (responder != null) responses.add(responder());
    }
    return NdkBroadcastResponse(
      publishEvent: event,
      broadcastDoneStream: Stream.value(responses),
    );
  };

  void ack(String relay) => ackAll([relay]);

  void ackAll(List<String> relays) {
    for (final r in relays) {
      responders[r] = () => RelayBroadcastResponse(
        relayUrl: r,
        okReceived: true,
        broadcastSuccessful: true,
      );
    }
  }

  void fail(String relay, {String msg = 'connection refused'}) {
    responders[relay] = () => RelayBroadcastResponse(
      relayUrl: relay,
      okReceived: false,
      broadcastSuccessful: false,
      msg: msg,
    );
  }

  void rejectOkFalse(String relay, {required String msg}) {
    responders[relay] = () => RelayBroadcastResponse(
      relayUrl: relay,
      okReceived: true,
      broadcastSuccessful: false,
      msg: msg,
    );
  }
}
