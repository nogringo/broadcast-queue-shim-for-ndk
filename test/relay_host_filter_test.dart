import 'package:broadcast_queue_shim_for_ndk/src/relay_host_filter.dart';
import 'package:test/test.dart';

void main() {
  group('isPublicRelayHost', () {
    test('accepts typical public relays', () {
      expect(isPublicRelayHost('wss://relay.damus.io'), isTrue);
      expect(isPublicRelayHost('wss://nos.lol'), isTrue);
      expect(isPublicRelayHost('ws://relay.example.com:7000/'), isTrue);
    });

    test('rejects localhost and loopback', () {
      expect(isPublicRelayHost('ws://localhost'), isFalse);
      expect(isPublicRelayHost('ws://localhost:7000'), isFalse);
      expect(isPublicRelayHost('ws://LOCALHOST/'), isFalse);
      expect(isPublicRelayHost('ws://127.0.0.1:7000'), isFalse);
      expect(isPublicRelayHost('ws://127.10.20.30'), isFalse);
    });

    test('rejects private IPv4 ranges', () {
      expect(isPublicRelayHost('ws://10.0.0.5'), isFalse);
      expect(isPublicRelayHost('ws://172.16.0.1'), isFalse);
      expect(isPublicRelayHost('ws://172.31.255.255'), isFalse);
      expect(isPublicRelayHost('ws://192.168.1.42:7000'), isFalse);
      expect(isPublicRelayHost('ws://169.254.10.20'), isFalse);
    });

    test('accepts public IPv4 outside private ranges', () {
      expect(isPublicRelayHost('ws://172.15.0.1'), isTrue);
      expect(isPublicRelayHost('ws://172.32.0.1'), isTrue);
      expect(isPublicRelayHost('ws://8.8.8.8'), isTrue);
    });

    test('rejects mDNS .local names', () {
      expect(isPublicRelayHost('ws://myserver.local'), isFalse);
      expect(isPublicRelayHost('wss://relay.local:443'), isFalse);
    });

    test('rejects IPv6 loopback and link-local', () {
      expect(isPublicRelayHost('ws://[::1]:7000'), isFalse);
      expect(isPublicRelayHost('ws://[fe80::1]:7000'), isFalse);
      expect(isPublicRelayHost('ws://[fd12:3456::1]:7000'), isFalse);
    });

    test('accepts public IPv6', () {
      expect(isPublicRelayHost('ws://[2001:4860:4860::8888]:443'), isTrue);
    });

    test('rejects malformed input', () {
      expect(isPublicRelayHost(''), isFalse);
      expect(isPublicRelayHost('   '), isFalse);
    });
  });
}
