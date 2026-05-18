/// Returns `true` when the host portion of [url] is a public-internet
/// address, i.e. not loopback, not private IPv4, not private/link-local
/// IPv6, and not an mDNS `.local` name.
///
/// Used by the connectivity layer to decide whether a connected relay is
/// evidence that the device can reach the internet at large.
bool isPublicRelayHost(String url) {
  final uri = Uri.tryParse(url.trim());
  if (uri == null) return false;
  final host = uri.host.toLowerCase();
  if (host.isEmpty) return false;
  if (host == 'localhost') return false;
  if (host.endsWith('.local')) return false;
  if (_isPrivateOrLoopbackIPv4(host)) return false;
  if (_isPrivateOrLoopbackIPv6(host)) return false;
  return true;
}

bool _isPrivateOrLoopbackIPv4(String host) {
  // Quick reject: not even close to an IPv4 literal.
  if (!RegExp(r'^\d{1,3}(\.\d{1,3}){3}$').hasMatch(host)) return false;
  final parts = host.split('.').map(int.parse).toList();
  if (parts.any((p) => p > 255)) return false;
  final a = parts[0];
  final b = parts[1];
  // 127.0.0.0/8 — loopback
  if (a == 127) return true;
  // 10.0.0.0/8
  if (a == 10) return true;
  // 172.16.0.0/12
  if (a == 172 && b >= 16 && b <= 31) return true;
  // 192.168.0.0/16
  if (a == 192 && b == 168) return true;
  // 169.254.0.0/16 — link-local
  if (a == 169 && b == 254) return true;
  return false;
}

bool _isPrivateOrLoopbackIPv6(String host) {
  // URI.host returns the IPv6 form without the surrounding brackets.
  if (!host.contains(':')) return false;
  // ::1 — loopback (canonical or expanded forms).
  if (host == '::1' || host == '0:0:0:0:0:0:0:1') return true;
  // :: — unspecified (treat as local).
  if (host == '::' || host == '0:0:0:0:0:0:0:0') return true;
  // fe80::/10 — link-local. First 10 bits = 1111 1110 10, i.e. first hextet
  // in [fe80, febf].
  final firstHextet = host.split(':').first;
  if (firstHextet.length >= 3) {
    final prefix = int.tryParse(firstHextet, radix: 16);
    if (prefix != null) {
      if (prefix >= 0xfe80 && prefix <= 0xfebf) return true;
      // fc00::/7 — Unique Local Address. First hextet in [fc00, fdff].
      if (prefix >= 0xfc00 && prefix <= 0xfdff) return true;
    }
  }
  return false;
}
