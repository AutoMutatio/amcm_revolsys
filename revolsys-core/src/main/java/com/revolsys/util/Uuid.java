package com.revolsys.util;

import java.util.Base64;
import java.util.UUID;

public class Uuid {

  public static UuidNamespace md5(final String namespace) {
    return new UuidNamespace(3, namespace);
  }

  public static UuidNamespace md5(final UUID namespace) {
    return new UuidNamespace(3, namespace);
  }

  public static UuidNamespace sha1(final String namespace) {
    return new UuidNamespace(5, namespace);
  }

  public static UuidNamespace sha1(final UUID namespace) {
    return new UuidNamespace(5, namespace);
  }

  public static String toBase64(final UUID uuid) {
    final byte[] bytes = toBytes(uuid);
    return Base64.getEncoder().encodeToString(bytes);
  }

  public static byte[] toBytes(final UUID uuid) {
    final long msb = uuid.getMostSignificantBits();
    final long lsb = uuid.getLeastSignificantBits();
    final byte[] buffer = new byte[16];
    for (int i = 0; i < 8; i++) {
      buffer[i] = (byte)(msb >>> 8 * (7 - i));
    }
    for (int i = 8; i < 16; i++) {
      buffer[i] = (byte)(lsb >>> 8 * (7 - i));
    }
    return buffer;
  }
}
