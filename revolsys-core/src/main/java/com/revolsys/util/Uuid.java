package com.revolsys.util;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;

import com.revolsys.collection.list.Lists;
import com.revolsys.exception.ExceptionWithProperties;
import com.revolsys.number.Longs;

public class Uuid {
  public static final Comparator<UUID> COMPARATOR = Uuid::compare;

  public static final UUID NIL = new UUID(0, 0);

  public static final UUID MAX = new UUID(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL);

  private static final SecureRandom numberGenerator = new SecureRandom();

  public static int compare(UUID uuid1, Object object2) {
    if (object2 instanceof final UUID uuid2) {
      return compare(uuid1, uuid2);
    } else if (uuid1 == null) {
      if (object2 == null) {
        return 0;
      } else {
        return 1;
      }
    } else if (object2 == null) {
      return -1;
    } else {
      return uuid1.toString().compareTo(object2.toString());
    }
  }

  public static int compare(final UUID uuid1, final UUID uuid2) {
    return uuid1.toString().compareTo(uuid2.toString());
  }

  public static UUID custom(byte[] namespace, int bits, long value) {
    Objects.checkIndex(bits, 61);
    long l1 = Longs.toLong(namespace, 0);
    l1 &= 0xFFFFFFFFFFFF0FFFL; // Clear Variant
    l1 |= 0x0000000000008000L; // Set Variant

    long l2 = Longs.toLong(namespace, 8);
    l2 &= 0x0FFFFFFFFFFFFFFFL; // Clear Variant
    l2 |= 0x8000000000000000L; // Set Variant

    final long mask = (1L << bits) - 1;
    if ((value & mask) != value) {
      throw new ExceptionWithProperties("Value is greater than number of allowed bits")
        .property("bits", bits)
        .property("value", value);
    }
    l2 &= ~mask;
    l2 |= value;
    return new UUID(l1, l2);
  }

  public static Iterable<UUID> fromString(final String... ids) {
    return Lists.newArray(ids).filter(s -> !s.isBlank()).map(UUID::fromString);
  }

  public static UuidNamespace md5(final byte[] namespace) {
    return new UuidNamespace(3, UuidNamespace.toUuid(3, namespace));
  }

  public static UuidNamespace md5(final String namespace) {
    final UUID uuid = UUID.fromString(namespace);
    return md5(uuid);
  }

  public static UuidNamespace md5(final UUID namespace) {
    return new UuidNamespace(3, namespace);
  }

  public static UuidNamespace sha1(final String namespace) {
    final UUID uuid = UUID.fromString(namespace);
    return sha1(uuid);
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

  public static UUID unixEpoch() {
    final var version = 0x70; // 7 << 4
    final var variant = 0x80; // 0b10 << 6
    final byte[] randomBytes = new byte[10];
    numberGenerator.nextBytes(randomBytes);
    final var timeMillis = System.currentTimeMillis() << 16;
    final var byte7 = version | randomBytes[0] & 0x0F;
    final long msb = timeMillis & 0xFFFFFFFFFFFF0000L | byte7 << 8 | randomBytes[1] & 0xFF;
    long lsb = variant | randomBytes[2] & 0x3F;
    for (int i = 3; i < 10; i++) {
      lsb = lsb << 8 | randomBytes[i] & 0xff;
    }
    return new UUID(msb, lsb);
  }
}
