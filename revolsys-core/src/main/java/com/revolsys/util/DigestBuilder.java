package com.revolsys.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.type.DataTypes;

public class DigestBuilder {
  private final MessageDigest digester;

  public DigestBuilder(final MessageDigest digester) {
    this.digester = digester;
  }

  public DigestBuilder append(final byte[] bytes) {
    if (bytes != null) {
      this.digester.update(bytes);
    }
    return this;
  }

  public void append(final byte[] data, final int offset, final int length) {
    this.digester.update(data, offset, length);
  }

  public DigestBuilder append(final Object value) {
    if (value instanceof String) {
      append((String)value);
    } else if (value != null) {
      final String string = DataTypes.toString(value);
      append(string);
    }
    return this;
  }

  public DigestBuilder append(final String string) {
    final byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    return append(bytes);
  }

  public String toAZ(final int length) {
    final byte[] digest = toBytes();

    final var size = Math.min(length, digest.length);
    final var c = new char[size];
    for (int i = 0; i < size; i++) {
      final var b = digest[i] & 0xFF;
      c[i] = (char)('a' + b % 26);
    }
    return new String(c);
  }

  public String toBase64() {
    final byte[] bytes = toBytes();
    return Base64.getEncoder().encodeToString(bytes);
  }

  public byte[] toBytes() {
    return this.digester.digest();
  }

  public String toHex() {
    final byte[] digest = toBytes();
    return Hex.toHex(digest);
  }

  @Override
  public String toString() {
    return toHex();
  }

  public Identifier toStringIdentifier() {
    final String string = toString();
    return Identifier.newIdentifier(string);
  }
}
