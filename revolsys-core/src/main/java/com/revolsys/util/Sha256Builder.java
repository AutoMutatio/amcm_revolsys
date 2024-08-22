package com.revolsys.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.type.DataTypes;
import com.revolsys.exception.Exceptions;

public class Sha256Builder {
  private MessageDigest digester;

  public Sha256Builder() {
    try {
      this.digester = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      Exceptions.throwUncheckedException(e);
    }
  }

  public Sha256Builder(final String namespace) {
    this();
    append(namespace);
  }

  public Sha256Builder append(final byte[] bytes) {
    if (bytes != null) {
      this.digester.update(bytes);
    }
    return this;
  }

  public Sha256Builder append(final Object value) {
    if (value instanceof String) {
      append((String)value);
    } else if (value != null) {
      final String string = DataTypes.toString(value);
      append(string);
    }
    return this;
  }

  public Sha256Builder append(final String string) {
    final byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    return append(bytes);
  }

  public String newAZ(final int length) {

    final byte[] digest = this.digester.digest();

    final var size = Math.min(length, digest.length);
    final var c = new char[size];
    for (int i = 0; i < size; i++) {
      final var b = digest[i] & 0xFF;
      c[i] = (char)('a' + b % 26);
    }
    return new String(c);
  }

  public String newHex() {
    final byte[] digest = this.digester.digest();
    return Hex.toHex(digest);
  }

  public Identifier newStringIdentifier() {
    final String string = toString();
    return Identifier.newIdentifier(string);
  }

  @Override
  public String toString() {
    return newHex();
  }
}
