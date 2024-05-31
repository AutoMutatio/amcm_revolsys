package com.revolsys.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.type.DataTypes;
import com.revolsys.exception.Exceptions;

public class Sha512Builder {

  public static String shortId(final String s, final int length) {
    try {
      final var digester = MessageDigest.getInstance("SHA-512");
      final var digest = digester.digest(s.getBytes(StandardCharsets.UTF_8));
      final var chars = new char[length];
      for (int i = 0; i < length; i++) {
        final var b = digest[i % digest.length] & 0xFF;
        final char c = (char) ('a' + b % 26);
        chars[i] = c;
      }
      return new String(chars);
    } catch (final NoSuchAlgorithmException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  private MessageDigest digester;

  public Sha512Builder() {
    try {
      this.digester = MessageDigest.getInstance("SHA-512");
    } catch (final NoSuchAlgorithmException e) {
      Exceptions.throwUncheckedException(e);
    }
  }

  public Sha512Builder(final String namespace) {
    this();
    append(namespace);
  }

  public Sha512Builder append(final byte[] bytes) {
    if (bytes != null) {
      this.digester.update(bytes);
    }
    return this;
  }

  public Sha512Builder append(final Object value) {
    if (value instanceof String) {
      append((String) value);
    } else if (value != null) {
      final String string = DataTypes.toString(value);
      append(string);
    }
    return this;
  }

  public Sha512Builder append(final String string) {
    final byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    return append(bytes);
  }

  public String newAZ(final int length) {

    final byte[] digest = this.digester.digest();

    final var size = Math.min(length, digest.length);
    final var c = new char[size];
    for (int i = 0; i < size; i++) {
      c[i] = (char) ('a' + digest[i] % 26);
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
