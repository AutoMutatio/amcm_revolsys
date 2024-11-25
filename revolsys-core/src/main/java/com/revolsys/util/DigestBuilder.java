package com.revolsys.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.type.DataTypes;
import com.revolsys.exception.Exceptions;

public class DigestBuilder {
  public static DigestBuilder md5() {
    return new DigestBuilder("MD5");
  }

  public static DigestBuilder sha1() {
    return new DigestBuilder("SHA-1");
  }

  public static DigestBuilder sha256() {
    return new DigestBuilder("SHA-256");
  }

  public static DigestBuilder sha512() {
    return new DigestBuilder("SHA-512");
  }

  public static String shortId(final String s, final int length) {
    try {
      final var digester = MessageDigest.getInstance("SHA-512");
      final var digest = digester.digest(s.getBytes(StandardCharsets.UTF_8));
      final var chars = new char[length];
      for (int i = 0; i < length; i++) {
        final var b = digest[i % digest.length] & 0xFF;
        final char c = (char)('a' + b % 26);
        chars[i] = c;
      }
      return new String(chars);
    } catch (final NoSuchAlgorithmException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  private MessageDigest digester;

  public DigestBuilder(final String algorithm) {
    try {
      this.digester = MessageDigest.getInstance(algorithm);
    } catch (final NoSuchAlgorithmException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public DigestBuilder append(final byte[] bytes) {
    if (bytes != null) {
      this.digester.update(bytes);
    }
    return this;
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

  public String buildAZ(final int length) {
    final byte[] digest = this.digester.digest();
    final var size = Math.min(length, digest.length);
    final var c = new char[size];
    for (int i = 0; i < size; i++) {
      final var b = digest[i] & 0xFF;
      c[i] = (char)('a' + b % 26);
    }
    return new String(c);
  }

  public String buildBase64() {
    final byte[] digest = this.digester.digest();
    return Base64.getEncoder()
      .encodeToString(digest);
  }

  public String buildHex() {
    final byte[] digest = this.digester.digest();
    return Hex.toHex(digest);
  }

  public Identifier buildHexIdentifier() {
    final String string = toString();
    return Identifier.newIdentifier(string);
  }

  @Override
  public String toString() {
    return buildHex();
  }
}
