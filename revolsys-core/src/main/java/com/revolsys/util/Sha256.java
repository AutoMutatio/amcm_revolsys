package com.revolsys.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Supplier;

import com.revolsys.io.DigestReadableByteChannel;
import com.revolsys.io.stream.DigestInputStream;
import com.revolsys.io.stream.DigestOutputStream;

public class Sha256 {

  public static Supplier<MessageDigest> SUPPLIER = Sha256::getMessageDigest;

  public static DigestBuilder builder() {
    final MessageDigest digest = getMessageDigest();
    return new DigestBuilder(digest);
  }

  public static DigestReadableByteChannel channel(final ReadableByteChannel in) {
    final MessageDigest messageDigest = getMessageDigest();
    return new DigestReadableByteChannel(in, messageDigest);

  }

  public static DigestBuilder fromBytes(final byte[] data) {
    return builder().append(data);
  }

  public static DigestBuilder fromInputStream(final InputStream data) throws IOException {
    final var digest = builder();
    final int bufferSize = 1024;
    final byte[] buffer = new byte[bufferSize];
    int read = data.read(buffer, 0, bufferSize);

    while (read > -1) {
      digest.append(buffer, 0, read);
      read = data.read(buffer, 0, bufferSize);
    }

    return digest;
  }

  public static DigestBuilder fromString(final String data) {
    return builder().append(data);
  }

  public static MessageDigest getMessageDigest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 Digest not found", e);
    }
  }

  public static MessageDigestEx getMessageDigestEx() {
    final var digest = getMessageDigest();
    return new MessageDigestEx(digest);
  }

  public static DigestInputStream inputStream(final InputStream in) {
    final MessageDigest messageDigest = getMessageDigest();
    return new DigestInputStream(in, messageDigest);

  }

  public static DigestOutputStream outputStream(final OutputStream out) {
    final MessageDigest messageDigest = getMessageDigest();
    return new DigestOutputStream(out, messageDigest);

  }

  public static void update(final MessageDigest digest, final double value) {
    final long l = Double.doubleToLongBits(value);
    final String data = Long.toString(l);
    update(digest, data);
  }

  public static void update(final MessageDigest digest, final String data) {
    final byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
    digest.update(bytes);
  }
}
