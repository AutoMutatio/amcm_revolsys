package com.revolsys.reactive;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.function.Consumer;

import org.jeometry.coordinatesystem.util.Md5;
import org.jeometry.coordinatesystem.util.MessageDigestProxy;

public class DigestConsumer<B extends ByteBuffer> implements Consumer<B>, MessageDigestProxy {

  public static DigestConsumer<ByteBuffer> md5() {
    final MessageDigest messageDigest = Md5.getMessageDigest();
    return new DigestConsumer<>(messageDigest);
  }

  private final MessageDigest digest;

  public DigestConsumer(final MessageDigest digest) {
    this.digest = digest;
  }

  @Override
  public void accept(final ByteBuffer buf) {
    this.digest.update(buf);
    buf.flip();
  }

  @Override
  public MessageDigest getMessageDigest() {
    return this.digest;
  }
}
