package com.revolsys.reactive.bytebuf;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.function.Consumer;

import org.jeometry.coordinatesystem.util.Md5;
import org.jeometry.coordinatesystem.util.MessageDigestProxy;

import io.netty.buffer.ByteBuf;

public class DigestConsumer implements Consumer<ByteBuf>, MessageDigestProxy {

  public static DigestConsumer md5() {
    final MessageDigest messageDigest = Md5.getMessageDigest();
    return new DigestConsumer(messageDigest);
  }

  private final MessageDigest digest;

  private final ByteBuffer buffer = ByteBuffer.allocate(8192);

  public DigestConsumer(final MessageDigest digest) {
    this.digest = digest;
  }

  @Override
  public void accept(final ByteBuf buffer) {
    final ByteBuffer tempBuffer = this.buffer;
    tempBuffer.clear();
    buffer.getBytes(0, tempBuffer);
    tempBuffer.flip();
    this.digest.update(tempBuffer);
  }

  @Override
  public MessageDigest getMessageDigest() {
    return this.digest;
  }
}
