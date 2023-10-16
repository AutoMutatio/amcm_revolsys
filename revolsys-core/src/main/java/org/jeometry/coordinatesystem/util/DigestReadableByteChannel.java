package org.jeometry.coordinatesystem.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.security.MessageDigest;

public class DigestReadableByteChannel extends AbstractInterruptibleChannel
  implements ReadableByteChannel, MessageDigestProxy {

  private static final int BUF_SIZE = 1024;

  private final MessageDigest digest;

  private final ReadableByteChannel channel;

  private final byte[] buf = new byte[BUF_SIZE];

  public DigestReadableByteChannel(final ReadableByteChannel channel, final MessageDigest digest) {
    this.channel = channel;
    this.digest = digest;
  }

  @Override
  public MessageDigest getMessageDigest() {
    return this.digest;
  }

  @Override
  protected void implCloseChannel() throws IOException {
  }

  @Override
  public int read(final ByteBuffer dst) throws IOException {
    final int readCount = this.channel.read(dst);
    if (readCount > 0) {
      dst.flip();
      while (dst.hasRemaining()) {
        final int remaining = dst.remaining();
        final int count = Math.min(remaining, BUF_SIZE);
        dst.get(this.buf, 0, count);
        this.digest.update(this.buf, 0, count);
      }
    }
    return readCount;
  }
}
