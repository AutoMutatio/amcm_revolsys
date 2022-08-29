package com.revolsys.reactive.bytebuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.function.Function;

import org.jeometry.common.exception.Exceptions;

import io.netty.buffer.ByteBuf;

public class ByteBufChannelWriter implements Function<ByteBuf, Integer> {
  private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8192);

  private final WritableByteChannel channel;

  public ByteBufChannelWriter(final WritableByteChannel channel) {
    this.channel = channel;
  }

  @Override
  public Integer apply(final ByteBuf buffer) {
    final int count = buffer.readableBytes();
    try {
      for (int remaining = buffer.readableBytes(); remaining > 0; remaining = buffer
        .readableBytes()) {
        this.byteBuffer.clear();
        this.byteBuffer.limit(Math.min(remaining, 8192));
        buffer.readBytes(this.byteBuffer);
        this.byteBuffer.flip();
        while (this.byteBuffer.hasRemaining()) {
          this.channel.write(this.byteBuffer);
        }
      }
      return count;
    } catch (final IOException e) {
      throw Exceptions.wrap(e);
    } finally {
      buffer.release();
    }
  }
}
