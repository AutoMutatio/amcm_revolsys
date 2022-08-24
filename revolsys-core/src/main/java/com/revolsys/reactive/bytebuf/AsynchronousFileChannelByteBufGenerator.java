package com.revolsys.reactive.bytebuf;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.function.BiFunction;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import reactor.core.publisher.SynchronousSink;

public class AsynchronousFileChannelByteBufGenerator
  implements BiFunction<AsynchronousFileChannel, SynchronousSink<ByteBuf>, AsynchronousFileChannel>,
  CompletionHandler<Integer, SynchronousSink<ByteBuf>> {

  private long position = 0;

  private final ByteBuffer buffer;

  private final int bufferSize;

  public AsynchronousFileChannelByteBufGenerator() {
    this(8192);
  }

  public AsynchronousFileChannelByteBufGenerator(final int bufferSize) {
    this.bufferSize = bufferSize;
    this.buffer = ByteBuffer.allocateDirect(bufferSize);
  }

  @Override
  public AsynchronousFileChannel apply(final AsynchronousFileChannel fileChannel,
    final SynchronousSink<ByteBuf> sink) {
    fileChannel.read(this.buffer, this.position, sink, this);
    return fileChannel;
  }

  @Override
  public void completed(final Integer length, final SynchronousSink<ByteBuf> sink) {
    try {
      final ByteBuffer tempBuffer = this.buffer;
      final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(this.bufferSize);
      tempBuffer.flip();
      buffer.writeBytes(tempBuffer);
      tempBuffer.clear();
      sink.next(buffer);
      this.position += length;
    } catch (final Exception e) {
      sink.error(e);
    }

  }

  @Override
  public void failed(final Throwable e, final SynchronousSink<ByteBuf> sink) {
    sink.error(e);
  }
}
