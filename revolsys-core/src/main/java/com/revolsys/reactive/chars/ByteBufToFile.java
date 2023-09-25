package com.revolsys.reactive.chars;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;

import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;

class ByteBufToFile extends AbstractByteBufToAsynchronousChannel<AsynchronousFileChannel> {
  ByteBufToFile(final Flux<ByteBuffer> source, final AsynchronousFileChannel channel,
    final MonoSink<Long> sink) {
    super(source, channel, sink);
  }

  @Override
  protected int doWrite(final ByteBuffer bytes, final long offset) {
    final int writeOutstanding = bytes.remaining();
    this.channel.write(bytes, offset, offset, this);
    return writeOutstanding;
  }

}
