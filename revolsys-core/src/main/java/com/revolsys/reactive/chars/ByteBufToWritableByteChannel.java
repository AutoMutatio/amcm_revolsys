package com.revolsys.reactive.chars;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;

class ByteBufToWritableByteChannel extends AbstractByteBufToChannel<WritableByteChannel> {
  ByteBufToWritableByteChannel(final Flux<ByteBuffer> source, final WritableByteChannel channel,
    final MonoSink<Long> sink) {
    super(source, channel, sink);
  }

  @Override
  protected int doWrite(final ByteBuffer bytes, final long offset) throws IOException {
    final int size = bytes.remaining();
    final int written = this.channel.write(bytes);
    return size - written;
  }

}
