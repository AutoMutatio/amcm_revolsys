package com.revolsys.reactive.chars;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;

import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;

public class ByteBufToAsyncChannel
  extends AbstractByteBufToAsynchronousChannel<AsynchronousByteChannel> {
  public ByteBufToAsyncChannel(final Flux<ByteBuffer> source, final AsynchronousByteChannel channel,
    final MonoSink<Long> sink) {
    super(source, channel, sink);
  }

  @Override
  protected int doWrite(final ByteBuffer bytes, final long offset) {
    final int writeOutstanding = bytes.remaining();
    this.channel.write(bytes, offset, this);
    return writeOutstanding;
  }

}
