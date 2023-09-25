package com.revolsys.reactive.chars;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.CompletionHandler;

import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;

public abstract class AbstractByteBufToAsynchronousChannel<C extends AsynchronousChannel>
  extends AbstractByteBufToChannel<C> implements CompletionHandler<Integer, Long> {

  public AbstractByteBufToAsynchronousChannel(final Flux<ByteBuffer> source, final C channel,
    final MonoSink<Long> sink) {
    super(source, channel, sink);
  }

  @Override
  public void completed(final Integer count, final Long attachment) {
    if (count < 0) {
      this.sink.error(new IllegalStateException("Unepected negative write: " + count));
    }
    this.writeOutstanding -= count;
    doWrite();
  }

  @Override
  public void failed(final Throwable e, final Long attachment) {
    this.sink.error(e);
  }

}
