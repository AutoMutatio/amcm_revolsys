package com.revolsys.reactive.chars;

import java.util.function.Predicate;

import io.netty.buffer.ByteBuf;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

class ByteBufFluxSplitWindow extends FluxOperator<ByteBuf, Flux<ByteBuf>>
  implements Predicate<ByteBuf> {

  private final long windowSize;

  private long size;

  ByteBufFluxSplitWindow(final Flux<? extends ByteBuf> source, final long windowSize) {
    super(source);
    this.windowSize = windowSize;
  }

  @Override
  public void subscribe(final CoreSubscriber<? super Flux<ByteBuf>> subscription) {
    ByteBufs.alignWindow(this.source, this.windowSize)
      .windowUntil(this, true, 1)
      .subscribe(subscription);
  }

  @Override
  public boolean test(final ByteBuf buffer) {
    try {
      if (this.size < this.windowSize) {
        return false;
      } else if (this.size == this.windowSize) {
        this.size = 0;
        return true;
      } else {
        throw new IllegalStateException("Data must be aligned on window size");
      }
    } finally {
      this.size += buffer.readableBytes();
    }
  }
}
