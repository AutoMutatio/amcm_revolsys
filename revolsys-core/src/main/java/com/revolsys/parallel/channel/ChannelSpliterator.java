package com.revolsys.parallel.channel;

import java.util.Spliterator;
import java.util.function.Consumer;

public class ChannelSpliterator<T> implements Spliterator<T> {

  private final Channel<? extends T> channel;

  private long estimateSize;

  public ChannelSpliterator(final Channel<? extends T> channel, final long estimateSize) {
    this.channel = channel;
    this.estimateSize = estimateSize;
  }

  @Override
  public int characteristics() {
    return CONCURRENT;
  }

  @Override
  public long estimateSize() {
    return this.estimateSize;
  }

  @Override
  public boolean tryAdvance(final Consumer<? super T> action) {
    if (action == null) {
      throw new NullPointerException();
    }
    if (this.channel.isOpen()) {
      try {
        action.accept(this.channel.read());
        return true;
      } catch (final ClosedException e) {
        return false;
      }
    }
    return false;
  }

  @Override
  public Spliterator<T> trySplit() {
    if (this.estimateSize == 0) {
      return null;
    }
    return new ChannelSpliterator<>(this.channel, this.estimateSize >>>= 1);
  }

}
