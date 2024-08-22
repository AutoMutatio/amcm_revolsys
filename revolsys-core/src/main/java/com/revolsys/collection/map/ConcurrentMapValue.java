package com.revolsys.collection.map;

import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import com.revolsys.exception.Exceptions;

public class ConcurrentMapValue<V> {
  private final Supplier<V> initializer;

  private final CountDownLatch latch = new CountDownLatch(1);

  private V value;

  public ConcurrentMapValue(final Supplier<V> initializer) {
    this.initializer = initializer;
  }

  public V getValue() {
    try {
      this.latch.await();
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return this.value;
  }

  public void refresh() {
    this.value = this.initializer.get();
    this.latch.countDown();
  }

  @Override
  public String toString() {
    if (this.value == null) {
      return null;
    } else {
      return this.value.toString();
    }
  }
}
