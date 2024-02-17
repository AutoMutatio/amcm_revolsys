package com.revolsys.collection.value;

import java.time.Duration;
import java.util.function.Supplier;

import com.revolsys.parallel.ReentrantLockEx;

public class CacheValueHolder<T> extends SimpleValueHolder<T> {
  private Supplier<T> valueSupplier;

  private long expireTime = Long.MAX_VALUE;

  private long expiry;

  private final ReentrantLockEx lock = new ReentrantLockEx();

  public CacheValueHolder(final Supplier<T> valueSupplier) {
    this(valueSupplier, Duration.ZERO);
  }

  public CacheValueHolder(final Supplier<T> valueSupplier, final Duration expiry) {
    this.valueSupplier = valueSupplier;
    this.expiry = expiry.toMillis();
  }

  @Override
  public T getValue() {
    try (
      var l = this.lock.lockX()) {
      T value = super.getValue();
      if (value == null || System.currentTimeMillis() > this.expireTime) {
        value = this.valueSupplier.get();
        super.setValue(value);
        if (this.expiry > 0) {
          this.expireTime = System.currentTimeMillis() + this.expiry;
        }
      }
      return value;
    }
  }

  public boolean isInitialized() {
    return this.valueSupplier == null;
  }

  @Override
  public T setValue(final T value) {
    throw new UnsupportedOperationException("Value cannot be changed");
  }

}
