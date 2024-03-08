package com.revolsys.util;

import java.util.function.Supplier;

import com.revolsys.parallel.ReentrantLockEx;

public class LazyValue<V> extends SimpleValueWrapper<V> {

  public static <T> LazyValue<T> newValue(final Supplier<T> supplier) {
    return new LazyValue<>(supplier);
  }

  private final ReentrantLockEx lock = new ReentrantLockEx();

  private boolean initialized = false;

  private Supplier<V> supplier;

  public LazyValue(final Supplier<V> supplier) {
    this.supplier = supplier;
  }

  public void clearValue() {
    try (
      var l = this.lock.lockX()) {
      this.initialized = false;
      this.value = null;
    }
  }

  @Override
  public void close() {
    try (
      var l = this.lock.lockX()) {
      super.close();
      this.supplier = null;
    }
  }

  @Override
  public V getValue() {
    try (
      var l = this.lock.lockX()) {
      final Supplier<V> supplier = this.supplier;
      if (!this.initialized && supplier != null) {
        this.initialized = true;
        this.value = supplier.get();
      }
      return this.value;
    }
  }

}
