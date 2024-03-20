package com.revolsys.collection.value;

import java.util.function.Supplier;

import com.revolsys.parallel.ReentrantLockEx;

public class RefreshableValueHolder<T> extends SimpleValueHolder<T> {
  private final Supplier<T> valueSupplier;

  private final ReentrantLockEx lock = new ReentrantLockEx();

  public RefreshableValueHolder(final Supplier<T> valueSupplier) {
    this.valueSupplier = valueSupplier;
    refresh();
  }

  public void refresh() {
    try (
      var l = this.lock.lockX()) {
      final T value = this.valueSupplier.get();
      setValue(value);
    }
  }

}
