package com.revolsys.data.refresh;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class SupplierRefreshableValueHolder<V> implements RefreshableValueHolder<V> {
  private V value;

  private boolean valueLoaded;

  private final Supplier<V> supplier;

  private final ReentrantLock lock = new ReentrantLock();

  public SupplierRefreshableValueHolder(final Supplier<V> supplier) {
    this.supplier = supplier;
  }

  @Override
  public void clear() {
    this.lock.lock();
    try {
      this.valueLoaded = false;
      this.value = null;
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public V get() {
    final V value = this.value;
    if (!this.valueLoaded) {
      this.lock.lock();
      try {
        if (this.valueLoaded) {
          return this.value;
        } else {
          return reload();
        }
      } finally {
        this.lock.unlock();
      }
    }
    return value;
  }

  @Override
  public boolean isValueLoaded() {
    return this.valueLoaded;
  }

  public void refreshIfNeeded() {
    this.lock.lock();
    try {
      if (!this.valueLoaded) {
        reload();
      }
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public V reload() {
    this.lock.lock();
    try {
      final V newValue = this.value = this.supplier.get();
      this.valueLoaded = true;
      return newValue;
    } finally {
      this.lock.unlock();
    }
  }

}
