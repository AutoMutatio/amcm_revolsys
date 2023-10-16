package org.jeometry.common.data.refresh;

import java.util.function.Supplier;

public class SupplierRefreshableValueHolder<V> implements RefreshableValueHolder<V> {
  private V value;

  private boolean valueLoaded;

  private final Supplier<V> supplier;

  public SupplierRefreshableValueHolder(final Supplier<V> supplier) {
    this.supplier = supplier;
  }

  @Override
  public synchronized void clear() {
    this.valueLoaded = false;
    this.value = null;
  }

  @Override
  public V get() {
    final V value = this.value;
    if (!this.valueLoaded) {
      synchronized (this) {
        if (this.valueLoaded) {
          return this.value;
        } else {
          return reload();
        }
      }
    }
    return value;
  }

  @Override
  public boolean isValueLoaded() {
    return this.valueLoaded;
  }

  @Override
  public V reload() {
    final V newValue = this.value = this.supplier.get();
    this.valueLoaded = true;
    return newValue;
  }

}
