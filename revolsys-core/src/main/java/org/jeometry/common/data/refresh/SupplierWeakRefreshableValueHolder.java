package org.jeometry.common.data.refresh;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.function.Supplier;

public class SupplierWeakRefreshableValueHolder<V> implements RefreshableValueHolder<V> {
  private WeakReference<V> reference;

  private final ReferenceQueue<V> queue = new ReferenceQueue<>();

  private final Supplier<V> supplier;

  public SupplierWeakRefreshableValueHolder(final Supplier<V> supplier) {
    this.supplier = supplier;
  }

  @Override
  public void clear() {
    this.reference = null;
  }

  @Override
  public synchronized V get() {
    if (this.reference != null) {
      final V value = this.reference.get();
      if (value != null) {
        return value;
      }
      if (value == null) {
        for (Reference<?> r = this.queue.poll(); r != null; r = this.queue.poll()) {
          if (r == this.reference) {
            this.reference = null;
          }
        }
        if (this.reference != null) {
          return null;
        }
      }
    }

    return reload();
  }

  @Override
  public boolean isValueLoaded() {
    for (Reference<?> r = this.queue.poll(); r != null; r = this.queue.poll()) {
      if (r == this.reference) {
        this.reference = null;
      }
    }
    return this.reference != null;
  }

  @Override
  public V reload() {
    final V value = this.supplier.get();
    this.reference = new WeakReference<>(value, this.queue);
    return value;
  }
}
