package com.revolsys.collection.list;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import com.revolsys.data.refresh.RefreshableValueHolder;
import com.revolsys.data.refresh.SupplierRefreshableValueHolder;

public abstract class AbstractRefreshableList<V> extends AbstractDelegatingList<V>
  implements RefreshableList<V> {

  private String label;

  private final RefreshableValueHolder<List<V>> value = new SupplierRefreshableValueHolder<>(
    this::loadValue);

  private final ReentrantLock lock = new ReentrantLock();

  public AbstractRefreshableList(final boolean editable) {
    super(editable);
  }

  @Override
  public void clearValue() {
    this.lock.lock();
    try {
      this.value.clear();
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public String getLabel() {
    if (this.label == null) {
      return toString();
    } else {
      return this.label;
    }
  }

  @Override
  protected List<V> getList() {
    final List<V> value = this.value.get();
    if (value == null) {
      return Collections.emptyList();
    } else {
      return value;
    }
  }

  protected abstract List<V> loadValue();

  @Override
  public void refresh() {
    this.lock.lock();
    try {
      this.value.reload();
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public void refreshIfNeeded() {
    this.lock.lock();
    try {
      this.value.get();
    } finally {
      this.lock.unlock();
    }
  }

  public AbstractRefreshableList<V> setLabel(final String label) {
    this.label = label;
    return this;
  }

  @Override
  public String toString() {
    if (this.value.isValueLoaded()) {
      return this.value.toString();
    } else if (this.label != null) {
      return this.label;
    } else {
      return getClass().getName() + "@" + Integer.toHexString(hashCode());
    }
  }
}
