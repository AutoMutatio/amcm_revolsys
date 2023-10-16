package org.jeometry.common.collection.list;

import java.util.Collections;
import java.util.List;

import org.jeometry.common.data.refresh.RefreshableValueHolder;
import org.jeometry.common.data.refresh.SupplierRefreshableValueHolder;

public abstract class AbstractRefreshableList<V> extends AbstractDelegatingList<V>
  implements RefreshableList<V> {

  private String label;

  private final RefreshableValueHolder<List<V>> value = new SupplierRefreshableValueHolder<>(
    this::loadValue);

  public AbstractRefreshableList(final boolean editable) {
    super(editable);
  }

  @Override
  public synchronized void clearValue() {
    this.value.clear();
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
  public synchronized void refresh() {
    this.value.reload();
  }

  @Override
  public synchronized void refreshIfNeeded() {
    this.value.get();
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
