package org.jeometry.common.collection.set;

import java.util.Collections;
import java.util.Set;

import org.jeometry.common.data.refresh.RefreshableValueHolder;
import org.jeometry.common.data.refresh.SupplierRefreshableValueHolder;

public abstract class AbstractRefreshableSet<V> extends AbstractDelegatingSet<V>
  implements RefreshableSet<V> {

  private String label;

  private final RefreshableValueHolder<Set<V>> value = new SupplierRefreshableValueHolder<>(
    this::loadValue);

  public AbstractRefreshableSet(final boolean editable) {
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
  protected Set<V> getSet() {
    final Set<V> value = this.value.get();
    if (value == null) {
      return Collections.emptySet();
    } else {
      return value;
    }
  }

  protected abstract Set<V> loadValue();

  @Override
  public synchronized void refresh() {
    this.value.reload();
  }

  @Override
  public synchronized void refreshIfNeeded() {
    this.value.get();
  }

  public AbstractRefreshableSet<V> setLabel(final String label) {
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
