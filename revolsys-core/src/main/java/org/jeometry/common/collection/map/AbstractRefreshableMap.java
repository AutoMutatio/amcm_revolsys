package org.jeometry.common.collection.map;

import java.util.Collections;
import java.util.Map;

import org.jeometry.common.data.refresh.RefreshableValueHolder;
import org.jeometry.common.data.refresh.SupplierRefreshableValueHolder;

public abstract class AbstractRefreshableMap<K, V> extends AbstractDelegatingMap<K, V>
  implements RefreshableMap<K, V> {

  private String label;

  private final RefreshableValueHolder<Map<K, V>> value = new SupplierRefreshableValueHolder<>(
    this::loadValue);

  private boolean valueLoaded;

  public AbstractRefreshableMap(final boolean editable) {
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
  protected Map<K, V> getMap() {
    final Map<K, V> value = this.value.get();
    if (value == null) {
      return Collections.emptyMap();
    } else {
      return value;
    }
  }

  protected abstract Map<K, V> loadValue();

  @Override
  public synchronized void refresh() {
    this.value.get();
  }

  @Override
  public synchronized void refreshIfNeeded() {
    if (!this.valueLoaded) {
      refresh();
    }
  }

  public AbstractRefreshableMap<K, V> setLabel(final String label) {
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
