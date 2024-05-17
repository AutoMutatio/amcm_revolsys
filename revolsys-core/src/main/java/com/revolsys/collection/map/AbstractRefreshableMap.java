package com.revolsys.collection.map;

import java.util.Collections;
import java.util.Map;

import com.revolsys.data.refresh.SupplierRefreshableValueHolder;

public abstract class AbstractRefreshableMap<K, V> extends AbstractDelegatingMap<K, V>
  implements RefreshableMap<K, V> {

  private String label;

  private final SupplierRefreshableValueHolder<Map<K, V>> value = new SupplierRefreshableValueHolder<>(
    this::loadValue);

  private boolean valueLoaded;

  public AbstractRefreshableMap(final boolean editable) {
    super(editable);
  }

  @Override
  public void clearValue() {
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
  public void refresh() {
    this.value.reload();
  }

  @Override
  public void refreshIfNeeded() {
    this.value.refreshIfNeeded();
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
