package com.revolsys.data.refresh;

public abstract class AbstractRefreshableValue<V> implements RefreshableValue<V> {

  private RefreshableValueHolder<V> value = new SupplierRefreshableValueHolder<>(this::loadValue);

  private String label;

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
  public V getValue() {
    return this.value.get();
  }

  public boolean hasValue() {
    return this.value.isValueLoaded();
  }

  protected abstract V loadValue();

  @Override
  public void refreshIfNeeded() {
    refreshValueIfNeeded();
  }

  @Override
  public V refreshValue() {
    return this.value.reload();
  }

  @Override
  public V refreshValueIfNeeded() {
    return this.value.get();
  }

  public AbstractRefreshableValue<V> setLabel(final String label) {
    this.label = label;
    return this;
  }

  public AbstractRefreshableValue<V> setWeak() {
    this.value = new SupplierWeakRefreshableValueHolder<>(this::loadValue);
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
