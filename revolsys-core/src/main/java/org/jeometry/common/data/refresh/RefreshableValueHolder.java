package org.jeometry.common.data.refresh;

public interface RefreshableValueHolder<V> {

  void clear();

  V get();

  boolean isValueLoaded();

  V reload();
}
