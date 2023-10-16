package com.revolsys.data.refresh;

public interface RefreshableValueHolder<V> {

  void clear();

  V get();

  boolean isValueLoaded();

  V reload();
}
