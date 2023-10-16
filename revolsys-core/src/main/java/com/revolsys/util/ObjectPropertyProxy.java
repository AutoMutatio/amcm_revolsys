package com.revolsys.util;

public interface ObjectPropertyProxy<T, O> {
  void clearValue();

  T getValue(final O object);
}
