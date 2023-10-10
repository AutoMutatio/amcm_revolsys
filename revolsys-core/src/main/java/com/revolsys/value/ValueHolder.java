package com.revolsys.value;

import org.jeometry.common.util.BaseCloseable;
import org.jeometry.common.util.Emptyable;

public interface ValueHolder<T> extends Emptyable {
  default BaseCloseable closeable(final T value) {
    return new ValueCloseable<>(this, value);
  }

  T getValue();

  @Override
  default boolean isEmpty() {
    return getValue() == null;
  }

  default void run(final T newValue, final Runnable runnable) {
    final T oldValue = setValue(newValue);
    try {
      runnable.run();
    } finally {
      setValue(oldValue);
    }
  }

  T setValue(T value);
}
