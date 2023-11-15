package com.revolsys.collection.value;

import java.util.function.Supplier;

import com.revolsys.util.BaseCloseable;
import com.revolsys.util.Emptyable;

public interface ValueHolder<T> extends Emptyable {
  public static <V> LazyValueHolder<V> lazy(final Supplier<V> valueSupplier) {
    return new LazyValueHolder<>(valueSupplier);
  }

  public static <V> ValueHolder<V> of(final V value) {
    return new SimpleValueHolder<>(value);
  }

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
