package com.revolsys.collection.value;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.revolsys.util.BaseCloseable;
import com.revolsys.util.Emptyable;

public interface ValueHolder<T> extends Emptyable, Supplier<T> {
  public static <V> LazyValueHolder<V> lazy(final Function<V, V> valueRefresh,
    final Predicate<V> validator) {
    return new LazyValueHolder<>(valueRefresh, validator);
  }

  public static <V> LazyValueHolder<V> lazy(final Supplier<V> valueSupplier) {
    return new LazyValueHolder<>(valueSupplier);
  }

  public static <V> LazyValueHolder<V> lazy(final Supplier<V> valueSupplier,
    final Predicate<V> validator) {
    return new LazyValueHolder<>(valueSupplier, validator);
  }

  public static <V> ValueHolder<V> of(final V value) {
    return new SimpleValueHolder<>(value);
  }

  default BaseCloseable closeable(final T value) {
    return new ValueCloseable<>(this, value);
  }

  @Override
  default T get() {
    return getValue();
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

  default T setValue(final T value) {
    throw new UnsupportedOperationException("Value is readonly");
  }

  default <V> ValueHolder<V> then(final Function<T, V> converter) {
    return () -> converter.apply(getValue());
  }
}
