package com.revolsys.collection.value;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.revolsys.util.BaseCloseable;
import com.revolsys.util.Emptyable;

public interface ValueHolder<T> extends Emptyable, Supplier<T> {

  @SuppressWarnings("unchecked")
  public static <T> ValueHolder<T> empty() {
    return (ValueHolder<T>)EmptyValueHolder.EMPTY;
  }

  public static <V> LazyValueHolder.Builder<V> lazy() {
    return new LazyValueHolder.Builder<>();

  }

  public static <V> LazyValueHolder<V> lazy(final Function<V, V> valueRefresh,
    final Predicate<V> validator) {
    return ValueHolder.<V> lazy()
      .valueRefresh(valueRefresh)
      .validator(validator)
      .build();
  }

  public static <V> LazyValueHolder<V> lazy(final Supplier<V> valueSupplier) {
    return new LazyValueHolder<>(valueSupplier);
  }

  public static <V> LazyValueHolder<V> lazy(final Supplier<V> valueSupplier,
    final Predicate<V> validator) {
    return ValueHolder.<V> lazy()
      .valueSupplier(valueSupplier)
      .validator(validator)
      .build();
  }

  public static <V> ValueHolder<V> of(final V value) {
    return new SimpleValueHolder<>(value);
  }

  public static <V> ValueHolder<V> ofNullable(final V value) {
    if (value == null) {
      return empty();
    } else {
      return ValueHolder.of(value);
    }
  }

  default void close() {
  }

  default BaseCloseable closeable(final T value) {
    return new ValueCloseable<>(this, value);
  }

  default ValueHolder<T> filter(final Predicate<? super T> predicate) {
    return predicate.test(getValue()) ? this : empty();
  }

  @Override
  default T get() {
    return getValue();
  }

  default T getOrDefault(final Supplier<? extends T> supplier) {
    return getValue();
  }

  default T getOrDefault(final T other) {
    return getValue();
  }

  default T getOrThrow() {
    return getValue();
  }

  default <X extends Throwable> T getOrThrow(final Supplier<? extends X> exceptionSupplier)
    throws X {
    return getValue();
  }

  T getValue();

  default void ifPresent(final Consumer<? super T> action) {
    action.accept(getValue());
  }

  default void ifPresentOrElse(final Consumer<? super T> action, final Runnable emptyAction) {
    action.accept(getValue());
  }

  @Override
  default boolean isEmpty() {
    return getValue() == null;
  }

  default boolean isPresent() {
    return true;
  }

  default <U> ValueHolder<U> map(final Function<? super T, ? extends U> mapper) {
    return ValueHolder.ofNullable(mapper.apply(getValue()));
  }

  default <U> ValueHolder<U> mapValueHolder(
    final Function<? super T, ? extends ValueHolder<? extends U>> mapper) {
    Objects.requireNonNull(mapper);
    @SuppressWarnings("unchecked")
    final ValueHolder<U> r = (ValueHolder<U>)mapper.apply(getValue());
    if (r == null) {
      return empty();
    } else {
      return r;
    }
  }

  default ValueHolder<T> orDefault(final Supplier<? extends T> supplier) {
    return this;
  }

  default ValueHolder<T> orDefault(final T defaultValue) {
    return this;
  }

  default ValueHolder<T> orDefaultValueHolder(
    final Supplier<? extends ValueHolder<? extends T>> defaultValue) {
    return this;
  }

  default ValueHolder<T> orDefaultValueHolder(final ValueHolder<T> defaultValue) {
    return this;
  }

  default ValueHolder<T> orThrow() {
    return this;
  }

  default <X extends Throwable> ValueHolder<T> orThrow(
    final Supplier<? extends X> exceptionSupplier) throws X {
    return this;
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

  default Stream<T> stream() {
    return Stream.of(getValue());
  }

  default ValueHolder<T> tap(final Consumer<? super T> action) {
    action.accept(getValue());
    return this;
  }

  default <V> ValueHolder<V> then(final Function<T, V> converter) {
    assert converter != null;
    return () -> {
      final var value = getValue();
      if (value == null) {
        return null;
      } else {
        return converter.apply(value);
      }
    };
  }

  default Optional<T> toOptional() {
    return Optional.of(getValue());
  }

}
