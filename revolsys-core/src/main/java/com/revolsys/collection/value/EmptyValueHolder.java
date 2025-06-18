package com.revolsys.collection.value;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class EmptyValueHolder<V> implements ValueHolder<V> {
  public static final EmptyValueHolder<?> EMPTY = new EmptyValueHolder<>();

  private EmptyValueHolder() {
  }

  @Override
  public boolean equals(final Object obj) {
    return this == obj;
  }

  @Override
  public ValueHolder<V> filter(final Predicate<? super V> predicate) {
    return this;
  }

  @Override
  public V get() {
    throw new NoSuchElementException("No value present");
  }

  @Override
  public <R extends V> R getOrDefault(final Supplier<R> supplier) {
    return supplier.get();
  }

  @Override
  public V getOrDefault(final V other) {
    return other;
  }

  @Override
  public V getOrThrow() {
    throw new NoSuchElementException("No value present");
  }

  @Override
  public <X extends Throwable> V getOrThrow(final Supplier<? extends X> exceptionSupplier)
    throws X {
    throw exceptionSupplier.get();
  }

  @Override
  public V getValue() {
    throw new NoSuchElementException("No value present");
  }

  @Override
  public int hashCode() {
    return -1;
  }

  @Override
  public void ifPresent(final Consumer<? super V> action) {
  }

  @Override
  public void ifPresentOrElse(final Consumer<? super V> action, final Runnable emptyAction) {
    emptyAction.run();
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public boolean isPresent() {
    return false;
  }

  @Override
  public <U> ValueHolder<U> map(final Function<? super V, ? extends U> mapper) {
    return ValueHolder.empty();
  }

  @Override
  public <U> ValueHolder<U> mapValueHolder(
    final Function<? super V, ? extends ValueHolder<? extends U>> mapper) {
    return ValueHolder.empty();
  }

  @Override
  public ValueHolder<V> orDefault(final Supplier<? extends V> supplier) {
    return ValueHolder.ofNullable(supplier.get());
  }

  @Override
  public ValueHolder<V> orDefault(final V defaultValue) {
    return ValueHolder.ofNullable(defaultValue);
  }

  @Override
  public ValueHolder<V> orDefaultValueHolder(
    final Supplier<? extends ValueHolder<? extends V>> supplier) {
    @SuppressWarnings("unchecked")
    final var result = (ValueHolder<V>)supplier.get();
    if (result == null) {
      return ValueHolder.empty();
    } else {
      return result;
    }
  }

  @Override
  public ValueHolder<V> orDefaultValueHolder(final ValueHolder<V> defaultValue) {
    return defaultValue;
  }

  @Override
  public ValueHolder<V> orThrow() {
    throw new NoSuchElementException("No value present");
  }

  @Override
  public <X extends Throwable> ValueHolder<V> orThrow(final Supplier<? extends X> exceptionSupplier)
    throws X {
    throw exceptionSupplier.get();
  }

  @Override
  public Stream<V> stream() {
    return Stream.empty();
  }

  @Override
  public ValueHolder<V> tap(final Consumer<? super V> action) {
    return this;
  }

  @Override
  public Optional<V> toOptional() {
    return Optional.empty();
  }

  @Override
  public String toString() {
    return "empty";
  }

}
