package com.revolsys.collection.value;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class Single<T> {
  private static class SingleEmpty<V> extends Single<V> {

    @Override
    public boolean equals(final Object obj) {
      return this == obj;
    }

    @Override
    public Single<V> filter(final Predicate<? super V> predicate) {
      return this;
    }

    @Override
    public V get() {
      throw new NoSuchElementException("No value present");
    }

    @Override
    public V getOrDefault(final Supplier<? extends V> supplier) {
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
    public <U> Single<U> map(final Function<? super V, ? extends U> mapper) {
      return empty();
    }

    @Override
    public <U> Single<U> mapSingle(
      final Function<? super V, ? extends Single<? extends U>> mapper) {
      return empty();
    }

    @Override
    public Single<V> orDefault(final Supplier<? extends V> supplier) {
      return ofNullable(supplier.get());
    }

    @Override
    public Single<V> orDefault(final V defaultValue) {
      return ofNullable(defaultValue);
    }

    @Override
    public Single<V> orDefaultSingle(final Single<V> defaultValue) {
      return defaultValue;
    }

    @Override
    public Single<V> orDefaultSingle(final Supplier<? extends Single<? extends V>> supplier) {
      @SuppressWarnings("unchecked")
      final var result = (Single<V>)supplier.get();
      if (result == null) {
        return empty();
      } else {
        return result;
      }
    }

    @Override
    public Single<V> orThrow() {
      throw new NoSuchElementException("No value present");
    }

    @Override
    public <X extends Throwable> Single<V> orThrow(final Supplier<? extends X> exceptionSupplier)
      throws X {
      throw exceptionSupplier.get();
    }

    @Override
    public Stream<V> stream() {
      return Stream.empty();
    }

    @Override
    public Single<V> tap(final Consumer<? super V> action) {
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

  private static class SingleValue<V> extends Single<V> {

    private final V value;

    public SingleValue(final V value) {
      super();
      this.value = value;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      return obj instanceof final SingleValue<?> other && Objects.equals(this.value, other.value);
    }

    @Override
    public Single<V> filter(final Predicate<? super V> predicate) {
      return predicate.test(this.value) ? this : empty();
    }

    @Override
    public V get() {
      return this.value;
    }

    @Override
    public V getOrDefault(final Supplier<? extends V> supplier) {
      return this.value;
    }

    @Override
    public V getOrDefault(final V other) {
      return this.value;
    }

    @Override
    public V getOrThrow() {
      return this.value;
    }

    @Override
    public <X extends Throwable> V getOrThrow(final Supplier<? extends X> exceptionSupplier)
      throws X {
      return this.value;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(this.value);
    }

    @Override
    public void ifPresent(final Consumer<? super V> action) {
      action.accept(this.value);
    }

    @Override
    public void ifPresentOrElse(final Consumer<? super V> action, final Runnable emptyAction) {
      action.accept(this.value);
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean isPresent() {
      return true;
    }

    @Override
    public <U> Single<U> map(final Function<? super V, ? extends U> mapper) {
      return Single.ofNullable(mapper.apply(this.value));
    }

    @Override
    public <U> Single<U> mapSingle(
      final Function<? super V, ? extends Single<? extends U>> mapper) {
      Objects.requireNonNull(mapper);
      @SuppressWarnings("unchecked")
      final Single<U> r = (Single<U>)mapper.apply(this.value);
      if (r == null) {
        return empty();
      } else {
        return r;
      }
    }

    @Override
    public Single<V> orDefault(final Supplier<? extends V> supplier) {
      return this;
    }

    @Override
    public Single<V> orDefault(final V defaultValue) {
      return this;
    }

    @Override
    public Single<V> orDefaultSingle(final Single<V> defaultValue) {
      return this;
    }

    @Override
    public Single<V> orDefaultSingle(final Supplier<? extends Single<? extends V>> defaultValue) {
      return this;
    }

    @Override
    public Single<V> orThrow() {
      return this;
    }

    @Override
    public <X extends Throwable> Single<V> orThrow(final Supplier<? extends X> exceptionSupplier)
      throws X {
      return this;
    }

    @Override
    public Stream<V> stream() {
      return Stream.of(this.value);
    }

    @Override
    public Single<V> tap(final Consumer<? super V> action) {
      action.accept(this.value);
      return this;
    }

    @Override
    public Optional<V> toOptional() {
      return Optional.of(this.value);
    }

    @Override
    public String toString() {
      return null;
    }

  }

  private static final Single<?> EMPTY = new SingleEmpty<>();

  @SuppressWarnings("unchecked")
  public static <T> Single<T> empty() {
    return (Single<T>)EMPTY;
  }

  public static <T> Single<T> of(final T value) {
    if (value == null) {
      throw new NullPointerException("A Single value must not be null");
    } else {
      return new SingleValue<>(value);
    }
  }

  public static <T> Single<T> ofNullable(final T value) {
    if (value == null) {
      return empty();
    } else {
      return new SingleValue<>(value);
    }
  }

  private Single() {
  }

  public abstract Single<T> filter(final Predicate<? super T> predicate);

  public abstract T get();

  public abstract T getOrDefault(final Supplier<? extends T> supplier);

  public abstract T getOrDefault(final T other);

  public abstract T getOrThrow();

  public abstract <X extends Throwable> T getOrThrow(final Supplier<? extends X> exceptionSupplier)
    throws X;

  public abstract void ifPresent(final Consumer<? super T> action);

  public abstract void ifPresentOrElse(final Consumer<? super T> action,
    final Runnable emptyAction);

  public abstract boolean isEmpty();

  public abstract boolean isPresent();

  public abstract <U> Single<U> map(final Function<? super T, ? extends U> mapper);

  public abstract <U> Single<U> mapSingle(
    final Function<? super T, ? extends Single<? extends U>> mapper);

  public abstract Single<T> orDefault(final Supplier<? extends T> defaultValue);

  public abstract Single<T> orDefault(final T defaultValue);

  public abstract Single<T> orDefaultSingle(final Single<T> defaultValue);

  public abstract Single<T> orDefaultSingle(
    final Supplier<? extends Single<? extends T>> defaultValue);

  public abstract Single<T> orThrow();

  public abstract <X extends Throwable> Single<T> orThrow(
    final Supplier<? extends X> exceptionSupplier) throws X;

  public abstract Stream<T> stream();

  public abstract Single<T> tap(Consumer<? super T> action);

  public abstract Optional<T> toOptional();

  @Override
  public String toString() {
    return "empty";
  }
}
