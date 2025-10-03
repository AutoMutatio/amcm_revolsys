package com.revolsys.collection.value;

import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class Single<T> implements ValueHolder<T> {
  private static class SingleEmpty<V> extends Single<V> {

    @Override
    public boolean equals(final Object obj) {
      return this == obj;
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
    public V getOrNull() {
      return null;
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

  private static class SingleEmptyExpiry<V> extends SingleEmpty<V> {
    private final long expireTime;

    public SingleEmptyExpiry(final long expireTime) {
      this.expireTime = expireTime;
    }

    @Override
    public boolean isExpired() {
      return System.currentTimeMillis() >= this.expireTime;
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

    @SuppressWarnings("unchecked")
    @Override
    public <R extends V> R getOrDefault(final Supplier<R> supplier) {
      return (R)this.value;
    }

    @Override
    public V getOrDefault(final V other) {
      return this.value;
    }

    @Override
    public V getOrNull() {
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
      return Objects.toString(this.value);
    }

  }

  private static final Single<?> EMPTY = new SingleEmpty<>();

  @SuppressWarnings("unchecked")
  public static <T> Single<T> empty() {
    return (Single<T>)EMPTY;
  }

  public static <T> Single<T> emptyExpiry(final Duration duration) {
    final var instant = Instant.now()
      .plus(duration);
    return emptyExpiry(instant);
  }

  public static <T> Single<T> emptyExpiry(final Instant instant) {
    final var millis = instant.toEpochMilli();
    return new SingleEmptyExpiry<T>(millis);
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

  Single() {
  }

  @Override
  public Single<T> filter(final Predicate<? super T> predicate) {
    return this;
  }

  @Override
  public abstract T get();

  public <OUT> OUT get(final Function<Single<T>, OUT> mapper) {
    return mapper.apply(this);
  }

  @Override
  public abstract <R extends T> R getOrDefault(final Supplier<R> supplier);

  @Override
  public abstract T getOrDefault(final T other);

  public abstract T getOrNull();

  @Override
  public abstract T getOrThrow();

  @Override
  public abstract <X extends Throwable> T getOrThrow(final Supplier<? extends X> exceptionSupplier)
    throws X;

  @Override
  public T getValue() {
    return get();
  }

  @Override
  public void ifPresent(final Consumer<? super T> action) {
  }

  @Override
  public abstract void ifPresentOrElse(final Consumer<? super T> action,
    final Runnable emptyAction);

  @Override
  public abstract boolean isEmpty();

  @Override
  public boolean isExpired() {
    return false;
  }

  @Override
  public boolean isPresent() {
    return false;
  }

  @Override
  public abstract <U> Single<U> map(final Function<? super T, ? extends U> mapper);

  public abstract <U> Single<U> mapSingle(
    final Function<? super T, ? extends Single<? extends U>> mapper);

  @Override
  public abstract Single<T> orDefault(final Supplier<? extends T> defaultValue);

  @Override
  public abstract Single<T> orDefault(final T defaultValue);

  public abstract Single<T> orDefaultSingle(final Single<T> defaultValue);

  public abstract Single<T> orDefaultSingle(
    final Supplier<? extends Single<? extends T>> defaultValue);

  @Override
  public abstract Single<T> orThrow();

  @Override
  public abstract <X extends Throwable> Single<T> orThrow(
    final Supplier<? extends X> exceptionSupplier) throws X;

  @Override
  public abstract Stream<T> stream();

  @Override
  public Single<T> tap(final Consumer<? super T> action) {
    return this;
  }

  @Override
  public abstract Optional<T> toOptional();

  @Override
  public String toString() {
    return "empty";
  }
}
