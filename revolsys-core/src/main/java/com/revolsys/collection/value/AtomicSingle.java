package com.revolsys.collection.value;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class AtomicSingle<V> extends Single<V> {

  private final AtomicReference<V> valueReference = new AtomicReference<>();

  public AtomicSingle() {
  }

  public AtomicSingle(final V value) {
    super();
    this.valueReference.set(value);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    return obj instanceof final AtomicSingle<?> other
      && Objects.equals(getOrNull(), other.getOrNull());
  }

  @Override
  public Single<V> filter(final Predicate<? super V> predicate) {
    return predicate.test(getOrNull()) ? this : Single.empty();
  }

  @Override
  public V get() {
    final var value = this.valueReference.get();
    if (value == null) {
      throw new NoSuchElementException("No value present");
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R extends V> R getOrDefault(final Supplier<R> supplier) {
    final var value = getOrNull();
    if (value == null) {
      return supplier.get();
    }
    return (R)value;
  }

  @Override
  public V getOrDefault(final V other) {
    final var value = getOrNull();
    if (value == null) {
      return other;
    }
    return value;
  }

  @Override
  public V getOrNull() {
    return this.valueReference.get();
  }

  @Override
  public V getOrThrow() {
    final var value = getOrNull();
    if (value == null) {
      throw new NoSuchElementException("No value present");
    }
    return value;
  }

  @Override
  public <X extends Throwable> V getOrThrow(final Supplier<? extends X> exceptionSupplier)
    throws X {
    final var value = getOrNull();
    if (value == null) {
      throw exceptionSupplier.get();
    }
    return value;
  }

  @Override
  public void ifPresent(final Consumer<? super V> action) {
    final var value = getOrNull();
    if (value != null) {
      action.accept(value);
    }
  }

  @Override
  public void ifPresentOrElse(final Consumer<? super V> action, final Runnable emptyAction) {
    final var value = getOrNull();
    if (value == null) {
      emptyAction.run();
    } else {
      action.accept(value);
    }
  }

  @Override
  public boolean isEmpty() {
    return getOrNull() == null;
  }

  @Override
  public boolean isPresent() {
    return getOrNull() != null;
  }

  @Override
  public <U> Single<U> map(final Function<? super V, ? extends U> mapper) {
    final var value = getOrNull();
    if (value == null) {
      return empty();
    } else {
      return Single.ofNullable(mapper.apply(value));
    }
  }

  @Override
  public <U> Single<U> mapSingle(final Function<? super V, ? extends Single<? extends U>> mapper) {
    Objects.requireNonNull(mapper);
    final var value = getOrNull();
    if (value == null) {
      return empty();
    } else {
      @SuppressWarnings("unchecked")
      final Single<U> r = (Single<U>)mapper.apply(value);
      if (r == null) {
        return empty();
      } else {
        return r;
      }
    }
  }

  @Override
  public Single<V> orDefault(final Supplier<? extends V> supplier) {
    if (isEmpty()) {
      return Single.ofNullable(supplier.get());
    } else {
      return this;
    }
  }

  @Override
  public Single<V> orDefault(final V defaultValue) {
    if (isEmpty()) {
      return Single.ofNullable(defaultValue);
    } else {
      return this;
    }
  }

  @Override
  public Single<V> orDefaultSingle(final Single<V> defaultValue) {
    if (isEmpty()) {
      return defaultValue;
    } else {
      return this;
    }
  }

  @Override
  public Single<V> orDefaultSingle(final Supplier<? extends Single<? extends V>> supplier) {
    if (isEmpty()) {
      @SuppressWarnings("unchecked")
      final var result = (Single<V>)supplier.get();
      if (result == null) {
        return empty();
      } else {
        return result;
      }
    } else {
      return this;
    }
  }

  @Override
  public Single<V> orThrow() {
    if (isEmpty()) {
      throw new NoSuchElementException("No value present");
    } else {
      return this;
    }
  }

  @Override
  public <X extends Throwable> Single<V> orThrow(final Supplier<? extends X> exceptionSupplier)
    throws X {
    if (isEmpty()) {
      throw exceptionSupplier.get();
    } else {
      return this;
    }
  }

  public AtomicSingle<V> setIfEmpty(final Supplier<V> supplier) {
    this.valueReference.updateAndGet(v -> {
      if (v == null) {
        return supplier.get();
      } else {
        return v;
      }
    });
    return this;
  }

  public AtomicSingle<V> setIfEmpty(final V value) {
    this.valueReference.updateAndGet(v -> {
      if (v == null) {
        return value;
      } else {
        return v;
      }
    });
    return this;
  }

  @Override
  public Stream<V> stream() {
    if (isEmpty()) {
      return Stream.empty();
    } else {
      return Stream.of(getOrNull());
    }
  }

  @Override
  public AtomicSingle<V> tap(final Consumer<? super V> action) {
    if (!isEmpty()) {
      action.accept(getOrNull());
    }
    return this;
  }

  @Override
  public Optional<V> toOptional() {
    if (isEmpty()) {
      return Optional.empty();
    } else {
      final var value = getOrNull();
      return Optional.of(value);
    }
  }

  @Override
  public String toString() {
    return Objects.toString(this.valueReference.get());
  }
}
