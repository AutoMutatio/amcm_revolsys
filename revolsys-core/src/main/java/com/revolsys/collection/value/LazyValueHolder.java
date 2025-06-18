package com.revolsys.collection.value;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.revolsys.exception.Exceptions;
import com.revolsys.predicate.Predicates;
import com.revolsys.util.BaseCloseable;

public class LazyValueHolder<T> implements ValueHolder<T>, BaseCloseable {
  public static class Builder<V> {
    private final LazyValueHolder<V> holder = new LazyValueHolder<>();

    public LazyValueHolder<V> build() {
      return this.holder;
    }

    public Builder<V> cacheTime(final Duration duration) {
      final var expireTime = new AtomicReference<>(Instant.MAX);
      this.holder.validator = v -> Instant.now()
        .isBefore(expireTime.get());
      this.holder.loadCallback = b -> expireTime.set(Instant.now()
        .plus(duration));
      return this;
    }

    public Builder<V> loadCallback(final Consumer<V> loadCallback) {
      this.holder.loadCallback = loadCallback;
      return this;
    }

    public Builder<V> validator(final Predicate<V> validator) {
      if (validator == null) {
        this.holder.validator = Predicates.all();
      } else {
        this.holder.validator = validator;
      }
      return this;
    }

    public Builder<V> valueRefresh(final Function<V, V> valueRefresh) {
      this.holder.valueRefresh = valueRefresh;
      return this;
    }

    public Builder<V> valueSupplier(final Supplier<V> valueSupplier) {
      this.holder.valueSupplier = valueSupplier;
      return this;
    }
  }

  private class ReloadValueReference<V> implements ValueReference<V> {

    private V value;

    private boolean loaded;

    private Instant timestamp;

    private final CountDownLatch latch = new CountDownLatch(1);

    private boolean closed;

    @Override
    public boolean awaitLoad() {
      if (this.closed) {
        return false;
      }
      try {
        this.latch.await();
      } catch (final InterruptedException e) {
        throw Exceptions.toRuntimeException(e);
      }
      return true;
    }

    @Override
    public void closeIfNotEqual(final V value) {
      if (!this.closed) {
        this.closed = true;
        if (value != this.value) {
          BaseCloseable.closeValueSilent(value);
        }
        this.value = null;
      }
    }

    @Override
    public V getValue() {
      return this.value;
    }

    @Override
    public boolean isLoaded() {
      return this.loaded;
    }

    public V setValue(final V value) {
      if (this.closed) {
        return null;
      }
      this.value = value;
      this.loaded = true;
      this.timestamp = Instant.now();
      this.latch.countDown();
      return value;
    }

    @Override
    public Instant timestamp() {
      return this.timestamp;
    }

  }

  private interface ValueReference<V> {
    default boolean awaitLoad() {
      return false;
    }

    default void cancel() {
    }

    default void closeIfNotEqual(final V value) {
    }

    default V getValue() {
      return null;
    }

    default boolean isLoaded() {
      return false;
    }

    default Instant timestamp() {
      return Instant.MIN;
    }
  }

  private static final ValueReference<?> EMPTY = new ValueReference<>() {};

  @SuppressWarnings("unchecked")
  public static <V> ValueReference<V> empty() {
    return (ValueReference<V>)EMPTY;
  }

  private Supplier<T> valueSupplier;

  private Function<T, T> valueRefresh;

  private Predicate<T> validator = Predicates.all();

  private final AtomicReference<ValueReference<T>> valueRef = new AtomicReference<>(empty());

  private Consumer<T> loadCallback = v -> {
  };

  protected LazyValueHolder() {
    this(null);
  }

  public LazyValueHolder(final Supplier<T> valueSupplier) {
    this.valueSupplier = valueSupplier;
  }

  public void clear() {
    final var oldRef = this.valueRef.getAndUpdate(old -> empty());
    final var value = oldRef.getValue();
    oldRef.cancel();
    BaseCloseable.closeValue(value);
  }

  @Override
  public void close() {
    this.valueRefresh = null;
    this.valueSupplier = null;
    this.clear();
  }

  @Override
  public T getValue() {
    final var ref = getValueReference();
    if (ref == null) {
      return null;
    } else {
      return ref.getValue();
    }
  }

  private ValueReference<T> getValueReference() {
    if (this.valueSupplier == null && this.valueRefresh == null) {
      return null;
    }
    while (true) {
      final var ref = this.valueRef.get();
      if (ref.awaitLoad()) {
        final var value = ref.getValue();
        if (this.validator.test(value)) {
          return ref;
        }
      }
      final var updateRef = refreshDo(ref);
      if (updateRef != null) {
        return updateRef;
      }
    }
  }

  public boolean isInitialized() {
    return this.valueRef.get()
        .isLoaded();
  }

  public void refresh() {
    final var ref = this.valueRef.get();
    refreshDo(ref);
  }

  protected ValueReference<T> refreshDo(final ValueReference<T> ref) {
    final var updateRef = new ReloadValueReference<T>();
    if (this.valueRef.compareAndSet(ref, updateRef)) {
      T value = ref.getValue();
      if (this.valueRefresh == null) {
        value = this.valueSupplier.get();
      } else {
        value = this.valueRefresh.apply(value);
      }
      this.loadCallback.accept(value);
      updateRef.setValue(value);
      return updateRef;
    } else {
      return null;
    }
  }

  @Override
  public T setValue(final T value) {
    final var ref = this.valueRef.get();
    final var updateRef = new ReloadValueReference<T>();
    if (this.valueRef.compareAndSet(ref, updateRef)) {
      ref.closeIfNotEqual(value);
      this.loadCallback.accept(value);
      return updateRef.setValue(value);
    } else {
      return this.valueRef.get()
          .getValue();
    }
  }

  protected void setValueSupplier(final Supplier<T> valueSupplier) {
    this.valueSupplier = valueSupplier;
  }

  @Override
  public <V> ValueHolder<V> then(final Function<T, V> converter) {
    final var timestamp = new AtomicReference<>(Instant.MIN);
    final Predicate<V> validator = v -> {
      final var valueTimestamp = getValueReference().timestamp();
      final var cacheTimestamp = timestamp.get();
      return valueTimestamp
          .equals(cacheTimestamp);
    };
    final Supplier<V> supplier = () -> {
      final var ref = getValueReference();
      final var value = getValue();
      timestamp.set(ref.timestamp());
      return converter.apply(value);
    };
    return new Builder<V>().validator(validator)
        .valueSupplier(supplier)
        .build();
  }

  @Override
  public String toString() {
    final var ref = this.valueRef.get();
    if (ref == EMPTY) {
      return "empty";
    } else if (!ref.isLoaded()) {
      return "loading";
    } else {
      final T value = ref.getValue();
      if (value == null) {
        return "null";
      } else {
        return value.toString();
      }
    }
  }
}
