package com.revolsys.collection.value;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.revolsys.predicate.Predicates;
import com.revolsys.util.BaseCloseable;

public class LazyValueHolder<T> extends SimpleValueHolder<T> implements BaseCloseable {
  private Supplier<T> valueSupplier;
  private Function<T, T> valueRefresh;
  private Predicate<T> validator;

  private final ReentrantLock lock = new ReentrantLock();

  private boolean initialized = false;

  protected LazyValueHolder() {
    this(null);
  }

  public LazyValueHolder(final Function<T, T> valueRefresh, final Predicate<T> validator) {
    super();
    this.valueRefresh = valueRefresh;
    this.validator = validator != null ? validator : Predicates.all();
  }

  public LazyValueHolder(final Supplier<T> valueSupplier) {
    this(valueSupplier, null);
  }

  public LazyValueHolder(final Supplier<T> valueSupplier, final Predicate<T> validator) {
    this.valueSupplier = valueSupplier;
    this.validator = validator != null ? validator : Predicates.all();
  }

  public void clear() {
    this.lock.lock();
    try {
      this.initialized = false;
      setValue(null);
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public void close() {
    this.lock.lock();
    try {
      final var value = getValue();
      clear();
      if (value instanceof final Closeable close) {
        close.close();
      }
      this.initialized = false;
    } catch (final IOException e) {
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public T getValue() {
    T value = super.getValue();
    if (value == null || !this.validator.test(value)) {
      this.lock.lock();
      try {
        value = super.getValue();
        if (!this.initialized || !this.validator.test(value)) {
          if (this.valueRefresh == null) {
            value = this.valueSupplier.get();
          } else {
            value = this.valueRefresh.apply(value);
          }
          this.initialized = true;
          super.setValue(value);
        }
      } finally {
        this.lock.unlock();
      }
    }
    return value;
  }

  public boolean isInitialized() {
    return this.initialized;
  }

  public void refresh() {
    clear();
    getValue();
  }

  @Override
  public T setValue(final T value) {
    throw new UnsupportedOperationException("Value cannot be changed");
  }

  protected void setValueSupplier(final Supplier<T> valueSupplier) {
    this.valueSupplier = valueSupplier;
  }
}
