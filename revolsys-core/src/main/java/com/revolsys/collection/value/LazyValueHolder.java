package com.revolsys.collection.value;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.revolsys.util.BaseCloseable;

public class LazyValueHolder<T> extends SimpleValueHolder<T> implements BaseCloseable {
  private Supplier<T> valueSupplier;

  private final ReentrantLock lock = new ReentrantLock();

  private boolean initialized = false;

  protected LazyValueHolder() {
  }

  public LazyValueHolder(final Supplier<T> valueSupplier) {
    setValueSupplier(valueSupplier);
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
      if (this.initialized) {
        final var value = getValue();
        clear();
        if (value instanceof final Closeable close) {
          close.close();
        }
      }
    } catch (final IOException e) {
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public T getValue() {
    this.lock.lock();
    try {
      if (this.valueSupplier != null && !this.initialized) {
        this.initialized = true;
        final T value = this.valueSupplier.get();
        super.setValue(value);
        return value;
      }
    } finally {
      this.lock.unlock();
    }

    return super.getValue();
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
