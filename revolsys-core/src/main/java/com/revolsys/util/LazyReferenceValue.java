package com.revolsys.util;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import java.util.function.Supplier;

public class LazyReferenceValue<V> implements ValueWrapper<V> {

  public interface Ref<V> {
    V get();
  }

  public static <T> LazyReferenceValue<T> of(final Supplier<T> supplier) {
    final Function<T, Ref<T>> referenceConstructor = value -> () -> value;
    return new LazyReferenceValue<>(referenceConstructor, supplier);
  }

  public static <T> LazyReferenceValue<T> soft(final Supplier<T> supplier) {
    final Function<T, Ref<T>> referenceConstructor = value -> {
      final Reference<T> ref = new SoftReference<>(value);
      return () -> ref.get();
    };
    return new LazyReferenceValue<>(referenceConstructor, supplier);
  }

  public static <T> LazyReferenceValue<T> weak(final Supplier<T> supplier) {
    final Function<T, Ref<T>> referenceConstructor = value -> {
      final Reference<T> ref = new WeakReference<>(value);
      return () -> ref.get();
    };
    return new LazyReferenceValue<>(referenceConstructor, supplier);
  }

  private Ref<V> reference;

  private final Function<V, Ref<V>> referenceConstructor;

  private Instant expireTime = Instant.MAX;

  private Duration expireDuration;

  private Supplier<V> supplier;

  public LazyReferenceValue(final Function<V, Ref<V>> referenceConstructor,
    final Supplier<V> supplier) {
    this.referenceConstructor = referenceConstructor;
    this.supplier = supplier;
  }

  public synchronized void clearValue() {
    this.reference = null;
  }

  @Override
  public synchronized void close() {
    this.reference = null;
    this.supplier = null;
  }

  @Override
  public synchronized V getValue() {
    V value = null;
    if (Instant.now().isBefore(this.expireTime)) {
      if (this.reference != null) {
        value = this.reference.get();

      }
    }
    if (value == null) {
      this.reference = null;
    }

    final Supplier<V> supplier = this.supplier;
    if (value == null && supplier != null) {
      value = supplier.get();
      if (value != null) {
        if (this.expireDuration == null) {
          this.expireTime = Instant.MAX;
        } else {
          this.expireTime = Instant.now().plus(this.expireDuration);
        }
        this.reference = this.referenceConstructor.apply(value);
      }
    }
    return value;
  }

  public LazyReferenceValue<V> setExpireDuration(final Duration expireDuration) {
    this.expireDuration = expireDuration;
    return this;
  }

  @Override
  public String toString() {
    Object value = null;
    if (this.reference != null) {
      value = this.reference.get();
    }
    if (value == null) {
      return "null";
    } else {
      return value.toString();
    }
  }

}
