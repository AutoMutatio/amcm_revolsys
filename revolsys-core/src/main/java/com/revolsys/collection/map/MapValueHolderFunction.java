package com.revolsys.collection.map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.revolsys.collection.value.ValueHolder;
import com.revolsys.exception.Exceptions;
import com.revolsys.parallel.ExecutorServiceFactory;
import com.revolsys.util.BaseCloseable;

public class MapValueHolderFunction<K, V> implements ValueHolder<V> {

  public static <K2, V2> ValueHolder<V2> computeIfAbsent(
    final ConcurrentHashMap<K2, ValueHolder<V2>> map, final K2 key,
    final Function<K2, V2> initializer) {
    return map.computeIfAbsent(key, function(initializer));
  }

  public static <K2, V2> Function<K2, MapValueHolderFunction<K2, V2>> function(
    final Function<K2, V2> initializer) {
    return key -> new MapValueHolderFunction<>(key, initializer);
  }

  private final Function<K, V> initializer;

  private final CountDownLatch latch = new CountDownLatch(1);

  private boolean closed;

  private V value;

  private final K key;

  private Future<?> task;

  public MapValueHolderFunction(final K key, final Function<K, V> initializer) {
    this(key, null, initializer);
  }

  public MapValueHolderFunction(final K key, final V initialValue,
    final Function<K, V> initializer) {
    this.key = key;
    this.value = initialValue;
    this.initializer = initializer;
    if (this.value == null) {
      this.task = ExecutorServiceFactory.getExecutorService()
        .submit(this::initialize);
    } else {
      this.latch.countDown();
    }
  }

  @Override
  public void close() {
    this.task.cancel(true);
    this.closed = true;
    final var value = this.value;
    this.value = null;
    BaseCloseable.closeValueSilent(value);
  }

  @Override
  public V getValue() {
    try {
      this.latch.await();
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return this.value;
  }

  private void initialize() {
    this.value = this.initializer.apply(this.key);
    if (this.closed) {
      close();
    }
    this.latch.countDown();
  }

  @Override
  public boolean isEmpty() {
    return this.value == null;
  }

  @Override
  public String toString() {
    if (this.value == null) {
      return null;
    } else {
      return this.value.toString();
    }
  }
}
