package com.revolsys.collection.map;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class LazyValueMap<K, V> extends DelegatingMap<K, V> {

  private final Function<K, V> loadFunction;

  private Map<K, V> map;

  private Map<K, V> externalMap;

  private final ReentrantLock lock = new ReentrantLock();

  public LazyValueMap(final Function<K, V> loadFunction) {
    this(new LinkedHashMap<>(), loadFunction);
  }

  public LazyValueMap(final Map<K, V> map, final Function<K, V> loadFunction) {
    this.map = map;
    this.externalMap = Collections.unmodifiableMap(map);
    this.loadFunction = loadFunction;
  }

  @SuppressWarnings("unchecked")
  @Override
  public V get(final Object key) {
    this.lock.lock();
    try {
      if (!this.map.containsKey(key)) {
        final V value = this.loadFunction.apply((K)key);
        this.map.put((K)key, value);
        return value;
      }
      return this.map.get(key);
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  public Map<K, V> getMap() {
    return this.externalMap;
  }

  @Override
  public V put(final K key, final V value) {
    this.lock.lock();
    try {
      return this.map.put(key, value);
    } finally {
      this.lock.unlock();
    }
  }

}
