package com.revolsys.collection.map;

import java.util.concurrent.ConcurrentHashMap;

import com.revolsys.collection.iterator.BaseIterable;
import com.revolsys.collection.iterator.Iterables;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

public class MapTree<V> {

  private final ConcurrentHashMap<String, MapTree<V>> mapByKey = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, ListEx<V>> listByKey = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, V> valueByKey = new ConcurrentHashMap<>();

  public MapTree<V> addValue(final String key, final V value) {
    this.valueByKey.put(key, value);
    return this;
  }

  public ListEx<V> list(final String key) {
    return this.listByKey.computeIfAbsent(key, k -> Lists.newArray());
  }

  public BaseIterable<String> listKeys() {
    return Iterables.fromIterable(this.mapByKey.keySet());
  }

  public MapTree<V> map(final String key) {
    return this.mapByKey.computeIfAbsent(key, k -> new MapTree<>());
  }

  public BaseIterable<String> mapKeys() {
    return Iterables.fromIterable(this.mapByKey.keySet());
  }

  public V value(final String key) {
    return this.valueByKey.get(key);
  }

  public BaseIterable<String> valueKeys() {
    return Iterables.fromIterable(this.mapByKey.keySet());
  }

  public BaseIterable<V> values() {
    return Iterables.fromIterable(this.valueByKey.values());
  }
}
