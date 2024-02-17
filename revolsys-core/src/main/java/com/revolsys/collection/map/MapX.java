package com.revolsys.collection.map;

public interface MapX<K, V> extends MapDefault<K, K, V, MapX<K, V>> {

  @Override
  default K toK(final K key) {
    return key;
  }
}
