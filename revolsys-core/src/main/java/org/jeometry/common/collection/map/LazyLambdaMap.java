package org.jeometry.common.collection.map;

import java.util.LinkedHashMap;
import java.util.function.Function;

public class LazyLambdaMap<K, V> extends LinkedHashMap<K, V> {
  private static final long serialVersionUID = 1L;

  private final Function<K, V> loadFunction;

  public LazyLambdaMap(final Function<K, V> loadFunction) {
    this.loadFunction = loadFunction;
  }

  @SuppressWarnings("unchecked")
  @Override
  public V get(final Object key) {
    if (!containsKey(key)) {
      final V value = this.loadFunction.apply((K)key);
      put((K)key, value);
      return value;
    }
    return super.get(key);
  }

}
