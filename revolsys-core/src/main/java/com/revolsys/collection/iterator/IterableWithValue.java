package com.revolsys.collection.iterator;

import java.util.Iterator;

public class IterableWithValue<V> {
  private final Iterator<V> iterator;

  private V value;

  public IterableWithValue(Iterable<V> iterable) {
    super();
    this.iterator = iterable.iterator();
    next();
  }

  public boolean hasValue() {
    return this.value != null;
  }

  public boolean next() {
    while (this.iterator.hasNext()) {
      this.value = this.iterator.next();
      if (this.value != null) {
        return true;
      }
    }
    this.value = null;
    return false;
  }

  public V value() {
    return this.value;
  }
}
