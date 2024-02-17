package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SingleIterator<V> implements Iterator<V> {

  private V value;

  SingleIterator(final V value) {
    this.value = value;
  }

  @Override
  public boolean hasNext() {
    return this.value != null;
  }

  @Override
  public V next() throws NoSuchElementException {
    final V value = this.value;
    this.value = null;
    if (value == null) {
      throw new NoSuchElementException();
    } else {
      return value;
    }
  }

}
