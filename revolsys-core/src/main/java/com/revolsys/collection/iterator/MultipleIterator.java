package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class MultipleIterator<V> extends AbstractIterator<V> {

  private Iterator<V> iterator;

  private final Iterator<Iterable<V>> sourceIterator;

  public MultipleIterator(final Iterable<Iterable<V>> iterables) {
    this.sourceIterator = iterables.iterator();
  }

  @Override
  protected V getNext() throws NoSuchElementException {
    while (this.iterator == null || !this.iterator.hasNext()) {
      if (this.sourceIterator.hasNext()) {
        this.iterator = this.sourceIterator.next().iterator();
      } else {
        throw new NoSuchElementException();
      }
    }
    return this.iterator.next();
  }

}
