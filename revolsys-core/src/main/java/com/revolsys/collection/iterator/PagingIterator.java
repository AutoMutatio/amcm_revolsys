package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class PagingIterator<V> extends AbstractIterator<V> {

  private Iterator<V> iterator;

  private final Supplier<Iterable<V>> supplier;

  public PagingIterator(final Supplier<Iterable<V>> supplier) {
    this.supplier = supplier;
  }

  @Override
  protected V getNext() throws NoSuchElementException {
    while (this.iterator == null || !this.iterator.hasNext()) {
      final Iterable<V> iterable = this.supplier.get();
      if (iterable == null) {
        throw new NoSuchElementException();
      } else {
        this.iterator = iterable.iterator();
      }
    }
    return this.iterator.next();
  }

}
