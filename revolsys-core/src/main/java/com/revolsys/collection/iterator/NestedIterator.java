package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class NestedIterator<O, V> extends AbstractIterator<V> {

  private Iterator<V> iterator;

  private final Iterator<O> outerIterator;

  private final Function<O, BaseIterable<V>> iteratorConstructor;

  public NestedIterator(final Iterable<O> outer,
    final Function<O, BaseIterable<V>> iteratorConstructor) {
    this.outerIterator = outer.iterator();
    this.iteratorConstructor = iteratorConstructor;
  }

  @Override
  protected V getNext() throws NoSuchElementException {
    while (this.iterator == null || !this.iterator.hasNext()) {
      this.iterator = null;
      if (this.outerIterator.hasNext()) {
        final var outerValue = this.outerIterator.next();
        if (outerValue != null) {
          final var iterable = this.iteratorConstructor.apply(outerValue);
          if (iterable != null) {
            this.iterator = iterable.iterator();
          }
        }
      } else {
        throw new NoSuchElementException();
      }
    }
    return this.iterator.next();
  }

}
