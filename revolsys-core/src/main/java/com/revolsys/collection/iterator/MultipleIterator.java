package com.revolsys.collection.iterator;

import java.util.Collections;
import java.util.Iterator;

public class MultipleIterator<V> extends BaseIterator<V> {

  private Iterator<V> iterator = Collections.emptyIterator();

  private final Iterator<Iterable<V>> sourceIterator;

  public MultipleIterator(final Iterable<Iterable<V>> iterables) {
    this.sourceIterator = iterables.iterator();
  }

  @Override
  protected boolean hasNextDo() {
    while (true) {
      while (this.iterator.hasNext()) {
        this.value = this.iterator.next();
        if (this.value != null) {
          return true;
        }
      }
      if (this.sourceIterator.hasNext()) {
        final Iterable<V> iterable = this.sourceIterator.next();
        if (iterable != null) {
          this.iterator = iterable.iterator();
          if (this.iterator == null) {
            this.iterator = Collections.emptyIterator();
          }
        }
      } else {
        return false;
      }
    }
  }

}
