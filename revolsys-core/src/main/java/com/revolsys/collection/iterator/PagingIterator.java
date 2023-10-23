package com.revolsys.collection.iterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Supplier;

import com.revolsys.util.BaseCloseable;

final class PagingIterator<V> extends BaseIterator<V> {

  private Iterator<V> iterator = Collections.emptyIterator();

  private final Supplier<Iterable<V>> supplier;

  public PagingIterator(final Supplier<Iterable<V>> supplier) {
    this.supplier = supplier;
  }

  @Override
  public final void close() {
    super.close();
    BaseCloseable.closeValue(this.iterator);
  }

  @Override
  protected final boolean hasNextDo() {
    while (true) {
      while (this.iterator.hasNext()) {
        this.value = this.iterator.next();
        if (this.value != null) {
          return true;
        }
      }
      final Iterable<V> iterable = this.supplier.get();
      if (iterable == null) {
        return false;
      } else {
        this.iterator = iterable.iterator();
      }
    }
  }

}
