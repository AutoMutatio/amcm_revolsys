package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import com.revolsys.util.BaseCloseable;

class FilterIterator<T> implements Iterator<T>, BaseCloseable {

  private final Predicate<? super T> filter;

  private final Iterator<T> iterator;

  private boolean hasNext = true;

  private boolean loadNext = true;

  private T value;

  public FilterIterator(final Predicate<? super T> filter, final Iterator<T> iterator) {
    this.filter = filter;
    this.iterator = iterator;
  }

  @Override
  public final void close() {
    this.hasNext = false;
    BaseCloseable.closeValue(this.iterator);
  }

  @Override
  public final boolean hasNext() {
    if (this.loadNext) {
      while (this.iterator.hasNext()) {
        this.value = this.iterator.next();
        if (this.filter.test(this.value)) {
          break;
        } else {
          this.value = null;
        }
      }
      if (this.value == null) {
        close();
      }
      this.loadNext = false;
    }
    return this.hasNext;
  }

  @Override
  public final T next() {
    if (hasNext()) {
      final T value = this.value;
      this.loadNext = true;
      return value;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
