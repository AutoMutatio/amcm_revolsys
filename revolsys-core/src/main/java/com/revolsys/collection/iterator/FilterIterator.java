package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.function.Predicate;

import com.revolsys.util.BaseCloseable;

final class FilterIterator<T> extends BaseIterator<T> {

  private final Predicate<? super T> filter;

  private final Iterator<T> iterator;

  public FilterIterator(final Predicate<? super T> filter, final Iterator<T> iterator) {
    this.filter = filter;
    this.iterator = iterator;
  }

  @Override
  public void close() {
    super.close();
    BaseCloseable.closeValue(this.iterator);
  }

  @Override
  protected boolean hasNextDo() {
    while (this.iterator.hasNext()) {
      this.value = this.iterator.next();
      if (this.value != null && this.filter.test(this.value)) {
        return true;
      }
    }
    return false;
  }

}
