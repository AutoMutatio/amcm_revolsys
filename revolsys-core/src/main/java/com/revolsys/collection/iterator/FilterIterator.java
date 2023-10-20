package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.function.Predicate;

import com.revolsys.util.BaseCloseable;

<<<<<<< HEAD
class FilterIterator<T> implements Iterator<T>, BaseCloseable {
=======
final class FilterIterator<T> extends BaseIterator<T> {
>>>>>>> upstream/main

  private final Predicate<? super T> filter;

  private final Iterator<T> iterator;
<<<<<<< HEAD

  private boolean hasNext = true;

  private boolean loadNext = true;

  private T value;
=======
>>>>>>> upstream/main

  public FilterIterator(final Predicate<? super T> filter, final Iterator<T> iterator) {
    this.filter = filter;
    this.iterator = iterator;
  }

  @Override
<<<<<<< HEAD
  public final void close() {
    this.hasNext = false;
=======
  public void close() {
    super.close();
>>>>>>> upstream/main
    BaseCloseable.closeValue(this.iterator);
  }

  @Override
<<<<<<< HEAD
  public final boolean hasNext() {
    if (this.loadNext) {
      while (this.iterator.hasNext()) {
        this.value = this.iterator.next();
        if (this.filter.test(this.value)) {
          break;
        } else {
          this.value = null;
        }
=======
  protected boolean hasNextDo() {
    while (this.iterator.hasNext()) {
      this.value = this.iterator.next();
      if (this.value != null && this.filter.test(this.value)) {
        return true;
      } else {
        this.value = null;
>>>>>>> upstream/main
      }
      if (this.value == null) {
        close();
      }
      this.loadNext = false;
    }
<<<<<<< HEAD
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

=======
    return false;
  }

>>>>>>> upstream/main
}
