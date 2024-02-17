package com.revolsys.collection.iterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

import com.revolsys.util.BaseCloseable;

final class TreeIterator<T> extends BaseIterator<T> implements BaseCloseable {

  private Iterator<T> innerIterator = Collections.emptyIterator();

  private final Function<T, Iterable<T>> iteratorConstructor;

  private Iterator<T> outerIterator;

  public TreeIterator(final Iterator<T> iterator,
    final Function<T, Iterable<T>> iteratorConstructor) {
    this.outerIterator = iterator;
    this.iteratorConstructor = iteratorConstructor;
  }

  @Override
  public final void close() {
    super.close();
    BaseCloseable.closeValue(this.innerIterator);
    BaseCloseable.closeValue(this.outerIterator);
    this.innerIterator = Collections.emptyIterator();
    this.outerIterator = Collections.emptyIterator();
  }

  @Override
  protected boolean hasNextDo() {
    while (this.innerIterator.hasNext()) {
      this.value = this.innerIterator.next();
      if (this.value != null) {
        return true;
      }
    }
    while (this.outerIterator.hasNext()) {
      this.value = this.outerIterator.next();
      if (this.value != null) {
        BaseCloseable.closeValue(this.innerIterator);
        final Iterable<T> iterable = this.iteratorConstructor.apply(this.value);
        if (iterable != null) {
          this.innerIterator = iterable.iterator();
        }
        return true;
      }
    }
    return false;
  }

}
