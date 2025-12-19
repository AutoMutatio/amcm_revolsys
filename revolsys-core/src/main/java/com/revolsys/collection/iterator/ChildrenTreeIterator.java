package com.revolsys.collection.iterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

import com.revolsys.util.BaseCloseable;

final class ChildrenTreeIterator<T, C> extends BaseIterator<C> implements BaseCloseable {

  private Iterator<C> innerIterator = Collections.emptyIterator();

  private final Function<T, Iterable<C>> iteratorConstructor;

  private Iterator<T> outerIterator;

  public ChildrenTreeIterator(final Iterator<T> outerIterator,
    final Function<T, Iterable<C>> iteratorConstructor) {
    this.outerIterator = outerIterator;
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
    do {
      while (this.innerIterator.hasNext()) {
        this.value = this.innerIterator.next();
        if (this.value != null) {
          return true;
        }
      }
      if (this.outerIterator.hasNext()) {
        final var outerValue = this.outerIterator.next();
        if (outerValue == null) {
          return false;
        } else {
          BaseCloseable.closeValue(this.innerIterator);
          final Iterable<C> iterable = this.iteratorConstructor.apply(outerValue);
          if (iterable == null) {
            this.innerIterator = Iterables.<C> empty()
              .iterator();
          } else {
            this.innerIterator = iterable.iterator();
          }
        }
      } else {
        return false;
      }
    } while (true);
  }

}
