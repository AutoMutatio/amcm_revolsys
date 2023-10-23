package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.function.Function;

import com.revolsys.util.BaseCloseable;

final class MapIterator<I, O> extends BaseIterator<O> {

  private final Function<? super I, O> converter;

  private final Iterator<I> iterator;

  MapIterator(final Iterator<I> iterator, final Function<? super I, O> converter) {
    this.iterator = iterator;
    this.converter = converter;
  }

  @Override
  public final void close() {
    super.close();
    BaseCloseable.closeValue(this.iterator);
  }

  @Override
  protected boolean hasNextDo() {
    while (this.iterator.hasNext()) {
      final I inValue = this.iterator.next();
      if (inValue != null) {
        this.value = this.converter.apply(inValue);
        if (this.value != null) {
          return true;
        }
      }
    }
    return false;
  }

}
