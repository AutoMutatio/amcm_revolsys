package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.function.BiFunction;

import com.revolsys.util.BaseCloseable;

final class MapIteratorWithIndex<I, O> extends BaseIterator<O> {

  private final BiFunction<Integer, ? super I, O> converter;

  private final Iterator<I> iterator;

  private int i = 0;

  MapIteratorWithIndex(final Iterator<I> iterator,
    final BiFunction<Integer, ? super I, O> converter) {
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
    if (this.iterator.hasNext()) {
      final I inValue = this.iterator.next();
      final int index = this.i++;
      if (inValue == null) {
        this.value = null;
      } else {
        this.value = this.converter.apply(index, inValue);
      }
      return true;
    }
    return false;
  }

}
