package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import com.revolsys.util.BaseCloseable;

public class MapIterator<I, O> implements Iterator<O>, BaseCloseable {

  private final Function<? super I, O> converter;

  private final Iterator<I> iterator;

  private boolean hasNext = true;

  private boolean loadNext = true;

  private O value;

  MapIterator(final Iterator<I> iterator, final Function<? super I, O> converter) {
    this.iterator = iterator;
    this.converter = converter;
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
        final I inValue = this.iterator.next();
        this.value = this.converter.apply(inValue);
        if (this.value != null) {
          break;
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
  public final O next() {
    if (hasNext()) {
      final O value = this.value;
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
