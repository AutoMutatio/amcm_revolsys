package com.revolsys.collection.map;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import com.revolsys.collection.iterator.AbstractIterator;

public class MapIterator<I, O> extends AbstractIterator<O> {

  private Function<? super I, O> converter;

  private Iterator<I> iterator;

  public MapIterator(final Iterator<I> iterator, final Function<? super I, O> converter) {
    this.iterator = iterator;
    this.converter = converter;
  }

  @Override
  protected void closeDo() {
    super.closeDo();
    if (this.iterator instanceof AbstractIterator) {
      final AbstractIterator<?> abstractIterator = (AbstractIterator<?>)this.iterator;
      abstractIterator.close();
    }
    this.converter = null;
    this.iterator = null;
  }

  @Override
  protected O getNext() throws NoSuchElementException {
    while (this.iterator != null && this.iterator.hasNext()) {
      final I value = this.iterator.next();
      return this.converter.apply(value);
    }
    throw new NoSuchElementException();
  }
}
