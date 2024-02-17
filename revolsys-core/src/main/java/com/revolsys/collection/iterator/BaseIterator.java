package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.revolsys.util.BaseCloseable;

public abstract class BaseIterator<V> implements Iterator<V>, BaseCloseable {

  protected boolean hasNext = true;

  protected boolean loadNext = true;

  protected V value;

  @Override
  public void close() {
    this.loadNext = false;
    this.hasNext = false;
    this.value = null;
  }

  @Override
  public final boolean hasNext() {
    if (this.loadNext) {
      if (hasNextDo()) {
        this.loadNext = false;
        return true;
      } else {
        close();
      }
    }
    return this.hasNext;
  }

  protected abstract boolean hasNextDo();

  public BaseIterable<V> iterable() {
    return Iterables.fromIterator(this);
  }

  @Override
  public final V next() {
    if (hasNext()) {
      final V value = this.value;
      this.loadNext = true;
      return value;
    } else {
      throw new NoSuchElementException();
    }
  }
}
