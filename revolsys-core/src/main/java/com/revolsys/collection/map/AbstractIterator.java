package com.revolsys.collection.map;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.revolsys.collection.iterator.BaseIterable;

public abstract class AbstractIterator<T> implements Iterator<T>, BaseIterable<T> {

  private boolean hasNext = true;

  private boolean initialized;

  private boolean loadNext = true;

  private T object;

  public final void close() {
    this.hasNext = false;
    this.object = null;
    closeDo();
  }

  protected void closeDo() {
  }

  @Override
  protected void finalize() throws Throwable {
    close();
  }

  protected abstract T getNext() throws NoSuchElementException;

  protected T getObject() {
    return this.object;
  }

  @Override
  public final boolean hasNext() {
    if (this.hasNext) {
      init();
      if (this.loadNext) {
        try {
          this.object = getNext();
          this.loadNext = false;
        } catch (final NoSuchElementException e) {
          close();
          this.hasNext = false;
        }
      }
    }
    return this.hasNext;
  }

  public void init() {
    if (!this.initialized) {
      this.initialized = true;
      initDo();
    }
  }

  protected void initDo() {
  }

  @Override
  public Iterator<T> iterator() {
    return this;
  }

  @Override
  public final T next() {
    if (hasNext()) {
      final T currentObject = this.object;
      this.loadNext = true;
      return currentObject;
    } else {
      throw new NoSuchElementException();
    }
  }

  public void open() {
    init();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  protected void setLoadNext(final boolean loadNext) {
    this.loadNext = loadNext;
    this.hasNext = true;
  }
}
