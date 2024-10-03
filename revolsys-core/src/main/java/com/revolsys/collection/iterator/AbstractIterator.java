package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.revolsys.properties.BaseObjectWithProperties;

public abstract class AbstractIterator<T> extends BaseObjectWithProperties
  implements Iterator<T>, Reader<T> {

  private final AtomicBoolean closed = new AtomicBoolean();

  private boolean hasNext = true;

  private boolean initialized;

  private boolean loadNext = true;

  private T object;

  @Override
  public final void close() {
    if (this.closed.compareAndSet(false, true)) {
      this.hasNext = false;
      this.object = null;
      closeDo();
    }
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

  @Override
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
