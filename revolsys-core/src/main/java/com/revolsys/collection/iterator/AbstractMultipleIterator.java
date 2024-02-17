package com.revolsys.collection.iterator;

import java.util.NoSuchElementException;

import com.revolsys.util.BaseCloseable;

public abstract class AbstractMultipleIterator<T> extends AbstractIterator<T> {
  private AbstractIterator<T> iterator;

  @Override
  public void closeDo() {
    if (this.iterator != null) {
      BaseCloseable.closeSilent(this.iterator);
      this.iterator = null;
    }
  }

  protected AbstractIterator<T> getIterator() {
    if (this.iterator == null) {
      this.iterator = getNextIterator();
    }
    return this.iterator;
  }

  @Override
  protected T getNext() throws NoSuchElementException {
    try {
      if (this.iterator == null) {
        this.iterator = getNextIterator();
      }
      while (this.iterator != null && !this.iterator.hasNext()) {
        BaseCloseable.closeSilent(this.iterator);
        this.iterator = getNextIterator();
      }
      if (this.iterator == null) {
        throw new NoSuchElementException();
      } else {
        return this.iterator.next();
      }
    } catch (final NoSuchElementException e) {
      this.iterator = null;
      throw e;
    }
  }

  /**
   * Get the next iterator, if no iterators are available throw
   * {@link NoSuchElementException}. Don't not return null.
   *
   */
  public abstract AbstractIterator<T> getNextIterator() throws NoSuchElementException;

}
