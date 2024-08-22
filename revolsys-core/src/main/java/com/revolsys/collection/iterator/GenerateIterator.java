package com.revolsys.collection.iterator;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

import com.revolsys.util.BaseCloseable;

final class GenerateIterator<T> extends AbstractIterator<T> {

  private final AutoCloseable resource;

  private final Supplier<T> generator;

  public GenerateIterator(final AutoCloseable resource, final Supplier<T> generator) {
    this.resource = resource;
    this.generator = generator;
  }

  @Override
  protected void closeDo() {
    super.closeDo();
    BaseCloseable.closeValue(this.resource);
  }

  @Override
  protected T getNext() throws NoSuchElementException {
    return this.generator.get();
  }

}
