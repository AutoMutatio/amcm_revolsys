package com.revolsys.exception;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

@SuppressWarnings("serial")
public class MultipleException extends RuntimeException {

  public MultipleException(final Iterable<? extends Throwable> exceptions) {
    exceptions.forEach(this::addSuppressed);
  }

  public MultipleException(final Throwable[] exceptions) {
    for (final var exception : exceptions) {
      addSuppressed(exception);
    }
  }

  public ListEx<Throwable> getExceptions() {
    final Throwable[] exceptions = getSuppressed();
    return Lists.newArray(exceptions);
  }

}
