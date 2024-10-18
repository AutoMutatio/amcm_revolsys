package com.revolsys.exception;

import com.revolsys.collection.list.ListEx;

public class MultipleException extends RuntimeException {
  private final ListEx<Throwable> exceptions;

  public MultipleException(final ListEx<Throwable> exceptions) {
    super(exceptions.get(0));
    this.exceptions = exceptions;
  }

  public ListEx<Throwable> getExceptions() {
    return this.exceptions;
  }

}
