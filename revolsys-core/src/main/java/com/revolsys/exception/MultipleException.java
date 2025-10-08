package com.revolsys.exception;

import java.util.List;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

@SuppressWarnings("serial")
public class MultipleException extends RuntimeException {
  private final ListEx<Throwable> exceptions;

  public MultipleException(final List<? extends Throwable> exceptions) {
    super(exceptions.get(0));
    this.exceptions = Lists.toArray(exceptions);
  }

  public ListEx<Throwable> getExceptions() {
    return this.exceptions;
  }

}
