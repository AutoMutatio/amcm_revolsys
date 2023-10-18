package com.revolsys.exception;

public class WrappedInterruptedException extends WrappedRuntimeException {
  private static final long serialVersionUID = 1L;

  public WrappedInterruptedException(final String message, final Throwable e) {
    super(message, e);
  }

  WrappedInterruptedException(final Throwable cause) {
    super(cause);
  }

}
