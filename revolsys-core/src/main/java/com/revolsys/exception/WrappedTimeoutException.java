package com.revolsys.exception;

public class WrappedTimeoutException extends WrappedRuntimeException {
  private static final long serialVersionUID = 1L;

  public WrappedTimeoutException(final String message, final Throwable e) {
    super(message, e);
  }

  WrappedTimeoutException(final Throwable cause) {
    super(cause);
  }

}
