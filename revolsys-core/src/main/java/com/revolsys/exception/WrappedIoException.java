package com.revolsys.exception;

public final class WrappedIoException extends WrappedRuntimeException {
  private static final long serialVersionUID = 1L;

  WrappedIoException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public WrappedIoException(final Throwable cause) {
    super(cause);
  }

}
