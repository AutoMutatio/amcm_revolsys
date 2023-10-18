package com.revolsys.exception;

public class WrappedRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public WrappedRuntimeException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public WrappedRuntimeException(final Throwable cause) {
    super(cause);
  }

  public <T extends Throwable> T getCause(final Class<T> clazz) {
    return Exceptions.getCause(this, clazz);
  }

  public boolean isException(final Class<? extends Throwable> clazz) {
    final Throwable cause = getCause();
    if (cause == null) {
      return false;
    } else if (cause instanceof WrappedRuntimeException) {
      return ((WrappedRuntimeException)cause).isException(clazz);
    } else if (clazz.isAssignableFrom(cause.getClass())) {
      return true;
    } else {
      return false;
    }
  }
}
