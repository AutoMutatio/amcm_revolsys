package com.revolsys.exception;

import com.revolsys.collection.json.JsonObject;

public class ExceptionWithProperties extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final JsonObject properties = JsonObject.hash();

  public ExceptionWithProperties() {
  }

  public ExceptionWithProperties(final String message) {
    super(message);
  }

  public ExceptionWithProperties(final String message, final Throwable cause) {
    super(message, cause);
  }

  public ExceptionWithProperties(final String message, final Throwable cause,
    final boolean enableSuppression, final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public ExceptionWithProperties(final Throwable cause) {
    super(cause);
  }

  public <T extends Throwable> T getCause(final Class<T> clazz) {
    return Exceptions.getCause(this, clazz);
  }

  public JsonObject getProperties() {
    return this.properties;
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

  public <V> V property(final String key) {
    return this.properties.getValue(key);
  }

  public ExceptionWithProperties property(final String key, final Object value) {
    this.properties.addValue(key, value);
    return this;
  }
}
