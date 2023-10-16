package com.revolsys.exception;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;

public interface Exceptions {
  @SuppressWarnings("unchecked")
  static <T extends Throwable> T getCause(Throwable e, final Class<T> clazz) {
    while (e != null) {
      if (clazz.isAssignableFrom(e.getClass())) {
        return (T)e;
      } else {
        e = e.getCause();
      }
    }
    return null;
  }

  static boolean hasCause(Throwable e, final Class<? extends Throwable> clazz) {
    while (e != null) {
      if (clazz.isAssignableFrom(e.getClass())) {
        return true;
      } else {
        e = e.getCause();
      }
    }
    return false;
  }

  static boolean hasMessage(Throwable e, final String expected) {
    do {
      final String message = e.getMessage();
      if (message != null && message.equalsIgnoreCase(expected)) {
        return true;
      }
      e = e.getCause();
    } while (e != null);
    return false;
  }

  static boolean hasMessagePart(Throwable e, final String expected) {
    do {
      final String message = e.getMessage();
      if (message != null && message.toLowerCase().contains(expected)) {
        return true;
      }
      e = e.getCause();
    } while (e != null);
    return false;
  }

  static boolean isException(final Throwable e, final Class<? extends Throwable> clazz) {
    while (e != null) {
      if (e instanceof WrappedException) {
        final WrappedException wrappedException = (WrappedException)e;
        return wrappedException.isException(clazz);
      } else if (clazz.isAssignableFrom(e.getClass())) {
        return true;
      } else {
        return false;
      }
    }
    return false;
  }

  static String stackTraceToString(final Throwable e) {
    final StringWriter stackTrace = new StringWriter();
    try (
      PrintWriter w = new PrintWriter(stackTrace)) {
      e.printStackTrace(w);
    }
    return stackTrace.toString();
  }

  @SuppressWarnings("unchecked")
  static <T> T throwCauseException(final Throwable e) {
    final Throwable cause = e.getCause();
    return (T)throwUncheckedException(cause);
  }

  @SuppressWarnings("unchecked")
  static <T> T throwUncheckedException(final Throwable e) {
    if (e == null) {
      return null;
    } else if (e instanceof InvocationTargetException) {
      return (T)throwCauseException(e);
    } else if (e instanceof ExecutionException) {
      return (T)throwCauseException(e);
    } else if (e instanceof RuntimeException) {
      throw (RuntimeException)e;
    } else if (e instanceof Error) {
      throw (Error)e;
    } else {
      throw wrap(e);
    }
  }

  static String toString(final Throwable e) {
    final StringWriter string = new StringWriter();
    final PrintWriter out = new PrintWriter(string);
    e.printStackTrace(out);
    return string.toString();
  }

  @SuppressWarnings("unchecked")
  static <T extends Throwable> T unwrap(final Exception e, final Class<T> clazz) {
    Throwable cause = e.getCause();
    while (cause != null) {
      if (clazz.isAssignableFrom(cause.getClass())) {
        return (T)cause;
      } else {
        cause = e.getCause();
      }
    }
    return null;
  }

  static Throwable unwrap(WrappedException e) {
    Throwable cause = e.getCause();
    do {
      if (cause == null) {
        return e;
      } else if (cause instanceof WrappedException) {
        e = (WrappedException)cause;
        cause = e.getCause();
      } else {
        return cause;
      }
    } while (true);
  }

  static WrappedException wrap(final String message, final Throwable e) {
    return new WrappedException(message, e);
  }

  static WrappedException wrap(final Throwable e) {
    return new WrappedException(e);
  }
}
