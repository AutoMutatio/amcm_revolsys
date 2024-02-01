package com.revolsys.exception;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.ClosedByInterruptException;
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
      if (e instanceof WrappedRuntimeException) {
        final WrappedRuntimeException wrappedException = (WrappedRuntimeException)e;
        return wrappedException.isException(clazz);
      } else if (clazz.isAssignableFrom(e.getClass())) {
        return true;
      } else {
        return false;
      }
    }
    return false;
  }

  static boolean isInterruptException(final Throwable e) {
    return hasCause(e, InterruptedException.class) || hasCause(e, InterruptedIOException.class)
      || hasCause(e, ClosedByInterruptException.class);
  }

  @SuppressWarnings("unchecked")
  static <T> T throwCauseException(final Throwable e) {
    final Throwable cause = e.getCause();
    return (T)throwUncheckedException(cause);
  }

  static <V> V throwUncheckedException(final InterruptedException e) {
    throw new WrappedInterruptedException(e);
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

  static RuntimeException toRuntimeException(final Throwable e) {
    if (e == null) {
      return null;
    } else if (e instanceof RuntimeException) {
      throw (RuntimeException)e;
    } else if (e instanceof InvocationTargetException) {
      final Throwable cause = e.getCause();
      return toRuntimeException(cause);
    } else if (e instanceof ExecutionException) {
      final Throwable cause = e.getCause();
      return toRuntimeException(cause);
    } else {
      throw wrap(e);
    }
  }

  static String toString(final Throwable e) {
    final StringWriter stackTrace = new StringWriter();
    try (
      PrintWriter w = new PrintWriter(stackTrace)) {
      e.printStackTrace(w);
    }
    return stackTrace.toString().replaceAll("\\u0000", "");
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

  static Throwable unwrap(WrappedRuntimeException e) {
    Throwable cause = e.getCause();
    do {
      if (cause == null) {
        return e;
      } else if (cause instanceof WrappedRuntimeException) {
        e = (WrappedRuntimeException)cause;
        cause = e.getCause();
      } else {
        return cause;
      }
    } while (true);
  }

  static WrappedRuntimeException wrap(final String message, final Throwable e) {
    if (hasCause(e, InterruptedException.class)) {
      return new WrappedInterruptedException(message, e);
    } else if (hasCause(e, InterruptedIOException.class)) {
      return new WrappedInterruptedException(message, e);
    } else if (hasCause(e, IOException.class)) {
      return new WrappedIoException(message, e);
    } else {
      return new WrappedRuntimeException(message, e);
    }
  }

  static WrappedRuntimeException wrap(final Throwable e) {
    if (hasCause(e, InterruptedException.class)) {
      return new WrappedInterruptedException(e);
    } else if (hasCause(e, InterruptedIOException.class)) {
      return new WrappedInterruptedException(e);
    } else if (hasCause(e, IOException.class)) {
      return new WrappedIoException(e);
    } else {
      return new WrappedRuntimeException(e);
    }
  }
}
