package com.revolsys.exception;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.SocketTimeoutException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.revolsys.collection.iterator.Iterables;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.util.Strings;

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
      if (message != null && message.toLowerCase()
        .contains(expected)) {
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
    if (hasCause(e, InterruptedException.class) || hasCause(e, InterruptedIOException.class)
      || hasCause(e, ClosedByInterruptException.class)
      || hasCause(e, WrappedInterruptedException.class) || Thread.interrupted()) {
      return true;
    } else {
      final var ioe = getCause(e, IOException.class);
      if (ioe != null) {
        if ("Closed by interrupt".equals(ioe.getMessage())) {
          return true;
        }
      }
      return false;
    }
  }

  static boolean isTimeoutException(final Throwable e) {
    if (hasCause(e, WrappedTimeoutException.class) || hasCause(e, SocketTimeoutException.class)
      || hasCause(e, HttpConnectTimeoutException.class) || hasCause(e, HttpTimeoutException.class)
      || hasCause(e, SQLTimeoutException.class) || hasCause(e, SQLTimeoutException.class)
      || hasCause(e, TimeoutException.class)) {
      return true;
    } else {
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  static <T> T throwCauseException(final Throwable e) {
    final Throwable cause = e.getCause();
    return (T)throwUncheckedException(cause);
  }

  static <T> T throwUncheckedException(final Throwable e) {
    if (e == null) {
      return null;
    } else if (e instanceof Error) {
      throw (Error)e;
    } else {
      throw toRuntimeException(e);
    }
  }

  static JsonObject toJson(final StackTraceElement element) {
    return JsonObject.hash()
      .addNotEmpty("c", element.getClassName())
      .addNotEmpty("m", element.getMethodName())
      .addNotEmpty("f", element.getFileName())
      .addNotEmpty("l", element.getLineNumber());

    // TODO? moduleName, moduleVersion, classLoader
  }

  static JsonObject toJson(final Throwable e) {
    final var message = e.getMessage();
    final var json = JsonObject.hash()
      .addNotEmpty("class", e.getClass())
      .addNotEmpty("message", message);
    final var localizedMessage = e.getLocalizedMessage();
    if (!Strings.equals(message, localizedMessage)) {
      json.addNotEmpty("localizedMessage", localizedMessage);
    }

    final var trace = e.getStackTrace();
    if (trace.length > 0) {
      final var traceJson = Iterables.fromValues(trace)
        .map(StackTraceElement::toString);
      json.addValue("trace", traceJson);
    }

    final var cause = e.getCause();
    if (cause != null) {
      final var causeJson = Exceptions.toJson(cause);
      json.addValue("cause", causeJson);
    }

    final var suppressed = e.getSuppressed();
    if (suppressed.length > 0) {
      final var suppressedJson = Iterables.fromValues(suppressed)
        .map(Exceptions::toJson);
      json.addValue("supressed", suppressedJson);
    }
    return json;
  }

  static RuntimeException toRuntimeException(final Throwable e) {
    if (e == null) {
      return null;
    } else if (isTimeoutException(e)) {
      return new WrappedTimeoutException(e);
    } else if (isInterruptException(e)) {
      return new WrappedInterruptedException(e);
    } else if (hasCause(e, IOException.class)) {
      return new WrappedIoException(e);
    } else if (e instanceof RuntimeException) {
      throw (RuntimeException)e;
    } else if (e instanceof InvocationTargetException) {
      final Throwable cause = e.getCause();
      return toRuntimeException(cause);
    } else if (e instanceof ExecutionException) {
      final Throwable cause = e.getCause();
      return toRuntimeException(cause);
    } else {
      throw new WrappedRuntimeException(e);
    }
  }

  static String toString(final Throwable e) {
    final StringWriter stackTrace = new StringWriter();
    try (
      PrintWriter w = new PrintWriter(stackTrace)) {
      e.printStackTrace(w);
    }
    return stackTrace.toString()
      .replaceAll("\\u0000", "");
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
    if (isTimeoutException(e)) {
      return new WrappedTimeoutException(message, e);
    } else if (isInterruptException(e)) {
      return new WrappedInterruptedException(message, e);
    } else if (hasCause(e, IOException.class)) {
      return new WrappedIoException(message, e);
    } else {
      return new WrappedRuntimeException(message, e);
    }
  }
}
