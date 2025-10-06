package com.revolsys.exception;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.SocketTimeoutException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.revolsys.collection.iterator.Iterables;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public interface Exceptions {
  static void addException(final JsonObject parent, final ListEx<String> fullTrace,
    final String key, final Throwable e) {
    if (e != null) {
      final var eJson = toJson(e, fullTrace);
      parent.addValue(key, eJson);
    }
  }

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

  static ListEx<StackTraceElement> getTrace(final Throwable e) {
    if (e == null) {
      return Lists.empty();
    }
    final var stackTrace = e.getStackTrace();
    return Lists.newArray(stackTrace);
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
    final var thread = Thread.currentThread();
    if (thread.isInterrupted()) {
      return true;
    } else if (hasCause(e, InterruptedException.class)
      || hasCause(e, ClosedByInterruptException.class)
      || hasCause(e, WrappedInterruptedException.class)) {
      thread.interrupt();
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

  static void removeCommonTrace(final ListEx<StackTraceElement> thisTrace, final Throwable cause) {
    final var causeTrace = getTrace(cause);
    if (!thisTrace.isEmpty() && !causeTrace.isEmpty()) {
      int thisIndex = thisTrace.size() - 1;
      int causeIndex = causeTrace.size() - 1;
      while (thisIndex >= 0 && causeIndex >= 0) {
        final var thisLine = thisTrace.get(thisIndex);
        final var causeLine = causeTrace.get(causeIndex);
        if (thisLine.equals(causeLine)) {
          thisTrace.remove(thisIndex);
        }
        thisIndex--;
        causeIndex--;
      }
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
    final var fullTrace = Lists.<String> newArray();
    return toJson(e, fullTrace)//
      .addValue("fullTrace", fullTrace);
  }

  static JsonObject toJson(final Throwable e, final ListEx<String> fullTrace) {
    if (e == null) {
      return JsonObject.EMPTY;
    }
    final var clazz = e.getClass()
      .getName();
    final var message = e.getMessage();

    fullTrace.addValue("CLASS: " + clazz);
    if (Property.hasValue(message)) {
      fullTrace.addNotEmpty("MESSAGE: " + message);
    }

    final var json = JsonObject.hash()
      .addNotEmpty("class", clazz)
      .addNotEmpty("message", message);

    try {
      // Can't use instanceof as it could be in a different class
      final var properties = e.getClass()
        .getMethod("getProperties")
        .invoke(e);
      json.addNotEmpty("properties", properties);
    } catch (final Throwable e2) {
      Debug.noOp();
    }

    final var localizedMessage = e.getLocalizedMessage();
    if (!Strings.equals(message, localizedMessage)) {
      json.addNotEmpty("localizedMessage", localizedMessage);
    }

    final var trace = getTrace(e);

    if (!trace.isEmpty()) {
      final var traceToAddToFull = trace.clone();
      removeCommonTrace(traceToAddToFull, e.getCause());
      traceToAddToFull.map(StackTraceElement::toString)
        .forEach(fullTrace::add);

      final var traceJson = trace.map(StackTraceElement::toString);
      json.addValue("trace", traceJson);
    }

    final var cause = e.getCause();
    addException(json, fullTrace, "cause", cause);

    if (e instanceof final SQLException sqlException) {
      final var next = sqlException.getNextException();
      if (next != cause) {
        addException(json, fullTrace, "next", next);
      }
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
    } else if (e instanceof final WrappedRuntimeException re) {
      throw re;
    } else if (isInterruptException(e)) {
      return wrap(e, WrappedInterruptedException.class, WrappedInterruptedException::new);
    } else if (isTimeoutException(e)) {
      return wrap(e, WrappedTimeoutException.class, WrappedTimeoutException::new);
    } else if (hasCause(e, IOException.class)) {
      return new WrappedIoException(e);
    } else if (e instanceof final Error error) {
      throw error;
    } else if (e instanceof final RuntimeException re) {
      throw re;
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

  static WrappedRuntimeException toWrapped(final Throwable e) {
    if (e == null) {
      return null;
    } else if (e instanceof final WrappedRuntimeException re) {
      throw re;
    } else if (isInterruptException(e)) {
      return new WrappedInterruptedException(e);
    } else if (isTimeoutException(e)) {
      return new WrappedTimeoutException(e);
    } else if (hasCause(e, IOException.class)) {
      return new WrappedIoException(e);
    } else if (e instanceof Error) {
      throw new WrappedRuntimeException(e);
    } else if (e instanceof RuntimeException) {
      throw new WrappedRuntimeException(e);
    } else if (e instanceof InvocationTargetException) {
      final Throwable cause = e.getCause();
      return toWrapped(cause);
    } else if (e instanceof ExecutionException) {
      final Throwable cause = e.getCause();
      return toWrapped(cause);
    } else {
      throw new WrappedRuntimeException(e);
    }
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
    if (e == null) {
      return null;
    } else if (isTimeoutException(e)) {
      return new WrappedTimeoutException(message, e);
    } else if (isInterruptException(e)) {
      return new WrappedInterruptedException(message, e);
    } else if (hasCause(e, IOException.class)) {
      return new WrappedIoException(message, e);
    } else {
      return new WrappedRuntimeException(message, e);
    }
  }

  /**
   * Wrap the exception using the constructor if it isn't doesn't have the exceptionClass as
   * a cause or it's not an Error or RuntimeException
   * @param e
   * @param exceptionClass
   * @param constructor
   * @return
   * @throws Error
   */
  static RuntimeException wrap(final Throwable e, final Class<? extends Throwable> exceptionClass,
    final Function<Throwable, RuntimeException> constructor) throws Error {
    if (hasCause(e, exceptionClass)) {
      if (e instanceof final Error error) {
        throw error;
      } else if (e instanceof final RuntimeException re) {
        throw re;
      }
    }
    return constructor.apply(e);
  }
}
