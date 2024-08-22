package com.revolsys.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

import com.revolsys.exception.Exceptions;
import com.revolsys.io.CloseableWrapper;
import com.revolsys.logging.Logs;

@FunctionalInterface
public interface BaseCloseable extends Closeable {

  static Consumer<AutoCloseable> CLOSER = resource -> {
    try {
      resource.close();
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  };

  static BaseCloseable EMPTY = () -> {
  };

  static <C extends BaseCloseable> Consumer<? super C> closer() {
    return CLOSER;
  }

  /**
   * Close the writer without throwing an I/O exception if the close failed. The
   * error will be logged instead.
   *
   * @param closeables The closables to close.
   */
  static void closeSilent(final AutoCloseable... closeables) {
    for (final AutoCloseable closeable : closeables) {
      closeSilent(closeable);
    }
  }

  static void closeSilent(final AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (final IOException e) {
      } catch (final Exception e) {
        Logs.error(BaseCloseable.class, e.getMessage(), e);
      }
    }
  }

  /**
   * Close the writer without throwing an I/O exception if the close failed. The
   * error will be logged instead.
   *
   * @param closeables The closables to close.
   */
  static void closeSilent(final Iterable<? extends AutoCloseable> closeables) {
    for (final AutoCloseable closeable : closeables) {
      closeSilent(closeable);
    }
  }

  static void closeValue(final Object v) {
    if (v instanceof final BaseCloseable closeable) {
      closeable.close();
    } else if (v instanceof final AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (final Exception e) {
        throw Exceptions.toRuntimeException(e);
      }
    }
  }

  static void closeValueSilent(final Object v) {
    if (v instanceof final BaseCloseable closeable) {
      closeable.close();
    } else if (v instanceof final AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (final Exception e) {

      }
    }
  }

  static BaseCloseable of(final Object v) {
    if (v instanceof final BaseCloseable closeable) {
      return closeable;
    } else if (v instanceof final AutoCloseable closeable) {
      return () -> {
        try {
          closeable.close();
        } catch (final Exception e) {
          throw Exceptions.toRuntimeException(e);
        }
      };
    } else {
      return BaseCloseable.EMPTY;
    }
  }

  @Override
  void close();

  default BaseCloseable wrap() {
    return new CloseableWrapper(this);
  }
}
