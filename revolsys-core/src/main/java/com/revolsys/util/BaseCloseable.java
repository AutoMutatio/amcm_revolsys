package com.revolsys.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jeometry.common.exception.Exceptions;
import org.jeometry.common.logging.Logs;
import org.reactivestreams.Publisher;

import com.revolsys.io.CloseableWrapper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface BaseCloseable extends Closeable {
  static Consumer<AutoCloseable> CLOSER = resource -> {
    try {
      resource.close();
    } catch (final Exception e) {
      throw Exceptions.wrap(e);
    }
  };

  static BaseCloseable EMPTY = () -> {
  };

  static void closeValue(final Object v) {
    if (v instanceof final BaseCloseable closeable) {
      closeable.close();
      ;
    } else if (v instanceof final AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (final Exception e) {
        Exceptions.throwUncheckedException(e);
      }
    }
  }

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

  static <R extends AutoCloseable, V> Flux<V> fluxUsing(final Callable<R> resourceSupplier,
    final Function<R, Publisher<V>> action) {
    return Flux.using(resourceSupplier, action, CLOSER);
  }

  static <R extends AutoCloseable, V> Mono<V> monoUsing(final Callable<R> resourceSupplier,
    final Function<R, Mono<V>> action) {
    return Mono.using(resourceSupplier, action, CLOSER);
  }

  @Override
  void close();

  @SuppressWarnings("unchecked")
  default <R extends BaseCloseable, V> Flux<V> fluxUsing(final Function<R, Publisher<V>> action) {
    return BaseCloseable.fluxUsing(() -> (R)this, action);
  }

  @SuppressWarnings("unchecked")
  default <R extends BaseCloseable, V> Mono<V> monoUsing(final Function<R, Mono<V>> action) {
    return BaseCloseable.monoUsing(() -> ((R)this), action);
  }

  static BaseCloseable of(final Object v) {
    if (v instanceof final BaseCloseable closeable) {
      return closeable;
    } else if (v instanceof final AutoCloseable closeable) {
      return () -> {
        try {
          closeable.close();
        } catch (final Exception e) {
          Exceptions.throwUncheckedException(e);
        }
      };
    } else {
      return BaseCloseable.EMPTY;
    }
  }

  default BaseCloseable wrap() {
    return new CloseableWrapper(this);
  }
}
