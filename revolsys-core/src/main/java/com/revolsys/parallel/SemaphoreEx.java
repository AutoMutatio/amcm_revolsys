package com.revolsys.parallel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.revolsys.exception.Exceptions;
import com.revolsys.util.BaseCloseable;

public class SemaphoreEx extends Semaphore {
  private final BaseCloseable release = this::release;

  public SemaphoreEx(final int permits) {
    super(permits);
  }

  public SemaphoreEx(final int permits, final boolean fair) {
    super(permits, fair);
  }

  public BaseCloseable acquireX() {
    try {
      acquire();
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return this.release;
  }

  public <V> void forEach(final ExecutorService executor, final Iterable<V> values,
    final Consumer<V> action) {
    if (values != null && action != null) {
      values.forEach(value -> {
        try {
          acquire();
          executor.execute(() -> {
            action.accept(value);
            release();
          });
        } catch (final InterruptedException e) {
          throw Exceptions.toRuntimeException(e);
        }
      });
    }
  }

  public <V> void forEach(final StructuredTaskScope<?> scope, final Iterable<V> values,
    final Consumer<V> action) throws InterruptedException {
    if (values != null && action != null) {
      values.forEach(value -> {
        try {
          acquire();
          scope.fork(() -> {
            action.accept(value);
            release();
            return null;
          });
        } catch (final InterruptedException e) {
          throw Exceptions.toRuntimeException(e);
        }
      });
      scope.join();
    }
  }

  public <V> void forEach(final Supplier<ExecutorService> executorSupplier,
    final Iterable<V> values, final Consumer<V> action) {
    if (values != null && action != null) {
      try (
        final var executor = executorSupplier.get()) {
        forEach(executor, values, action);
      }
    }
  }

  public CompletableFuture<Void> runAsync(final Runnable runnable, final Executor executor) {
    try {
      acquire();
      return CompletableFuture.runAsync(runnable, executor)
        .whenComplete((r, e) -> release());
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public <V> CompletableFuture<V> supplyAsync(final Supplier<V> supplier, final Executor executor) {
    try {
      acquire();
      return CompletableFuture.supplyAsync(supplier, executor)
        .whenComplete((r, e) -> release());
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public <V> void virtualForEach(final Iterable<V> values, final Consumer<V> action) {
    if (values != null && action != null) {
      try (
        final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
        forEach(executor, values, action);
      }
    }
  }

}
