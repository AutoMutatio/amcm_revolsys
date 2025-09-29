package com.revolsys.util.concurrent;

import java.util.Objects;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Function;

import com.revolsys.collection.iterator.ForEachHandler;
import com.revolsys.collection.iterator.ForEachMethods;
import com.revolsys.collection.iterator.RunnableMethods;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.exception.Exceptions;
import com.revolsys.exception.MultipleException;
import com.revolsys.logging.Logs;
import com.revolsys.parallel.channel.Channel;
import com.revolsys.parallel.channel.ChannelOutput;
import com.revolsys.parallel.channel.store.Buffer;
import com.revolsys.util.BaseCloseable;

public class Parallel
  implements BaseCloseable, ForEachMethods<Parallel>, RunnableMethods<Parallel> {
  private final ListEx<Throwable> exceptions = Lists.newArray();

  private final ThreadFactory threadFactory;

  private final Phaser phaser = new Phaser(1);

  private final Consumer<Runnable> forkAction = this::run;

  public Parallel(final ThreadFactory threadFactory) {
    this.threadFactory = threadFactory;
  }

  @Override
  public void close() {
    join();
  }

  public <V> Parallel consume(final Consumer<? super V> action, final V value) {
    return run(() -> action.accept(value));
  }

  @Override
  public <V> Parallel forEach(final ForEachHandler<V> forEach, final Consumer<? super V> action) {
    forEach.forEach(value -> consume(action, value));
    return this;
  }

  @SuppressWarnings("unchecked")
  public <I> Parallel fork(final Consumer<I> task, final I... values) {
    for (final var value : values) {
      consume(task, value);
    }
    return this;
  }

  @SuppressWarnings("unchecked")
  public <U, I> Parallel fork(final Function<I, U> task, final I... values) {
    for (final var value : values) {
      run(() -> task.apply(value));
    }
    return this;
  }

  public <T> Parallel fork(final int workerCount, final Channel<T> channel,
    final Consumer<T> inputHandler) {
    for (int i = 0; i < workerCount && !channel.isClosed(); i++) {
      try {
        channel.readConnect();
      } catch (final IllegalStateException e) {
        return this;
      }
    }
    for (int i = 0; i < workerCount && !channel.isClosed(); i++) {
      run(() -> {
        try {
          while (!channel.isClosed()) {
            final T value = channel.read();
            inputHandler.accept(value);
          }
        } catch (RuntimeException | Error e) {
          if (channel.isClosed() || Exceptions.isInterruptException(e)) {
            return;
          }
          Logs.error(this, "Shutdown: Task handler error", e);
        } finally {
          channel.readDisconnect();
        }
      });
    }
    return this;
  }

  public <T> Parallel fork(final int workerCount, final Consumer<ChannelOutput<T>> source,
    final Consumer<T> inputHandler) {
    Objects.requireNonNull(source, "Source required");
    Objects.requireNonNull(inputHandler, "Input handler required");
    final var channel = new Channel<T>(new Buffer<>(workerCount));
    channel.writeConnect();

    run(() -> {
      try {
        source.accept(channel);
      } catch (RuntimeException | Error e) {
        if (channel.isClosed() || Exceptions.isInterruptException(e)) {
          return;
        }
        Logs.error(this, "Shutdown: Task source error", e);
      } finally {
        channel.writeDisconnect();
      }
    });

    return fork(workerCount, channel, inputHandler);
  }

  public <T> Parallel fork(final int workerCount, final Iterable<T> values,
    final Consumer<T> inputHandler) {
    return fork(workerCount, out -> values.forEach(out::write), inputHandler);
  }

  public <I> Parallel fork(final Iterable<I> values, final Consumer<I> task) {
    for (final var value : values) {
      consume(task, value);
    }
    return this;
  }

  public <T> void fork(final Semaphore semaphore, final ForEachHandler<T> forEach,
    final Consumer<? super T> action) {
    forEach.forEach(value -> fork(semaphore, value, action));
  }

  public <T> void fork(final Semaphore semaphore, final T value, final Consumer<? super T> action) {
    try {
      semaphore.acquire();
      run(() -> {
        try {
          action.accept(value);
        } finally {
          semaphore.release();
        }
      });
    } catch (final InterruptedException e) {
      Thread.interrupted();
      throw Exceptions.toRuntimeException(e);
    }
  }

  public <T> void forkIterable(final Semaphore semaphore, final Iterable<T> values,
    final Consumer<? super T> action) {
    if (values != null) {
      final ForEachHandler<T> handler = values::forEach;
      fork(semaphore, handler, action);
    }
  }

  public void join() {
    this.phaser.arriveAndAwaitAdvance();
    if (!this.exceptions.isEmpty()) {
      if (this.exceptions.size() == 1) {
        final var exception = this.exceptions.get(0);
        if (exception instanceof final Error error) {
          throw error;
        } else {
          throw Exceptions.toRuntimeException(exception);
        }
      } else {
        throw new MultipleException(this.exceptions);
      }

    }
  }

  /**
   * Use the @{link {@link #run(Runnable)} method to for a subtask
   *
   * @param forEach The handler that loops through
   * @see RunnableMethods
   */
  @Override
  public Parallel run(final ForEachHandler<Runnable> forEach) {
    forEach.forEach(this.forkAction);
    return this;
  }

  /**
   * Use the @{link {@link #run(Runnable)} method to for a subtask
   *
   * @param forEach The handler that loops through
   * @see RunnableMethods
   */
  @Override
  public Parallel run(final Runnable... tasks) {
    for (final Runnable task : tasks) {
      run(task);
    }
    return this;
  }

  public Parallel run(final Runnable runnable) {
    Parallel.this.phaser.register();
    this.threadFactory.newThread(() -> {
      try {
        runnable.run();
      } catch (final Throwable e) {
        this.exceptions.add(e);
      } finally {
        Parallel.this.phaser.arriveAndDeregister();
      }
    }).start();
    return this;
  }
}
