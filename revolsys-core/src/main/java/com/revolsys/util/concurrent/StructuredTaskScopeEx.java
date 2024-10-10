package com.revolsys.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import com.revolsys.collection.iterator.ForEachHandler;
import com.revolsys.collection.iterator.ForEachMethods;
import com.revolsys.collection.iterator.Iterables;
import com.revolsys.collection.iterator.RunableMethods;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.exception.Exceptions;
import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.parallel.channel.Channel;
import com.revolsys.parallel.channel.ChannelOutput;
import com.revolsys.parallel.channel.store.Buffer;
import com.revolsys.util.Cancellable;

public class StructuredTaskScopeEx<V> extends StructuredTaskScope<V>
  implements Cancellable, ForEachMethods {

  private final ReentrantLockEx lock = new ReentrantLockEx();

  private final Condition done = this.lock.newCondition();

  private final AtomicInteger count = new AtomicInteger();

  private boolean cancelled = false;

  public StructuredTaskScopeEx() {
    super();
  }

  public StructuredTaskScopeEx(final String name, final ThreadFactory factory) {
    super(name, factory);
  }

  @Override
  public void cancel() {
    try (
      var l = this.lock.lockX()) {
      this.cancelled = true;
      this.done.signal();
    }
  }

  /**
   * <pre>
   * scope.channelSupplierWorker(
   *   (channel) -> {
   *     var values = ...;
   *     values.forEach(channel::write);
   *   },
   *   (value) -> {
   *     scope.fork(()->{
   *       // Perform task on value
   *     });
   *   }
   * );
   * </pre>
   * @param <T>
   * @param bufferSize
   * @param source
   * @param inputHandler
   */
  public <T> void channelSupplierWorker(final int bufferSize,
    final Consumer<ChannelOutput<T>> source, final Consumer<T> inputHandler) {
    final var channel = new Channel<T>(new Buffer<>(bufferSize));
    run(() -> {
      try (
        var read = channel.writeConnect()) {
        source.accept(channel);
      }
    }, () -> {
      try (
        final var write = channel.readConnect()) {
        channel.forEach(value -> inputHandler.accept(value));
      }
    });

  }

  private void done() {
    try (
      var l = this.lock.lockX()) {
      this.done.signal();
    }
  }

  @Override
  public <T> void forEach(final Consumer<? super T> action, final ForEachHandler<T> forEach) {
    final Consumer<? super T> forkAction = forkConsumerValue(action);
    forEach.forEach(forkAction);
  }

  @Override
  public <U extends V> Subtask<U> fork(final Callable<? extends U> task) {
    if (this.cancelled) {
      return null;
    } else {
      this.count.incrementAndGet();
      return super.fork(task);
    }
  }

  public <I> Subtask<V> fork(final Consumer<I> task, final I value) {
    return run(() -> task.accept(value));
  }

  @SuppressWarnings("unchecked")
  public <U extends V, I> ListEx<Subtask<U>> fork(final Function<I, U> task, final I... values) {
    return Lists.<I> newArray(values)
      .cancellable(this)
      .map(v -> fork(() -> task.apply(v)))
      .toList();
  }

  public <U extends V, I> ListEx<Subtask<U>> fork(final Function<I, U> task,
    final Iterable<I> values) {
    return Iterables.fromIterable(values)
      .cancellable(this)
      .map(v -> fork(() -> task.apply(v)))
      .toList();
  }

  public void fork(final int count, final Callable<V> task) {
    for (int i = 0; i < count; i++) {
      fork(task);
    }
  }

  public <T> void fork(final Semaphore semaphore, final Consumer<? super T> action,
    final ForEachHandler<T> forEach) {
    forEach.forEach(value -> fork(semaphore, action, value));
  }

  public <T> void fork(final Semaphore semaphore, final Consumer<? super T> action,
    final Iterable<T> values) {
    if (values != null) {
      final ForEachHandler<T> handler = values::forEach;
      fork(semaphore, action, handler);
    }
  }

  public <T> void fork(final Semaphore semaphore, final Consumer<? super T> action,
    final Stream<T> values) {
    if (values != null) {
      final ForEachHandler<T> handler = values::forEach;
      fork(semaphore, action, handler);
    }
  }

  public <T> void fork(final Semaphore semaphore, final Consumer<? super T> action, final T value) {
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
      throw Exceptions.toRuntimeException(e);
    }
  }

  public <T> void fork(final Semaphore semaphore, final Runnable action) {
    try {
      semaphore.acquire();
      run(() -> {
        try {
          action.run();
        } finally {
          semaphore.release();
        }
      });
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  <T> Consumer<? super T> forkConsumerValue(final Consumer<T> action) {
    return v -> fork(action, v);
  }

  @Override
  protected void handleComplete(final Subtask<? extends V> subtask) {
    super.handleComplete(subtask);
    if (this.count.decrementAndGet() == 0) {
      done();
    }
  }

  @Override
  public boolean isCancelled() {
    return this.cancelled;
  }

  @Override
  public StructuredTaskScopeEx<V> join() {
    try {
      while (true) {
        try (
          var l = this.lock.lockX()) {
          if (this.cancelled) {
            shutdown();
            break;
          } else if (this.count.get() == 0) {
            break;
          }
          this.done.await();

        }
      }
      super.join();
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return this;
  }

  /**
   * Use the @{link {@link #run(Runnable)} method to for a subtask
   *
   * @param forEach The handler that loops through
   * @see RunableMethods
   */
  public void run(final ForEachHandler<Runnable> forEach) {
    final Consumer<Runnable> forkAction = this::run;
    forEach.forEach(forkAction);
  }

  /**
   * Use the @{link {@link #run(Runnable)} method to for a subtask
   *
   * @param forEach The handler that loops through
   * @see RunableMethods
   */
  public void run(final Runnable... tasks) {
    for (final Runnable task : tasks) {
      run(task);
    }
  }

  /**
   * Fork a subtask that runs the @{link {@link Runnable} and returns null as the result.
   *
   * @param task The task to run
   * @return The subtask
   */
  public Subtask<V> run(final Runnable task) {
    return fork(() -> {
      task.run();
      return null;
    });
  }
}
