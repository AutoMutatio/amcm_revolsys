package com.revolsys;

import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;
import java.util.function.Function;

import com.revolsys.collection.iterator.Iterables;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.exception.Exceptions;
import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.util.Cancellable;

public class StructuredTaskScopeEx<V> extends StructuredTaskScope<V> implements Cancellable {
  @SuppressWarnings("unchecked")
  public static <I> void forEach(final Consumer<I> action, final I... values) {
    try (var scope = new StructuredTaskScopeEx<Void>()) {
      scope.fork(value -> {
        action.accept(value);
        return null;
      }, values);
      scope.join();
    }
  }

  public static <I> void forEach(final Consumer<I> action, final Iterable<I> values) {
    try (var scope = new StructuredTaskScopeEx<Void>()) {
      scope.fork(value -> {
        action.accept(value);
        return null;
      }, values);
      scope.join();
    }
  }

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

  private void done() {
    try (
        var l = this.lock.lockX()) {
      this.done.signal();
    }
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

  @SuppressWarnings("unchecked")
  public <U extends V, I> ListEx<Subtask<U>> fork(final Function<I, U> task, final I... values) {
    return Lists.<I>newArray(values).cancellable(this).map(v -> fork(() -> task.apply(v))).toList();
  }

  public <U extends V, I> ListEx<Subtask<U>> fork(final Function<I, U> task, final Iterable<I> values) {
    return Iterables.fromIterable(values).cancellable(this).map(v -> fork(() -> task.apply(v))).toList();
  }

  public void fork(final int count, final Callable<V> task) {
    for (int i = 0; i < count; i++) {
      fork(task);
    }
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
}
