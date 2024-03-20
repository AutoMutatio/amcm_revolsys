package com.revolsys;

import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import com.revolsys.parallel.ReentrantLockEx;

public class StructuredTaskScopeEx<V> extends StructuredTaskScope<V> {

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
  public StructuredTaskScopeEx<V> join() throws InterruptedException {
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
    return this;
  }
}
