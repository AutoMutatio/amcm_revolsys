package com.revolsys.parallel;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.revolsys.exception.Exceptions;
import com.revolsys.util.BaseCloseable;

public class ReentrantLockEx extends ReentrantLock {

  private static final long serialVersionUID = 1L;

  public static final void await(final long time, final TimeUnit unit) {
    final var lock = new ReentrantLock();
    try {
      lock.lock();
      lock.newCondition()
          .await(time, unit);
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  private final BaseCloseable unlock = this::unlock;

  public ReentrantLockEx() {
  }

  public ReentrantLockEx(final boolean fair) {
    super(fair);
  }

  public void lock(final Runnable action) {
    lock();
    try {
      action.run();
    } finally {
      unlock();
    }
  }

  public <V> V lock(final Supplier<V> action) {
    lock();
    try {
      return action.get();
    } finally {
      unlock();
    }
  }

  public BaseCloseable lockX() {
    lock();
    return this.unlock;
  }
}
