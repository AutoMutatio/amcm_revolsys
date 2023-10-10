package com.revolsys.parallel;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.jeometry.common.util.BaseCloseable;

public class ReentrantLockEx extends ReentrantLock {

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
