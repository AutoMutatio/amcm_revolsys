package com.revolsys.parallel;

import java.util.concurrent.Semaphore;

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
}
