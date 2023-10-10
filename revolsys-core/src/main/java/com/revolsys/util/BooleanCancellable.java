package com.revolsys.util;

import org.jeometry.common.util.Cancellable;

public class BooleanCancellable implements Cancellable {

  private boolean cancelled = false;

  @Override
  public void cancel() {
    this.cancelled = true;
  }

  @Override
  public boolean isCancelled() {
    return this.cancelled;
  }

  @Override
  public String toString() {
    return Boolean.toString(this.cancelled);
  }
}
