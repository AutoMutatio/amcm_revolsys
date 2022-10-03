package com.revolsys.parallel.channel;

import java.time.Duration;
import java.time.Instant;

public class Timer implements SelectableInput {
  private long timeoutMillis;

  private final Duration timeout;

  private final ThreadLocal<Instant> disabledTime = new ThreadLocal<>();

  public Timer(final Duration timeout) {
    this.timeout = timeout;
  }

  public Timer(final long timeout) {
    this(Duration.ofMillis(timeout));
    this.timeoutMillis = timeout;
  }

  @Override
  public boolean disable(final AbstractMultiInputSelector selector) {
    final Instant disabledTime = this.disabledTime.get();
    if (disabledTime == null) {
      return false;
    } else {
      final Instant now = Instant.now();
      return !now.isBefore(disabledTime);
    }
  }

  @Override
  public boolean enable(final AbstractMultiInputSelector selector) {
    final Instant now = Instant.now();
    final Instant disabledTime = now.plus(this.timeout);
    this.disabledTime.set(disabledTime);
    return false;
  }

  public Instant getDisabledTime(final AbstractMultiInputSelector selector) {
    return this.disabledTime.get();
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  public boolean isTimeout() {
    final boolean timeout = System.currentTimeMillis() > this.timeoutMillis;
    return timeout;
  }
}
