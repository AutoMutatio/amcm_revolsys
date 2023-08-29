package com.revolsys.reactive;

public class RequestCounter {
  private long count;

  public RequestCounter() {
  }

  public boolean hasDemand() {
    return this.count > 0;
  }

  public boolean isUnbounded() {
    return this.count == Long.MAX_VALUE;
  }

  public long release(final long count) {
    if (this.count == Long.MAX_VALUE) {
      return count;
    } else if (count < 0) {
      return 0;
    } else if (this.count < count) {
      final long oldCount = this.count;
      this.count = 0;
      return oldCount;
    } else {
      this.count -= count;
      return count;
    }
  }

  public void request(final long count) {
    if (count == Long.MAX_VALUE) {
      this.count = Long.MAX_VALUE;
    } else if (count > 0) {
      final long oldCount = this.count;
      this.count += count;
      if (this.count < oldCount) {
        this.count = Long.MAX_VALUE;
      }
    }
  }
}
