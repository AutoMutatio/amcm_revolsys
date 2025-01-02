package com.revolsys.util.concurrent;

public class LastAccess {

  private long accessTime = System.currentTimeMillis();

  public void access() {
    this.accessTime = System.currentTimeMillis();
  }

  public long accessTime() {
    return this.accessTime;
  }

  @Override
  public String toString() {
    return Long.toString(this.accessTime);
  }

}
