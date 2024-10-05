package com.revolsys.util.concurrent;

public class Concurrent {

  public static ThreadFactoryEx virtual() {
    final var factory = Thread.ofVirtual()
      .factory();
    return new ThreadFactoryEx(factory);
  }

  public static ThreadFactoryEx virtual(final String prefix) {
    final var factory = Thread.ofVirtual()
      .name(prefix, 0)
      .factory();
    return new ThreadFactoryEx(prefix, factory);
  }
}
