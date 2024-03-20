package com.revolsys.parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.revolsys.util.BaseCloseable;

public class ThreadUtil {

  public static boolean isInterrupted() {
    return Thread.currentThread()
      .isInterrupted();
  }

  public static BaseCloseable lock(final ReentrantLock lock) {
    lock.lock();
    return lock::unlock;
  }

  public static ExecutorService newVirtualThreadPerTaskExecutor(final String name) {
    final ThreadFactory factory = Thread.ofVirtual()
      .name(name)
      .factory();
    return Executors.newThreadPerTaskExecutor(factory);
  }

  public static ExecutorService newVirtualThreadPerTaskExecutor(final String prefix,
    final long start) {
    final ThreadFactory factory = Thread.ofVirtual()
      .name(prefix, start)
      .factory();
    return Executors.newThreadPerTaskExecutor(factory);
  }

  public static Supplier<ExecutorService> supplyVirtualThreadPerTaskExecutor(final String prefix,
    final long start) {
    return () -> newVirtualThreadPerTaskExecutor(prefix, start);
  }
}
