package com.revolsys.parallel;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.revolsys.exception.Exceptions;
import com.revolsys.util.BaseCloseable;

public class ThreadUtil {

  public static boolean awaitDuration(final Condition condition, final Duration duration) {
    try {
      if (duration.isZero()) {
        // Don't wait
        return true;
      } else if (duration.isNegative()) {
        condition.await();
        return true;
      } else {
        final var seconds = duration.getSeconds();
        final var nano = duration.getNano();
        if (nano == 0 || seconds > 9223372034L) {
          return condition.await(seconds, TimeUnit.SECONDS);
        } else {
          return condition.await(duration.toNanos(), TimeUnit.NANOSECONDS);
        }
      }
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

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
