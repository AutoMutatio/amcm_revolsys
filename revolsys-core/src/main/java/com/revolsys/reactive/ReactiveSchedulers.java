package com.revolsys.reactive;

import org.jeometry.common.logging.Logs;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ReactiveSchedulers {
  private static Scheduler blocking;

  private static Scheduler task;

  private static Scheduler nonBlocking;

  private static Scheduler limit;

  public static Scheduler blocking() {
    if (blocking == null) {
      synchronized (ReactiveSchedulers.class) {
        if (blocking == null) {
          blocking = Schedulers.boundedElastic();
        }
      }
    }
    return blocking;
  }

  public static Scheduler limit() {
    if (limit == null) {
      synchronized (ReactiveSchedulers.class) {
        if (limit == null) {
          final int parallelLimit = limitDefaultParallelLimit();
          final int queueSize = limitDefaultQueueSize();
          limit = ReactiveSchedulers.newLimit(parallelLimit, queueSize);
        }
      }
    }
    return limit;
  }

  public static int limitDefaultParallelLimit() {
    final String envLimit = System.getenv("REVOLSYS_SCHEDULER_LIMIT_THREAD_COUNT");
    if (envLimit != null) {
      try {
        return Integer.parseInt(envLimit);
      } catch (final NumberFormatException e) {
        Logs.error(ReactiveSchedulers.class, "com.revolsys.scheduler.limitSize=" + envLimit
          + " is not a valid integer, using the default");
      }
    }
    final Runtime runtime = Runtime.getRuntime();
    final int size = runtime.availableProcessors();
    long maxMemory = runtime.maxMemory();
    if (maxMemory == Long.MAX_VALUE) {
      maxMemory = 8527020032L; // 8GB
    }
    final int sizeByMemory = (int)Math.floorDiv(maxMemory, 1 * 1024 * 1204 * 1024);
    final int cpuCountEstimate = Math.floorDiv(size, 2);
    return Math.min(cpuCountEstimate, sizeByMemory);
  }

  public static int limitDefaultQueueSize() {
    final int queueSize = limitDefaultParallelLimit() * 100;
    return queueSize;
  }

  public static LimitScheduler newLimit(final int paralellLimit, final int queueSize) {
    final Scheduler scheduler = ReactiveSchedulers.blocking();
    return newLimit(scheduler, paralellLimit, queueSize);
  }

  public static LimitScheduler newLimit(final Scheduler scheduler, final int paralellLimit,
    final int queueSize) {
    final LimitScheduler limitScheduler = new LimitScheduler(scheduler, paralellLimit, queueSize,
      null);
    limitScheduler.start();
    return limitScheduler;
  }

  public static Scheduler nonBlocking() {
    if (nonBlocking == null) {
      synchronized (ReactiveSchedulers.class) {
        if (nonBlocking == null) {
          nonBlocking = Schedulers.parallel();
        }
      }
    }
    return nonBlocking;
  }

  public static Scheduler task() {
    if (task == null) {
      synchronized (ReactiveSchedulers.class) {
        if (task == null) {
          // task = Schedulers.newBoundedElastic(2, 100, "task");
          task = Schedulers.boundedElastic();
        }
      }
    }
    return task;
  }

}
