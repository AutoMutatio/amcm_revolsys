package com.revolsys.reactive;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ReactiveSchedulers {
  private static Scheduler blocking;

  private static Scheduler task;

  private static Scheduler nonBlocking;

  public static Scheduler blocking() {
    if (blocking == null) {
      blocking = Schedulers.boundedElastic();
    }
    return blocking;
  }

  public static Scheduler nonBlocking() {
    if (nonBlocking == null) {
      nonBlocking = Schedulers.parallel();
    }
    return nonBlocking;
  }

  public static Scheduler task() {
    if (task == null) {
      task = Schedulers.boundedElastic();
    }
    return task;
  }

}
