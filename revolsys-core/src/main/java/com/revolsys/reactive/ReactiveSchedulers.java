package com.revolsys.reactive;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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

  public static Scheduler nonBlockingIo() {
    if (nonBlocking == null) {
      nonBlocking = Schedulers.parallel();
    }
    return nonBlocking;
  }

  public static <T> Flux<T> schedule(final Flux<T> f, final Scheduler scheduler) {
    return f.subscribeOn(scheduler).publishOn(task());
  }

  public static <T> Mono<T> schedule(final Mono<T> f, final Scheduler scheduler) {
    return f.subscribeOn(scheduler).publishOn(task());
  }

  public static <T> Flux<T> scheduleBlocking(final Flux<T> publisher) {
    final Scheduler scheduler = blocking();
    return schedule(publisher, scheduler);
  }

  public static <T> Mono<T> scheduleBlocking(final Mono<T> publisher) {
    final Scheduler scheduler = blocking();
    return schedule(publisher, scheduler);
  }

  public static <T> Flux<T> scheduleNonBlocking(final Flux<T> publisher) {
    final Scheduler scheduler = nonBlockingIo();
    return schedule(publisher, scheduler);
  }

  public static <T> Mono<T> scheduleNonBlocking(final Mono<T> publisher) {
    final Scheduler scheduler = nonBlockingIo();
    return schedule(publisher, scheduler);
  }

  public static Scheduler task() {
    if (task == null) {
      task = Schedulers.boundedElastic();
    }
    return task;
  }

}
