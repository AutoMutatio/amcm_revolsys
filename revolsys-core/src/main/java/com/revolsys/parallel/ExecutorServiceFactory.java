package com.revolsys.parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.revolsys.collection.value.LazyValueHolder;
import com.revolsys.collection.value.ValueHolder;

public class ExecutorServiceFactory {
  private static LazyValueHolder<ExecutorService> EXECUTOR = ValueHolder
      .lazy(Executors::newCachedThreadPool);

  private static LazyValueHolder<ScheduledExecutorService> SCHEDULED_VIRTUAL = ValueHolder
      .lazy(() -> Executors.newScheduledThreadPool(0, Thread.ofVirtual().name("rssched", 0).factory()));

  public static ExecutorService getExecutorService() {
    return EXECUTOR.getValue();
  }

  public static ScheduledExecutorService getScheduledVirtual() {
    return SCHEDULED_VIRTUAL.getValue();
  }
}
