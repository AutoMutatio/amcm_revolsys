package com.revolsys.parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jeometry.common.collection.value.LazyValueHolder;
import org.jeometry.common.collection.value.ValueHolder;

public class ExecutorServiceFactory {
  private static LazyValueHolder<ExecutorService> EXECUTOR = ValueHolder
    .lazy(Executors::newCachedThreadPool);

  public static ExecutorService getExecutorService() {
    return EXECUTOR.getValue();
  }

}
