package com.revolsys.parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.revolsys.collection.value.LazyValueHolder;
import com.revolsys.collection.value.ValueHolder;

public class ExecutorServiceFactory {
  private static LazyValueHolder<ExecutorService> EXECUTOR = ValueHolder
    .lazy(Executors::newCachedThreadPool);

  public static ExecutorService getExecutorService() {
    return EXECUTOR.getValue();
  }

}
