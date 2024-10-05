package com.revolsys.util.concurrent;

import java.util.function.Consumer;

import com.revolsys.collection.iterator.ForEachHandler;
import com.revolsys.collection.iterator.ForEachMethods;
import com.revolsys.collection.iterator.RunableMethods;
import com.revolsys.parallel.SemaphoreEx;

public class SemaphoreScope implements ForEachMethods, RunableMethods {
  private final SemaphoreEx semaphore;

  private final ThreadFactoryEx threadFactory;

  public SemaphoreScope(final SemaphoreEx semaphore, final ThreadFactoryEx threadFactory) {
    this.semaphore = semaphore;
    this.threadFactory = threadFactory;
  }

  @Override
  public <V> void forEach(final Consumer<? super V> action, final ForEachHandler<V> forEach) {
    this.threadFactory
      .scope(scope -> forEach.forEach(value -> scope.fork(this.semaphore, action, value)));
  }

  @Override
  public <V> void run(final ForEachHandler<Runnable> forEach) {
    this.threadFactory
      .scope(scope -> forEach.forEach(action -> scope.fork(this.semaphore, action)));
  }

  public <V> void run(final Runnable action) {
    this.threadFactory.scope(scope -> scope.fork(this.semaphore, action));
  }
}
