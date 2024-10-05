package com.revolsys.util.concurrent;

import java.util.function.Consumer;

import com.revolsys.collection.iterator.ForEachHandler;
import com.revolsys.collection.iterator.ForEachMethods;
import com.revolsys.collection.iterator.RunableMethods;

public class UnboundedScope implements ForEachMethods, RunableMethods {

  private final ThreadFactoryEx threadFactory;

  public UnboundedScope(final ThreadFactoryEx threadFactory) {
    this.threadFactory = threadFactory;
  }

  @Override
  public <V> void forEach(final Consumer<? super V> action, final ForEachHandler<V> forEach) {
    this.threadFactory.scope(scope -> forEach.forEach(scope.forkConsumerValue(action)));
  }

  @Override
  public <V> void run(final ForEachHandler<Runnable> forEach) {
    this.threadFactory.scope(scope -> forEach.forEach(scope.forkConsumerRunnable()));
  }

  public <V> void run(final Runnable action) {
    this.threadFactory.scope(scope -> scope.fork(action));
  }
}
