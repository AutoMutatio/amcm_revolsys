package com.revolsys.util.concurrent;

import java.lang.Thread.Builder;
import java.lang.Thread.Builder.OfPlatform;
import java.lang.Thread.Builder.OfVirtual;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Function;

import com.revolsys.collection.iterator.ForEachHandler;
import com.revolsys.collection.iterator.ForEachMethods;
import com.revolsys.collection.iterator.RunableMethods;
import com.revolsys.parallel.SemaphoreEx;

public class ThreadFactoryEx implements ThreadFactory, ForEachMethods, RunableMethods {

  public static ThreadFactoryEx builder(final Builder builder) {
    final var factory = builder.factory();
    return new ThreadFactoryEx(factory);
  }

  public static ThreadFactoryEx platform(final Consumer<OfPlatform> configurer) {
    final var builder = Thread.ofPlatform();
    if (configurer != null) {
      configurer.accept(builder);
    }
    return builder(builder);
  }

  public static ThreadFactoryEx virtual(final Consumer<OfVirtual> configurer) {
    final var builder = Thread.ofVirtual();
    if (configurer != null) {
      configurer.accept(builder);
    }
    return builder(builder);
  }

  private final ThreadFactory factory;

  private String name;

  public ThreadFactoryEx(final String name, final ThreadFactory factory) {
    this.name = name;
    this.factory = factory;
  }

  public ThreadFactoryEx(final ThreadFactory factory) {
    this.factory = factory;
  }

  @Override
  public <V> void forEach(final Consumer<? super V> action, final ForEachHandler<V> forEach) {
    this.scope(scope -> forEach.forEach(scope.forkConsumerValue(action)));
  }

  public String name() {
    return this.name;
  }

  @Override
  public Thread newThread(final Runnable r) {
    return this.factory.newThread(r);
  }

  public ExecutorService newThreadPerTaskExecutor() {
    return Executors.newThreadPerTaskExecutor(this.factory);
  }

  @Override
  public <V> void run(final ForEachHandler<Runnable> forEach) {
    scope(scope -> scope.run(forEach));
  }

  public <V> void run(final Runnable action) {
    scope(scope -> scope.run(action));
  }

  public <V> void scope(final Consumer<StructuredTaskScopeEx<V>> action) {
    new LambdaStructuredTaskScope.Builder<V>(this.name, this.factory).throwErrors()
      .join(action);
  }

  public <V> void scope(final String name, final Consumer<StructuredTaskScopeEx<V>> action) {
    new LambdaStructuredTaskScope.Builder<V>(name, this.factory).throwErrors()
      .join(action);
  }

  public <V> V scope(final String name, final Function<StructuredTaskScopeEx<V>, V> action) {
    return new LambdaStructuredTaskScope.Builder<V>(name, this.factory).throwErrors()
      .join(action);
  }

  public <V> void scopeBuild(final String name,
    final Consumer<LambdaStructuredTaskScope.Builder<V>> action) {
    action.accept(new LambdaStructuredTaskScope.Builder<V>(name, this.factory));
  }

  public <V> V scopeBuild(final String name,
    final Function<LambdaStructuredTaskScope.Builder<V>, V> action) {
    return action.apply(new LambdaStructuredTaskScope.Builder<V>(name, this.factory));
  }

  public SemaphoreScope semaphore(final int permits) {
    return semaphore(new SemaphoreEx(permits));
  }

  public SemaphoreScope semaphore(final SemaphoreEx semaphore) {
    return new SemaphoreScope(semaphore, this);
  }

  @Override
  public String toString() {
    return this.name;
  }

}
