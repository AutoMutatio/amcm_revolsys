package com.revolsys.util.concurrent;

import java.lang.Thread.Builder;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

import com.revolsys.collection.iterator.ForEachHandler;
import com.revolsys.collection.iterator.ForEachMethods;
import com.revolsys.collection.iterator.RunableMethods;
import com.revolsys.parallel.SemaphoreEx;

public class ThreadFactoryEx
  implements ThreadFactory, ForEachMethods, RunableMethods, ExecutorService {

  public static ThreadFactoryEx builder(final Builder builder) {
    final var factory = builder.factory();
    return new ThreadFactoryEx(factory);
  }

  private final ThreadFactory factory;

  private String name;

  private final ExecutorService executorService;

  public ThreadFactoryEx(final String name, final ThreadFactory factory) {
    this(factory);
    this.name = name;
  }

  public ThreadFactoryEx(final ThreadFactory factory) {
    this.factory = factory;
    this.executorService = Executors.newThreadPerTaskExecutor(this);
  }

  @Override
  public boolean awaitTermination(final long timeout, final TimeUnit unit)
    throws InterruptedException {
    return this.executorService.awaitTermination(timeout, unit);
  }

  @Override
  public void execute(final Runnable command) {
    this.executorService.execute(command);
  }

  @Override
  public <V> void forEach(final Consumer<? super V> action, final ForEachHandler<V> forEach) {
    this.scope(scope -> forEach.forEach(scope.forkConsumerValue(action)));
  }

  @Override
  public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks)
    throws InterruptedException {
    return this.executorService.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks,
    final long timeout, final TimeUnit unit) throws InterruptedException {
    return this.executorService.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(final Collection<? extends Callable<T>> tasks)
    throws InterruptedException, ExecutionException {
    return this.executorService.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout,
    final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return this.executorService.invokeAny(tasks, timeout, unit);
  }

  @Override
  public boolean isShutdown() {
    return this.executorService.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return this.executorService.isTerminated();
  }

  public String name() {
    return this.name;
  }

  public ExecutorService newFixedThreadPool(final int nThreads) {
    return Executors.newFixedThreadPool(nThreads);
  }

  public ScheduledExecutorService newScheduledThreadPool() {
    return Executors.newScheduledThreadPool(0, this);
  }

  public ScheduledExecutorService newScheduledThreadPool(final int corePoolSize) {
    return new ScheduledThreadPoolExecutor(corePoolSize, this);
  }

  @Override
  public Thread newThread(final Runnable r) {
    return this.factory.newThread(r);
  }

  public ExecutorService newThreadPerTaskExecutor() {
    return Executors.newThreadPerTaskExecutor(this);
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
  public void shutdown() {
    this.executorService.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return this.executorService.shutdownNow();
  }

  public Thread start(final Runnable action) {
    final var thread = newThread(action);
    thread.start();
    return thread;
  }

  @Override
  public <T> Future<T> submit(final Callable<T> task) {
    return this.executorService.submit(task);
  }

  @Override
  public Future<?> submit(final Runnable task) {
    return this.executorService.submit(task);
  }

  @Override
  public <T> Future<T> submit(final Runnable task, final T result) {
    return this.executorService.submit(task, result);
  }

  @Override
  public String toString() {
    return this.name;
  }

}
