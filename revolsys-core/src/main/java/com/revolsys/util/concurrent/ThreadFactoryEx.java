package com.revolsys.util.concurrent;

import java.lang.management.ThreadInfo;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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

import com.revolsys.collection.iterator.BaseIterable;
import com.revolsys.collection.iterator.ForEachHandler;
import com.revolsys.collection.iterator.ForEachMethods;
import com.revolsys.collection.iterator.Iterables;
import com.revolsys.collection.iterator.RunnableMethods;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.Jsonable;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.set.Sets;

public class ThreadFactoryEx implements ThreadFactory, ForEachMethods<ThreadFactoryEx>,
  RunnableMethods<ThreadFactoryEx>, ExecutorService, Jsonable {
  private static final Set<Reference<ThreadFactoryEx>> FACTORIES = ConcurrentHashMap.newKeySet();

  private static final ReferenceQueue<ThreadFactoryEx> FACTORY_QUEUE = new ReferenceQueue<>();

  private static final Set<String> ignoreThreadNames = Sets
    .newHash(Lists.newArray("JDWP Transport Listener: dt_socket", "JDWP Event Helper Thread",
      "JDWP Command Reader", "RMI TCP Accept-59634", "RMI TCP Accept-0", "Live Reload Server",
      "DestroyJavaVM", "Reference Handler", "Finalizer", "File Watcher"));

  private static void cleanupReferences() {
    for (var ref = FACTORY_QUEUE.poll(); ref != null; ref = FACTORY_QUEUE.poll()) {
      FACTORIES.remove(ref);
    }
  }

  public static BaseIterable<ThreadFactoryEx> factories() {
    return Iterables.fromIterable(FACTORIES).map(Reference::get).filter(f -> f != null);
  }

  private static boolean hideThread(final String name) {
    if (name.startsWith("RMI TCP ")) {
      return true;
    } else if (name.startsWith("JMX ")) {
      return true;
    } else if (name.startsWith("container-")) {
      return true;
    } else if (name.endsWith("-Poller")) {
      return true;
    } else if (name.endsWith("-Acceptor")) {
      return true;
    } else if (ignoreThreadNames.contains(name)) {
      return true;
    }
    return false;
  }

  public static boolean hideThread(final Thread t) {
    final var threadName = t.getName();
    if (hideThread(threadName)) {
      return false;
    }

    final var stackTrace = t.getStackTrace();
    if (stackTrace.length == 0) {
      return false;
      // } else if (stackTrace[0].toString()
      // .contains("jdk.internal.misc.Unsafe.park")) {
      // return false;
      // } else if (stackTrace[0].toString()
      // .contains("java.lang.VirtualThread.park")) {
      // return false;
    } else {
      return true;
    }
  }

  public static boolean hideThread(final ThreadInfo t) {
    final var threadName = t.getThreadName();
    if (hideThread(threadName)) {
      return false;
    }

    final var stackTrace = t.getStackTrace();
    if (stackTrace.length == 0) {
      return false;
    } else {
      return true;
    }
  }

  private final ThreadFactory factory;

  private String name;

  private final ExecutorService executorService;

  private final Set<Thread> threads = ConcurrentHashMap.newKeySet();

  ThreadFactoryEx(final String name, final ThreadFactory factory) {
    this(factory);
    this.name = name;
  }

  ThreadFactoryEx(final ThreadFactory factory) {
    this.factory = factory;
    this.executorService = Executors.newThreadPerTaskExecutor(this);
    FACTORIES.add(new WeakReference<ThreadFactoryEx>(this, FACTORY_QUEUE));
    cleanupReferences();
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
  public <V> ThreadFactoryEx forEach(final ForEachHandler<V> forEach,
    final Consumer<? super V> action) {
    try (
      var parallel = new Parallel(this)) {
      parallel.forEach(forEach, action);
    }
    return this;
  }

  public boolean hasThreads() {
    return !this.threads.isEmpty();
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
    final var thread = this.factory.newThread(() -> {
      try {
        r.run();
      } finally {
        final var removeThread = Thread.currentThread();
        this.threads.remove(removeThread);
        // System.out.println("finish\t" + this.name + "\t" +
        // this.threads.size());
      }
    });
    this.threads.add(thread);
    // System.out.println("start\t" + this.name + "\t" + this.threads.size());

    thread.setUncaughtExceptionHandler(null);
    return thread;
  }

  public ExecutorService newThreadPerTaskExecutor() {
    return Executors.newThreadPerTaskExecutor(this);
  }

  public Parallel parallel() {
    return new Parallel(this);
  }

  public ThreadFactoryEx parallel(final Consumer<Parallel> action) {
    try (
      var parallel = new Parallel(this)) {
      action.accept(parallel);
    }
    return this;
  }

  public ThreadFactoryEx parallel(final Runnable... tasks) {
    try (
      var parallel = new Parallel(this)) {
      parallel.run(tasks);
    }
    return this;
  }

  @Override
  public ThreadFactoryEx run(final ForEachHandler<Runnable> forEach) {
    try (
      var parallel = parallel()) {
      parallel.run(forEach);
    }
    return this;
  }

  public ThreadFactoryEx run(final Runnable action) {
    try (
      var parallel = parallel()) {
      parallel.run(action);
    }
    return this;
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

  public Future<?> submit(final Consumer<ThreadFactoryEx> action) {
    return submit(() -> action.accept(this));
  }

  public <V> Future<V> submit(final Function<ThreadFactoryEx, V> action) {
    return submit(() -> action.apply(this));
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
  public JsonObject toJson() {
    final var json = JsonObject.hash()
      .addValue("name", this.name)
      .addValue("threads",
        Iterables.fromIterable(this.threads)
          .filter(ThreadFactoryEx::hideThread)
          .map(this::toJson)
          .toList());
    json.removeEmptyProperties();
    return json;
  }

  public JsonObject toJson(final Thread thread) {
    final var json = JsonObject.hash()
      .addValue("id", thread.threadId())
      .addValue("name", thread.getName())
      .addValue("priority", thread.getPriority())
      .addNotEmpty("alive", thread.isAlive());

    final boolean daemon = thread.isDaemon();
    if (daemon) {
      json.addValue("daemon", daemon);
    }
    final boolean virtual = thread.isVirtual();
    if (virtual) {
      json.addValue("virtual", virtual);
    }
    final boolean interrupted = thread.isInterrupted();
    if (interrupted) {
      json.addValue("interrupted", interrupted);
    }

    final var stackTraceArray = thread.getStackTrace();
    final var stackTrace = Lists.newArray(stackTraceArray);

    json.addNotEmpty("stackTrace", stackTrace);
    json.removeEmptyProperties();
    return json;
  }

  @Override
  public String toString() {
    return this.name;
  }

}
