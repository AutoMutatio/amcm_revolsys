package com.revolsys.reactive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.scheduler.Scheduler;

public class LimitScheduler implements Scheduler {

  private static abstract class AbstractLimitSchedulerTask implements Disposable, Queueable {

    private Runnable task;

    private Disposable disposable;

    private boolean disposed;

    public AbstractLimitSchedulerTask(final Runnable task) {
      this.task = task;
    }

    @Override
    public void dispose() {
      Disposable disposable;
      synchronized (this) {
        disposable = this.disposable;
        postDisposed();
      }
      if (disposable != null && !disposable.isDisposed()) {
        disposable.dispose();
        disposeDo();
      }
    }

    protected abstract void disposeDo();

    @Override
    public boolean isDisposed() {
      synchronized (this) {
        if (this.disposed) {
          return true;
        } else if (this.disposable != null && this.disposable.isDisposed()) {
          postDisposed();
          return true;
        } else {
          return false;
        }
      }
    }

    private void postDisposed() {
      this.disposed = true;
      this.disposable = null;
      this.task = null;
    }

    public void run() {
      Runnable task;
      synchronized (this) {
        task = this.task;
      }
      if (task != null && !this.disposed) {
        try {
          task.run();
        } finally {
          disposeDo();
          postDisposed();
        }
      }
    }

    public void schedule(final Worker worker) {
      this.disposable = worker.schedule(this.task);
    }

    @Override
    public void start() {
      this.disposable = startDo();
    }

    protected abstract Disposable startDo();
  }

  private class LimitSchedulerTask extends AbstractLimitSchedulerTask {

    public LimitSchedulerTask(final Runnable task) {
      super(task);
    }

    @Override
    protected void disposeDo() {
      onDispose(this);
    }

    @Override
    protected Disposable startDo() {
      return LimitScheduler.this.scheduler.schedule(this::run);
    }
  }

  private final class LimitSchedulerWorker implements Worker, Queueable {

    private class Task extends AbstractLimitSchedulerTask {

      public Task(final Runnable task) {
        super(task);
      }

      @Override
      protected void disposeDo() {
        LimitSchedulerWorker.this.disposables.remove(this);
      }

      @Override
      protected Disposable startDo() {
        return LimitSchedulerWorker.this.worker.schedule(this::run);
      }
    }

    private Worker worker;

    private Composite disposables = Disposables.composite();

    private final boolean disposed = false;

    private List<Queueable> queue = new ArrayList<>();

    @Override
    public void dispose() {
      Composite disposables = null;
      Worker worker = null;

      synchronized (this) {
        if (!this.disposed) {
          disposables = this.disposables;
          this.disposables = null;
          worker = this.worker;
          this.worker = null;
        }
      }
      if (disposables != null) {
        disposables.dispose();
      }
      if (worker != null) {
        worker.dispose();
      }
      onDispose(this);
    }

    @Override
    public boolean isDisposed() {
      return this.disposables.isDisposed();
    }

    @Override
    public Disposable schedule(final Runnable action) {
      final LimitSchedulerTask task = new LimitSchedulerTask(action);
      synchronized (this) {
        this.disposables.add(task);
        if (this.worker == null) {
          this.queue.add(task);
        } else {
          task.schedule(this.worker);
        }

      }
      return task;
    }

    @Override
    public void start() {
      synchronized (this) {
        if (this.worker == null) {
          this.worker = LimitScheduler.this.scheduler.createWorker();
          for (final Queueable task : this.queue) {
            task.start();
          }
          this.queue = null;
        }
      }

    }
  }

  private interface Queueable extends Disposable {
    public void start();
  }

  static final AtomicLong EVICTOR_COUNTER = new AtomicLong();

  static final ThreadFactory EVICTOR_FACTORY = r -> {
    final Thread t = new Thread(r, "limit" + "-evictor-" + EVICTOR_COUNTER.incrementAndGet());
    t.setDaemon(true);
    return t;
  };

  private boolean disposed;

  private final Scheduler scheduler;

  private final int limit;

  private final int maxQueueSize;

  private final Deque<Queueable> queue = new LinkedList<>();

  private final Set<Disposable> active = new LinkedHashSet<>();

  private final Set<Disposable> disposables = new LinkedHashSet<>();

  private ScheduledFuture<?> cleaner$;

  private String name;

  LimitScheduler(final Scheduler scheduler, final int paralellLimit, final int queueSize,
    final String suffix) {
    this.scheduler = scheduler;
    this.limit = paralellLimit;
    this.maxQueueSize = paralellLimit + Math.max(0, queueSize);
    if (suffix == null) {
      this.name = "limit";
    } else {
      this.name = "limit" + "-" + suffix;
    }
  }

  @Override
  public Worker createWorker() {
    final LimitSchedulerWorker worker = new LimitSchedulerWorker();
    enqueue(worker);
    return worker;
  }

  private Queueable dequeue() {
    synchronized (this.queue) {
      if (!this.queue.isEmpty()) {
        if (this.active.size() < this.limit) {
          final Queueable item = this.queue.removeFirst();
          this.active.add(item);
          return item;
        }
      }
      return null;
    }
  }

  @Override
  public void dispose() {

    boolean disposed;
    synchronized (this.queue) {
      disposed = this.disposed;
      this.disposed = true;
    }
    if (disposed) {
      if (this.cleaner$ != null) {
        this.cleaner$.cancel(true);
      }
      for (final Collection<? extends Disposable> disposables : Arrays.asList(this.active,
        this.disposables)) {
        for (final Disposable disposable : disposables) {
          disposable.dispose();
        }
        disposables.clear();
      }
    }

  }

  private void enqueue(final Queueable task) {
    synchronized (this.queue) {
      if (this.disposed) {
        throw new IllegalStateException("Scheduler is closed");
      } else if (this.queue.size() < this.maxQueueSize) {
        this.queue.addLast(task);
      } else {
        throw new IllegalStateException("Maximum number of queued tasks exceeded");
      }
    }
    reschedule();
  }

  private void eviction() {
    try {
      for (final Iterator<Disposable> iterator = this.disposables.iterator(); iterator.hasNext();) {
        final Disposable disposable = iterator.next();
        if (disposable.isDisposed()) {
          iterator.remove();
        }
      }
    } catch (final ConcurrentModificationException e) {
      // Ignore as it will get run next time
    }
    reschedule();
  }

  @Override
  public boolean isDisposed() {
    return this.disposed;
  }

  private void onDispose(final Queueable item) {
    synchronized (this.queue) {
      if (!this.disposed) {
        this.active.remove(item);
        this.disposables.remove(item);
      }
    }
    reschedule();
  }

  private void reschedule() {
    Queueable itemToSchedule;
    do {
      itemToSchedule = dequeue();
      if (itemToSchedule != null) {
        itemToSchedule.start();
      }
    } while (itemToSchedule != null);
  }

  @Override
  public Disposable schedule(final Runnable action) {
    final LimitSchedulerTask task = new LimitSchedulerTask(action);
    enqueue(task);
    return task;
  }

  @Override
  public void start() {
    final ScheduledExecutorService e = Executors.newScheduledThreadPool(1, EVICTOR_FACTORY);
    try {
      this.cleaner$ = e.scheduleAtFixedRate(this::eviction, 60L, 60L, TimeUnit.SECONDS);
    } catch (final RejectedExecutionException ree) {
      // the executor was most likely shut down in parallel
      if (!isDisposed()) {
        throw ree;
      } // else swallow
    }
  }

  @Override
  public String toString() {
    return this.name.toString();
  }

}
