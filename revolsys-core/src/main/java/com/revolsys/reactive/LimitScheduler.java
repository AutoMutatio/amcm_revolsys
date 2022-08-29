package com.revolsys.reactive;

import java.util.ArrayList;
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

  private class LimitSchedulerTask implements Disposable {

    private Runnable task;

    private Disposable disposable;

    private boolean disposed;

    public LimitSchedulerTask(final Runnable task) {
      this.task = task;
    }

    private boolean cleanIfNeeded() {
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

    @Override
    public void dispose() {
      Disposable disposable;
      synchronized (this) {
        disposable = this.disposable;
        postDisposed();
      }
      if (disposable != null && !disposable.isDisposed()) {
        disposable.dispose();
        onDispose(this);
      }
    }

    @Override
    public boolean isDisposed() {
      return this.disposed;
    }

    private void postDisposed() {
      this.disposed = true;
      this.disposable = null;
      this.task = null;
    }

    private void run() {
      Runnable task;
      synchronized (this) {
        task = this.task;
      }
      if (task != null && !LimitScheduler.this.schedulerDisposed && !this.disposed) {
        try {
          task.run();
        } finally {
          onDispose(this);
          postDisposed();
        }
      }
    }

    private void runOnScheduler(final Scheduler scheduler) {
      this.disposable = scheduler.schedule(this::run);
    }
  }

  private final class LimitSchedulerWorker implements Worker {

    final Composite disposables = Disposables.composite();

    @Override
    public void dispose() {
      this.disposables.dispose();
    }

    @Override
    public boolean isDisposed() {
      return this.disposables.isDisposed();
    }

    @Override
    public Disposable schedule(final Runnable task) {
      final Disposable disposable = LimitScheduler.this.schedule(task);
      this.disposables.add(disposable);
      return disposable;
    }

  }

  static final AtomicLong EVICTOR_COUNTER = new AtomicLong();

  static final ThreadFactory EVICTOR_FACTORY = r -> {
    final Thread t = new Thread(r, "limit" + "-evictor-" + EVICTOR_COUNTER.incrementAndGet());
    t.setDaemon(true);
    return t;
  };

  private boolean schedulerDisposed;

  private final Scheduler scheduler;

  private final int limit;

  private final int queueSize;

  private final Deque<LimitSchedulerTask> queuedTasks = new LinkedList<>();

  private final Set<LimitSchedulerTask> scheduledTasks = new LinkedHashSet<>();

  private ScheduledFuture<?> cleaner$;

  public LimitScheduler(final Scheduler scheduler, final int paralellLimit, final int queueSize) {
    this.scheduler = scheduler;
    this.limit = paralellLimit;
    this.queueSize = paralellLimit + Math.max(0, queueSize);
  }

  @Override
  public Worker createWorker() {
    return new LimitSchedulerWorker();
  }

  @Override
  public void dispose() {
    if (!this.schedulerDisposed) {
      this.schedulerDisposed = true;
      if (this.cleaner$ != null) {
        this.cleaner$.cancel(true);
      }
      final List<LimitSchedulerTask> tasksToClean = new ArrayList<>();
      synchronized (this.queuedTasks) {
        tasksToClean.addAll(this.queuedTasks);
        this.queuedTasks.clear();
      }
      synchronized (this.scheduledTasks) {
        tasksToClean.addAll(this.scheduledTasks);
        this.scheduledTasks.clear();
      }
      for (final LimitSchedulerTask task : tasksToClean) {
        task.dispose();
      }
    }
  }

  private void eviction() {
    synchronized (this.scheduledTasks) {
      if (!this.schedulerDisposed) {
        for (final Iterator<LimitSchedulerTask> iterator = this.scheduledTasks.iterator(); iterator
          .hasNext();) {
          final LimitSchedulerTask limitDisposable = iterator.next();
          if (limitDisposable.cleanIfNeeded()) {
            iterator.remove();
          }
        }
      }
    }
    reschedule();
  }

  private LimitSchedulerTask nextTask() {
    synchronized (this.queuedTasks) {
      if (this.queuedTasks.isEmpty()) {
        return null;
      } else {
        return this.queuedTasks.removeFirst();
      }
    }
  }

  private void onDispose(final LimitSchedulerTask task) {
    synchronized (this.scheduledTasks) {
      this.scheduledTasks.remove(task);
    }
    reschedule();
  }

  private void reschedule() {
    LimitSchedulerTask taskToSchedule;
    do {
      synchronized (this.scheduledTasks) {
        if (this.scheduledTasks.size() < this.limit) {
          taskToSchedule = nextTask();
          if (taskToSchedule != null) {
            this.scheduledTasks.add(taskToSchedule);
          }
        } else {
          taskToSchedule = null;
        }
      }
      if (taskToSchedule != null && !this.schedulerDisposed) {
        taskToSchedule.runOnScheduler(this.scheduler);
      }
    } while (taskToSchedule != null);
  }

  @Override
  public Disposable schedule(final Runnable task) {
    final LimitSchedulerTask disposable = new LimitSchedulerTask(task);
    synchronized (this.queuedTasks) {
      if (this.schedulerDisposed) {
        throw new IllegalStateException("Scheduler is closed");
      } else if (this.queuedTasks.size() < this.queueSize) {
        this.queuedTasks.addLast(disposable);
      } else {
        throw new IllegalStateException("Maximum number of queued tasks exceeded");
      }
    }
    reschedule();
    return disposable;
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

}
