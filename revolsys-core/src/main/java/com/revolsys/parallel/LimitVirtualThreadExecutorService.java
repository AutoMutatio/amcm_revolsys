package com.revolsys.parallel;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.revolsys.exception.Exceptions;

public class LimitVirtualThreadExecutorService extends AbstractExecutorService {
  private class TaskRunner implements Runnable {
    private final Runnable command;

    private Thread thread;

    public TaskRunner(final Runnable command) {
      this.command = command;
    }

    public void interrupt() {
      this.thread.interrupt();
    }

    @Override
    public void run() {
      try {
        if (!isShutdown()) {
          this.command.run();
        }
      } finally {
        try (
          var l = LimitVirtualThreadExecutorService.this.lock.lockX()) {
          LimitVirtualThreadExecutorService.this.runners.remove(this);
        }
        LimitVirtualThreadExecutorService.this.semaphore.release();
      }
    }
  }

  private final Set<TaskRunner> runners = new LinkedHashSet<>();

  private final CountDownLatch shutdown = new CountDownLatch(1);

  private final Semaphore semaphore;

  private final ReentrantLockEx lock = new ReentrantLockEx();

  public LimitVirtualThreadExecutorService(final int maxThreads) {
    this.semaphore = new Semaphore(maxThreads);
  }

  @Override
  public boolean awaitTermination(final long timeout, final TimeUnit unit)
    throws InterruptedException {
    return this.shutdown.await(timeout, unit);
  }

  @Override
  public void execute(final Runnable command) {
    try {
      this.semaphore.acquire();
      final var runner = new TaskRunner(command);

      try {
        runner.thread = Thread.startVirtualThread(runner);
        try (
          var l = this.lock.lockX()) {
          this.runners.add(runner);
        }
        runner.thread.start();
      } catch (RuntimeException | Error e) {
        throw e;
      }
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @Override
  public boolean isShutdown() {
    return this.shutdown.getCount() == 0;
  }

  @Override
  public boolean isTerminated() {
    return isShutdown();
  }

  @Override
  public void shutdown() {
    try (
      var l = this.lock.lockX()) {
      for (final TaskRunner runner : this.runners) {
        runner.interrupt();
      }
      this.runners.clear();
      this.shutdown.countDown();
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown();
    return Collections.emptyList();
  }

}
