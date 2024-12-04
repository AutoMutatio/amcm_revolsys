package com.revolsys.util.concurrent;

import java.lang.Thread.Builder.OfPlatform;
import java.lang.Thread.Builder.OfVirtual;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.revolsys.collection.value.ValueHolder;
import com.revolsys.exception.Exceptions;
import com.revolsys.logging.Slf4jUncaughtExceptionHandler;

public class Concurrent {

  private static final AtomicLong THREAD_INDEX = new AtomicLong();

  private static final ValueHolder<ScheduledExecutorService> VIRTUAL_SCHEDULED = ValueHolder
    .lazy(() -> virtual("scheduled").newScheduledThreadPool());

  public static ClassLoader contextClassLoader() {
    return Thread.currentThread()
      .getContextClassLoader();
  }

  public static ThreadFactoryEx platform() {
    return platform("Platform" + THREAD_INDEX.incrementAndGet() + "-");
  }

  public static ThreadFactoryEx platform(final String prefix) {
    final var factory = Thread.ofPlatform()
      .uncaughtExceptionHandler(Slf4jUncaughtExceptionHandler.INSTANCE)
      .name(prefix, 0)
      .factory();
    return new ThreadFactoryEx(prefix, factory);
  }

  public static ThreadFactoryEx platform(final String name, final Consumer<OfPlatform> configurer) {
    final var builder = Thread.ofPlatform()
      .uncaughtExceptionHandler(Slf4jUncaughtExceptionHandler.INSTANCE);
    if (configurer != null) {
      configurer.accept(builder);
    }
    final var factory = builder.factory();
    return new ThreadFactoryEx(name, factory);
  }

  public static void sleep(final Duration duration) {
    if (duration != null) {
      try {
        Thread.sleep(duration);
      } catch (final InterruptedException e) {
        throw Exceptions.toRuntimeException(e);
      }
    }
  }

  public static void sleepUntil(final Instant instant) {
    if (instant != null) {
      try {
        final var duration = Duration.between(Instant.now(), instant);
        Thread.sleep(duration);
      } catch (final InterruptedException e) {
        throw Exceptions.toRuntimeException(e);
      }
    }
  }

  public static String threadName() {
    return Thread.currentThread()
      .getName();
  }

  public static ThreadFactoryEx virtual() {
    return virtual("Virtual" + THREAD_INDEX.incrementAndGet() + "-");
  }

  public static ThreadFactoryEx virtual(final String prefix) {
    final var factory = Thread.ofVirtual()
      .uncaughtExceptionHandler(Slf4jUncaughtExceptionHandler.INSTANCE)
      .name(prefix, 0)
      .factory();
    return new ThreadFactoryEx(prefix, factory);
  }

  public static ThreadFactoryEx virtual(final String name, final Consumer<OfVirtual> configurer) {
    final var builder = Thread.ofVirtual()
      .uncaughtExceptionHandler(Slf4jUncaughtExceptionHandler.INSTANCE);
    if (configurer != null) {
      configurer.accept(builder);
    }
    final var factory = builder.factory();
    return new ThreadFactoryEx(name, factory);
  }

  public static ScheduledExecutorService virtualSceduled() {
    return VIRTUAL_SCHEDULED.get();
  }
}
