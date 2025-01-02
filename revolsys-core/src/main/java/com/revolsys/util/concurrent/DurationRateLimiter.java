package com.revolsys.util.concurrent;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.revolsys.http.HttpThrottle;
import com.revolsys.parallel.SemaphoreEx;
import com.revolsys.util.Strings;

public class DurationRateLimiter implements RateLimiter, LastAccessProxy {

  private record Pause(Instant time, AtomicInteger permits) {

    /**
     * Create a new Pause with the new time and the current permits;
     *
     * @param newTime
     * @return
     */
    private Pause time(final Instant newTime) {
      if (newTime.isAfter(this.time)) {
        return new Pause(newTime, this.permits);
      } else {
        return this;
      }
    }

    private void sleep() {
      Concurrent.sleepUntil(this.time);
    }

    private boolean expired() {
      return Instant.now()
        .isAfter(this.time);
    }

    private boolean releaseIfExpired(final DurationRateLimiter limiter) {
      if (expired()) {
        HttpThrottle.unThrottle(limiter);
        final var releasePermits = this.permits.getAndAccumulate(0,
          (oldValue, newValue) -> newValue);
        if (releasePermits > 0) {
          limiter.limit.release(releasePermits);
        }
        return true;
      } else {
        return false;
      }
    }

  }

  private static final String PAUSE_KEY = "pause";

  private final SemaphoreEx limit;

  private final long divisionMillis;

  private final ConcurrentHashMap<Long, AtomicInteger> permitsByInterval = new ConcurrentHashMap<>();

  private final ScheduledFuture<?> future;

  private final int intervalCount;

  private final ConcurrentHashMap<String, Pause> paused = new ConcurrentHashMap<>();

  private String name = "";

  private final int limitCount;

  private LastAccess lastAccess = new LastAccess();

  public DurationRateLimiter(final int limit, final Duration period) {
    this(limit, period, 1);
  }

  /**
   * Construct a new RateLimiter.
   *
   * @param limit The limit (maximum number of permits) during the time period.
   * @param period The time period the limit.
   * @param intervalCount The number of intervals to split the period into. This creates a rolling period. 4 would be a good starting point.
   */
  public DurationRateLimiter(final int limit, final Duration period, final int intervalCount) {
    this.limitCount = limit;
    this.limit = new SemaphoreEx(limit);
    final var periodMillis = period.toMillis();
    final var intervalMillis = Math.floorDiv(periodMillis, intervalCount);
    if (intervalMillis * intervalCount != periodMillis) {
      throw new IllegalArgumentException(
        "Duration " + period + " must be divisable by " + intervalCount);
    }
    this.intervalCount = intervalCount;
    this.divisionMillis = intervalMillis;

    // Don't have the cleanup run more than every 2 seconds
    final var taskInterval = Math.max(intervalMillis, 2000);
    this.future = Concurrent.virtualSceduled()
      .scheduleAtFixedRate(this::removeExpired, taskInterval, taskInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Acquire permission to continue if the rate limit has not been reached.
   */
  @Override
  public void aquire() {
    this.lastAccess.access();
    this.limit.acquireX();
    this.lastAccess.access();
    final long interval = currentInterval();
    this.permitsByInterval.compute(interval, this::increment);
  }

  @Override
  public void close() {
    this.future.cancel(true);
  }

  private long currentInterval() {
    return Math.floorDiv(System.currentTimeMillis(), this.divisionMillis);
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    close();
  }

  private AtomicInteger increment(final Long key, AtomicInteger counter) {
    if (counter == null) {
      counter = new AtomicInteger();
    }
    counter.incrementAndGet();
    return counter;
  }

  @Override
  public LastAccess lastAccess() {
    return this.lastAccess;
  }

  public DurationRateLimiter lastAccess(final LastAccess lastAccess) {
    this.lastAccess = lastAccess;
    return this;
  }

  public DurationRateLimiter name(final String name) {
    this.name = name;
    return this;
  }

  private void pause(final Instant time, final Duration duration) {
    this.lastAccess.access();
    if (duration.isPositive()) {
      final var paused = this.paused.compute(PAUSE_KEY, (k, oldValue) -> {
        if (oldValue == null) {
          HttpThrottle.throttle(this, time, duration);
          final int permits = this.limit.drainPermits();
          return new Pause(time, new AtomicInteger(permits));
        } else {
          return oldValue.time(time);
        }
      });
      try {
        paused.sleep();
        this.lastAccess.access();
        removeExpired();
        while (true) {
          final Pause currentPause = this.paused.get(PAUSE_KEY);
          if (currentPause == null) {
            break;
          } else {
            this.lastAccess.access();
            currentPause.sleep();
          }
        }
      } finally {
        removeExpired();
      }
    }
  }

  /**
   * Pause the rate limiter for the duration. This can be used where a service has requested
   * rate limiting (e.g HTTP 429 with Retry-After header).
   *
   * @param duration The duration to pause for.
   */
  @Override
  public void pauseFor(final Duration duration) {
    final Instant time = Instant.now()
      .plus(duration);
    pause(time, duration);
  }

  /**
   * Pause the rate limiter until the specified time. This can be used where a service has requested
   * rate limiting (e.g HTTP 429 with Retry-After header).
   *
   * @param duration The duration to pause for.
   */
  @Override
  public void pauseUtil(final Instant time) {
    final var duration = Duration.between(Instant.now(), time);
    pause(time, duration);
  }

  /**
   * Remove paused and expired interval permits
   */
  private void removeExpired() {
    removeExpiredPause();
    removeExpiredPermits();
  }

  /**
   * Remove an paused
   */
  private void removeExpiredPause() {
    this.paused.entrySet()
      .removeIf(entry -> entry.getValue()
        .releaseIfExpired(this));
  }

  private void removeExpiredPermits() {
    final long expireInterval = currentInterval() - this.intervalCount;
    this.permitsByInterval.entrySet()
      .removeIf(entry -> {
        if (entry.getKey() <= expireInterval) {
          // Run this inside the pauseTime map so that it will only release if
          // a pause doesn't exist
          final var paused = this.paused.compute(PAUSE_KEY, (k, pause) -> {
            if (pause == null) {
              final var releasePermits = entry.getValue()
                .getAndAccumulate(0, (oldValue, newValue) -> newValue);
              if (releasePermits > 0) {
                this.limit.release(releasePermits);
              }
            }
            return pause;
          });

          return paused == null;
        } else {
          return false;
        }
      });
  }

  @Override
  public String toString() {
    return Strings.toString(" ", this.name, this.limitCount);
  }

}
