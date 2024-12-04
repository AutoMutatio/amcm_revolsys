package com.revolsys.util.concurrent;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.revolsys.date.Dates;
import com.revolsys.http.HttpThrottle;
import com.revolsys.parallel.SemaphoreEx;

public class RateLimiter {

  private record Pause(Instant time, int permits) {

    /**
     * Create a new Pause with the new time and the current permits;
     *
     * @param newTime
     * @return
     */
    private Pause time(final Instant newTime) {
      return new Pause(newTime, this.permits);
    }

    private void sleep() {
      Concurrent.sleepUntil(this.time);
    }

    private boolean expired() {
      return Instant.now()
        .isAfter(this.time);
    }

    private boolean releaseIfExpired(final RateLimiter limiter) {
      if (expired()) {
        limiter.limit.release(this.permits);
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

  /**
   * Construct a new RateLimiter.
   *
   * @param limit The limit (maximum number of permits) during the time period.
   * @param period The time period the limit.
   * @param intervalCount The number of intervals to split the period into. This creates a rolling period. 4 would be a good starting point.
   */
  public RateLimiter(final int limit, final Duration period, final int intervalCount) {
    this.limit = new SemaphoreEx(limit);
    final var periodMillis = period.toMillis();
    final var intervalMillis = Math.floorDiv(periodMillis, intervalCount);
    if (intervalMillis * intervalCount != periodMillis) {
      throw new IllegalArgumentException(
        "Duration " + period + " must be divisable by " + intervalCount);
    }
    this.intervalCount = intervalCount;
    this.divisionMillis = intervalMillis;
    this.future = Concurrent.virtualSceduled()
      .scheduleAtFixedRate(this::removeExpired, intervalMillis / 2, intervalMillis / 2,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Acquire permission to continue if the rate limit has not been reached.
   */
  public void aquire() {
    this.limit.acquireX();
    final long interval = currentInterval();
    this.permitsByInterval.compute(interval, this::increment);
  }

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

  private void pause(final Instant time, final Duration duration) {
    if (duration.isPositive()) {
      final var paused = this.paused.compute(PAUSE_KEY, (k, oldValue) -> {
        if (oldValue == null) {
          final int permits = this.limit.drainPermits();
          return new Pause(time, permits);
        } else {
          return oldValue.time(time);
        }
      });
      try {
        paused.sleep();
        removeExpired();
        while (true) {
          final Pause currentPause = this.paused.get(PAUSE_KEY);
          if (currentPause == null) {
            break;
          } else {
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
  public void pauseFor(final Duration duration) {
    final Instant time = Instant.now()
      .plus(duration);
    pause(time, duration);
  }

  public void pauseHttpRetryAfter(final String retryAfter) {
    if (retryAfter == null) {
      pauseFor(HttpThrottle.DEFAULT_RETRY);
    } else {
      try {
        final int retryAfterSeconds = Integer.parseInt(retryAfter);
        if (retryAfterSeconds > 0) {
          final var duration = Duration.ofSeconds(retryAfterSeconds);
          pauseFor(duration);
        }
      } catch (final NumberFormatException e1) {
        final var time = Dates.RFC_1123_DATE_TIME.parse(retryAfter, Instant::from);
        pauseUtil(time);
      }
    }
  }

  /**
   * Pause the rate limiter until the specified time. This can be used where a service has requested
   * rate limiting (e.g HTTP 429 with Retry-After header).
   *
   * @param duration The duration to pause for.
   */
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
              final var count = entry.getValue()
                .get();
              this.limit.release(count);
            }
            return pause;
          });

          return paused == null;
        } else {
          return false;
        }
      });
  }

}
