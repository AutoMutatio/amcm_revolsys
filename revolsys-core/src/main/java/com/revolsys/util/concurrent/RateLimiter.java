package com.revolsys.util.concurrent;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.revolsys.date.Dates;
import com.revolsys.util.BaseCloseable;

public interface RateLimiter extends BaseCloseable {

  RateLimiter UNLIMITED = () -> {
  };

  static final Duration DEFAULT_RETRY = Duration.ofSeconds(10);

  static void aquire(final RateLimiterProxy rateLimiterProxy) {
    if (rateLimiterProxy != null) {
      final var rateLimiter = rateLimiterProxy.rateLimiter();
      if (rateLimiter != null) {
        rateLimiter.aquire();
      }
    }
  }

  static Instant retryInstant(final String retryAfter) {
    if (retryAfter != null) {
      try {
        final int retryAfterSeconds = Integer.parseInt(retryAfter);
        if (retryAfterSeconds >= 0) {
          return Instant.now()
            .plus(retryAfterSeconds, ChronoUnit.SECONDS);
        }
      } catch (final NumberFormatException e1) {
        final var timeout = Dates.RFC_1123_DATE_TIME.parse(retryAfter, Instant::from);
        return timeout;
      }
    }
    return Instant.now()
      .plus(DEFAULT_RETRY);
  }

  /**
   * Acquire permission to continue if the rate limit has not been reached.
   */
  void aquire();

  @Override
  default void close() {
  }

  /**
   * Pause the rate limiter for the duration. This can be used where a service has requested
   * rate limiting (e.g HTTP 429 with Retry-After header).
   *
   * @param duration The duration to pause for.
   */
  default void pauseFor(final Duration duration) {
    Concurrent.sleep(duration);
  }

  /**
  * Pause for the duration specified in the retryAfter header or the maximum timeout.
  *
  * @param retryAfter The time in seconds or date returned from the Http Retry-After header.
  * @param timeout The maximum time to wait until
  * @return True if the thread paused, false if the timeout would be exceeded.
  */
  default boolean pauseHttpRetryAfter(final String retryAfter, final Instant timeout) {
    final var retryInstant = RateLimiter.retryInstant(retryAfter);
    if (retryInstant.isAfter(timeout)) {
      return false;
    } else {
      pauseUntil(retryInstant);
      return true;
    }
  }

  /**
   * Pause the rate limiter until the specified time. This can be used where a service has requested
   * rate limiting (e.g HTTP 429 with Retry-After header).
   *
   * @param duration The duration to pause for.
   */
  default void pauseUntil(final Instant time) {
    Concurrent.sleepUntil(time);
  }

}
