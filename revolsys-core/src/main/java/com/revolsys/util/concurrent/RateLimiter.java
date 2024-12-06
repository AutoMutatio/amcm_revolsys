package com.revolsys.util.concurrent;

import java.time.Duration;
import java.time.Instant;

import com.revolsys.date.Dates;
import com.revolsys.http.HttpThrottle;
import com.revolsys.util.BaseCloseable;

public interface RateLimiter extends BaseCloseable {

  RateLimiter UNLIMITED = () -> {
  };

  static void aquire(final RateLimiterProxy rateLimiterProxy) {
    if (rateLimiterProxy != null) {
      final var rateLimiter = rateLimiterProxy.rateLimiter();
      if (rateLimiter != null) {
        rateLimiter.aquire();
      }
    }
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

  default void pauseHttpRetryAfter(final String retryAfter) {
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
  default void pauseUtil(final Instant time) {
    Concurrent.sleepUntil(time);
  }

}
