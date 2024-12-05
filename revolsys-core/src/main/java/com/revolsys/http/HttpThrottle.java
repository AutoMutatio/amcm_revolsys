package com.revolsys.http;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.date.Dates;
import com.revolsys.exception.Exceptions;
import com.revolsys.logging.Logs;
import com.revolsys.net.http.ApacheHttpException;
import com.revolsys.util.Pair;

public class HttpThrottle {

  public static final Duration DEFAULT_RETRY = Duration.ofSeconds(10);

  private static final AtomicBoolean THROTTLING = new AtomicBoolean();

  private static ConcurrentHashMap<String, Pair<Instant, Duration>> URLS = new ConcurrentHashMap<>();

  public static Duration retryTime(final ApacheHttpException e) {
    final String retryAfter = e.getHeader("Retry-After");
    return retryTime(retryAfter);
  }

  private static Duration retryTime(final String retryAfter) {
    if (retryAfter != null) {
      try {
        final int retryAfterSeconds = Integer.parseInt(retryAfter);
        if (retryAfterSeconds > 0) {
          return Duration.ofSeconds(retryAfterSeconds);
        }
      } catch (final NumberFormatException e1) {
        final var timeout = Dates.RFC_1123_DATE_TIME.parse(retryAfter, Instant::from);
        return Duration.between(Instant.now(), timeout);
      }
    }
    return DEFAULT_RETRY;
  }

  public static void throttle(final ApacheHttpException e) {
    final var timeout = retryTime(e);
    HttpThrottle.throttle(e.getRequestUri()
      .toString(), timeout);
  }

  public static void throttle(final ApacheHttpException e, final String retryAfter) {
    final var timeout = retryTime(retryAfter);
    HttpThrottle.throttle(e.getRequestUri()
      .toString(), timeout);
  }

  public static void throttle(final String key, final Duration timout) {

    try {
      if (THROTTLING.compareAndSet(false, true)) {
        Logs.error(HttpThrottle.class, "Throttling");
      }
      final var now = Instant.now();
      URLS.put(key, Pair.newPair(now, timout));
      Thread.sleep(timout);
    } catch (final InterruptedException e1) {
      throw Exceptions.toRuntimeException(e1);
    } finally {
      THROTTLING.set(false);
      URLS.remove(key);
    }
  }

  public static void throttle(final String key, final long retryTime) {
    final var duration = Duration.of(retryTime, ChronoUnit.MILLIS);
    throttle(key, duration);
  }

  public static JsonObject toJson() {
    return JsonObject.hash("count", URLS.size())
      .addAll(URLS);
  }
}
