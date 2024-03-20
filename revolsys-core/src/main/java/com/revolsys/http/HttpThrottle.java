package com.revolsys.http;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.exception.Exceptions;
import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.util.Pair;

public class HttpThrottle {

  private static Map<String, Pair<Instant, Duration>> URLS = new LinkedHashMap<>();

  private static ReentrantLockEx lock = new ReentrantLockEx();

  public static void throttle(final String key, final long retryTime) {
    final var now = Instant.now();
    try (
      var l = lock.lockX()) {
      URLS.put(key, Pair.newPair(now, Duration.of(retryTime, ChronoUnit.MILLIS)));
    }
    try {
      Thread.sleep(retryTime);
    } catch (final InterruptedException e1) {
      throw Exceptions.toRuntimeException(e1);
    } finally {
      try (
        var l = lock.lockX()) {
        URLS.remove(key);
      }
    }
  }

  public static JsonObject toJson() {
    return JsonObject.hash("count", URLS.size())
      .addAll(URLS);
  }
}
