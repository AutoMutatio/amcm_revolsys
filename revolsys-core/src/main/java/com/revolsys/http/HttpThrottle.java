package com.revolsys.http;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.exception.Exceptions;
import com.revolsys.parallel.ReentrantLockEx;

public class HttpThrottle {

  private static Map<String, Instant> URLS = new LinkedHashMap<>();

  private static ReentrantLockEx lock = new ReentrantLockEx();

  public static void throttle(final String key, final long retryTime) {
    final var now = Instant.now();
    try (
      var l = lock.lockX()) {
      URLS.put(key, now.plus(retryTime, ChronoUnit.MILLIS));
    }
    try {
      Thread.sleep(retryTime);
    } catch (final InterruptedException e1) {
      Exceptions.throwUncheckedException(e1);
    } finally {
      try (
        var l = lock.lockX()) {
        URLS.remove(key);
      }
    }
  }

  public static JsonObject toJson() {
    return JsonObject.hash("count", URLS.size()).addAll(URLS);
  }
}
