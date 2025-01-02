package com.revolsys.collection.map;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.revolsys.collection.value.SimpleValueHolder;
import com.revolsys.collection.value.Single;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.concurrent.Concurrent;
import com.revolsys.util.concurrent.LastAccessProxy;

public class CacheWithAccessExpiry<K, V> {
  private class CachedValue {
    private long accessTime = System.currentTimeMillis();

    private final V value;

    public CachedValue(final V value) {
      this.value = value;
    }

    public boolean isExpired() {
      if (this.value instanceof final LastAccessProxy lastAccessProxy) {
        this.accessTime = Math.max(this.accessTime, lastAccessProxy.lastAccess()
          .accessTime());
      }
      final var expired = System.currentTimeMillis() > this.accessTime
        + CacheWithAccessExpiry.this.expiryMillis;
      if (expired) {
        BaseCloseable.closeValueSilent(this.value);
      }
      return expired;
    }

    public Single<V> single() {
      this.accessTime = System.currentTimeMillis();
      return Single.ofNullable(this.value);
    }

    public V value() {
      this.accessTime = System.currentTimeMillis();
      return this.value;
    }
  }

  private static <K2, V2> Runnable cleanupTask(
    final WeakReference<CacheWithAccessExpiry<K2, V2>> ref,
    final SimpleValueHolder<ScheduledFuture<?>> futureHolder) {
    return () -> {
      final var cache = ref.get();
      if (cache == null) {
        // Attempt to cancel task if garbage collected
        final var scheduledFuture = futureHolder.get();
        if (scheduledFuture != null) {
          scheduledFuture.cancel(true);
        }
      } else {
        cache.removeExpired();
      }
    };
  }

  private final long expiryMillis;

  private final ConcurrentHashMap<K, CachedValue> cachedValues = new ConcurrentHashMap<>();

  public CacheWithAccessExpiry(final Duration expiry) {
    this.expiryMillis = expiry.toMillis();
    final var ref = new WeakReference<>(this);
    final var futureHolder = new SimpleValueHolder<ScheduledFuture<?>>();
    final var cleanupTask = cleanupTask(ref, futureHolder);
    final var future = Concurrent.virtualSceduled()
      .scheduleWithFixedDelay(cleanupTask, this.expiryMillis, this.expiryMillis,
        TimeUnit.MILLISECONDS);
    futureHolder.setValue(future);
  }

  public V get(final K key, final Function<K, ? extends V> initFunction) {
    return this.cachedValues.computeIfAbsent(key, i -> {
      final var value = initFunction.apply(i);
      return new CachedValue(value);
    })
      .value();
  }

  public Single<V> getSingle(final K key, final Function<K, ? extends V> initFunction) {
    return this.cachedValues.computeIfAbsent(key, i -> {
      final var value = initFunction.apply(i);
      return new CachedValue(value);
    })
      .single();
  }

  private void removeExpired() {
    this.cachedValues.entrySet()
      .removeIf(e -> e.getValue()
        .isExpired());
  }

}
