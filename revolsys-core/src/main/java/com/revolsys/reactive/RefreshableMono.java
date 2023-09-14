package com.revolsys.reactive;

import java.time.Duration;
import java.time.Instant;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

public class RefreshableMono<V> {

  private final Many<Boolean> source;

  public Mono<V> mono;

  private Duration expiry;

  private Instant expireTime = Instant.MAX;

  private boolean refreshRequired = false;

  public RefreshableMono(final Mono<V> refreshAction) {
    this(refreshAction, null);
  }

  public RefreshableMono(final Mono<V> refreshAction, final Duration expiry) {
    this.expiry = expiry;
    this.source = Sinks.many().replay().latestOrDefault(true);
    final Mono<V> flux = this.source//
      .asFlux()
      .flatMap(b -> refreshAction)
      .doOnNext(v -> {
        if (this.expiry != null) {
          this.expireTime = Instant.now().plus(this.expiry);
        }
      })
      .take(1)
      .single()
      .cacheInvalidateIf(this::expired);
    this.mono = flux.single();
  }

  public Mono<V> asMono() {
    return this.mono;
  }

  private boolean expired(final V value) {
    if (this.refreshRequired) {
      this.refreshRequired = false;
      return true;
    } else if (this.expiry != null) {
      final Instant now = Instant.now();
      if (now.isAfter(this.expireTime)) {
        return true;
      }
    }
    return false;
  }

  public void refresh() {
    this.refreshRequired = true;
  }
}
