package com.revolsys.net.oauth;

import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.JsonParser;
import com.revolsys.collection.map.LruMap;
import com.revolsys.collection.value.ValueHolder;

public class OpenIdConfiguration {
  private static Map<URI, OpenIdConfiguration> CACHE = new LruMap<>(100);

  public static OpenIdConfiguration getConfiguration(final URI uri) {
    var configuration = CACHE.get(uri);
    if (configuration == null) {
      final JsonObject json = JsonParser.read(uri);
      configuration = new OpenIdConfiguration(json);
    }

    for (final var iterator = CACHE.values()
      .iterator(); iterator.hasNext();) {
      final var entry = iterator.next();
      if (entry.isExpired()) {
        iterator.remove();
      }
    }
    return configuration;
  }

  private final JsonObject configuration;

  private final Instant expiry;

  private final ValueHolder<JwtKeySet> jwtKeySet = ValueHolder.lazy(this::loadJwtKeySet);

  public OpenIdConfiguration(final JsonObject configuration) {
    this.configuration = configuration;
    this.expiry = Instant.now()
      .plus(15, ChronoUnit.MINUTES);
  }

  public JwtKeySet getJwtKeySet() {
    return this.jwtKeySet.getValue();
  }

  public boolean isExpired() {
    return this.expiry.isAfter(Instant.now());
  }

  private JwtKeySet loadJwtKeySet() {
    final URI url = URI.create(this.configuration.getString("jwks_uri"));
    final JsonObject json = JsonParser.read(url);
    return new JwtKeySet(json);
  }

  @Override
  public String toString() {
    return this.configuration.toString();
  }
}
