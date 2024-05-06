package com.revolsys.net.oauth;

import java.time.Instant;

import com.revolsys.collection.json.JsonObject;

public class BearerToken {

  private final String accessToken;

  private long expireTime;

  private String scope;

  private String returnedScope;

  private JsonWebToken jwt;

  public BearerToken(final JsonObject config) {
    this(config, null);
  }

  public BearerToken(final JsonObject config, final String scope) {
    this.accessToken = config.getString("access_token");
    this.scope = scope;
  }

  public BearerToken(final String accessToken) {
    this.accessToken = accessToken;
  }

  public String getAccessToken() {
    return this.accessToken;
  }

  public Instant getExpireTime() {
    return Instant.ofEpochMilli(this.expireTime);
  }

  protected JsonWebToken getJwt() {
    if (this.jwt == null) {
      this.jwt = initJwt();
    }
    return this.jwt;
  }

  public String getReturnedScope() {
    return this.returnedScope;
  }

  public String getScope() {
    return this.scope;
  }

  public String getStringClaim(final String name) {
    return getJwt().getString(name);
  }

  protected JsonWebToken initJwt() {
    return new JsonWebToken(this.accessToken);
  }

  public boolean isExpired() {
    final var time = System.currentTimeMillis();
    return time >= this.expireTime;
  }

  public boolean isValid() {
    final var time = System.currentTimeMillis();
    return time < this.expireTime;
  }

  protected void setExpireTime(final long expireTime) {
    this.expireTime = expireTime;
  }

  public void setScope(final String scope, final String returnedScope) {
    this.returnedScope = returnedScope;
    if (scope == null) {
      this.scope = returnedScope;
    } else {
      this.scope = scope;
    }
  }

  @Override
  public String toString() {
    if (this.accessToken == null) {
      return "No Token";
    } else {
      try {
        return getJwt().toString();
      } catch (final Exception e) {
        return this.accessToken;
      }
    }
  }

  public String toStringDump() {
    return this.jwt.toStringDump();
  }

  public String toStringJwt() {
    return this.jwt.toString();
  }

}
