package com.revolsys.net.oauth;

import java.util.function.Function;

import com.revolsys.record.io.format.json.JsonObject;

public class OpenIdBearerToken extends BearerToken {

  private JsonWebToken jwt;

  private final String refreshToken;

  private final String idToken;

  private final OpenIdConnectClient client;

  private Function<String, OpenIdBearerToken> tokenRefresh;

  public OpenIdBearerToken(final OpenIdConnectClient client, final JsonObject config,
    final OpenIdResource resource) {
    super(config, resource.getResource());
    this.client = client;
    this.refreshToken = config.getString("refresh_token");
    this.idToken = config.getString("id_token");
    final Integer expiresIn = config.getInteger("expires_in");
    final long expireTime = System.currentTimeMillis() + expiresIn * 1000;
    setExpireTime(expireTime);
    final String returnedScope = config.getString("scope");
    setScope(resource.getResource(), returnedScope);
  }

  public OpenIdBearerToken(final OpenIdConnectClient client, final JsonObject config,
    final String scope) {
    super(config, scope);
    this.client = client;
    this.refreshToken = config.getString("refresh_token");
    this.idToken = config.getString("id_token");
    final Integer expiresIn = config.getInteger("expires_in");
    final long expireTime = System.currentTimeMillis() + expiresIn * 1000;
    setExpireTime(expireTime);
    final String returnedScope = config.getString("scope");
    setScope(scope, returnedScope);
  }

  public OpenIdConnectClient getClient() {
    return this.client;
  }

  public String getIdToken() {
    return this.idToken;
  }

  protected JsonWebToken getJwt() {
    if (this.jwt == null) {
      String token = this.idToken;
      if (token == null) {
        token = getAccessToken();
      }
      this.jwt = new JsonWebToken(token);
    }
    return this.jwt;
  }

  public String getRefreshToken() {
    return this.refreshToken;
  }

  public String getStringClaim(final String name) {
    return getJwt().getString(name);
  }

  public OpenIdBearerToken getValid() {
    if (isExpired()) {
      return refreshToken();
    } else {
      return this;
    }
  }

  public OpenIdBearerToken refreshToken() {
    final String scope = getScope();
    if (this.tokenRefresh != null) {
      return this.tokenRefresh.apply(scope);
    } else if (this.refreshToken == null || this.client == null) {
      return null;
    } else {
      return this.client.tokenRefresh(this.refreshToken, scope);
    }
  }

  public OpenIdBearerToken setTokenRefresh(final Function<String, OpenIdBearerToken> tokenRefresh) {
    this.tokenRefresh = tokenRefresh;
    return this;
  }
}
