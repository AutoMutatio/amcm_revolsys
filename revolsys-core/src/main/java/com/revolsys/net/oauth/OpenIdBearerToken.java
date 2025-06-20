package com.revolsys.net.oauth;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.http.OpenIdRefreshTokenRequestBuilderFactory;

public class OpenIdBearerToken extends AbstractRefereshableBearerToken {

  private final String refreshToken;

  private final String idToken;

  private final OpenIdConnectClient client;

  public OpenIdBearerToken(final OpenIdConnectClient client, final JsonObject config,
    final OpenIdScope resource) {
    super(config, resource.getScope());
    this.client = client;
    this.refreshToken = config.getString("refresh_token");
    this.idToken = config.getString("id_token");
    final Integer expiresIn = config.getInteger("expires_in");
    final long expireTime = System.currentTimeMillis() + expiresIn * 1000;
    setExpireTime(expireTime);
    final String returnedScope = config.getString("scope");
    setScope(resource.getScope(), returnedScope);
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

  public String getRefreshToken() {
    return this.refreshToken;
  }

  public OpenIdBearerToken getValid() {
    if (isExpired()) {
      return refreshToken();
    } else {
      return this;
    }
  }

  @Override
  protected JsonWebToken initJwt() {
    if (this.idToken == null) {
      return super.initJwt();
    } else {
      return new JsonWebToken(this.idToken);
    }
  }

  @Override
  public OpenIdRefreshTokenRequestBuilderFactory newHttpRequestBuilderFactory() {
    return OpenIdRefreshTokenRequestBuilderFactory.newFactory(this);
  }

  @Override
  public OpenIdBearerToken refreshToken() {
    if (this.refreshToken == null || this.client == null) {
      return null;
    } else {
      final String scope = getScope();
      return this.client.tokenRefresh(this.refreshToken, scope);
    }
  }

}
