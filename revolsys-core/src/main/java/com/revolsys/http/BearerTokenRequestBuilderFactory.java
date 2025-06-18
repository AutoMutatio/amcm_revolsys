package com.revolsys.http;

import com.revolsys.net.oauth.BearerToken;
import com.revolsys.net.oauth.BearerTokenRefresher;

public class BearerTokenRequestBuilderFactory extends HttpRequestBuilderFactory {

  private final BearerTokenRefresher tokenRefresh;

  private BearerToken token;

  public BearerTokenRequestBuilderFactory() {
    this.tokenRefresh = this::refreshTokenDo;
  }

  public BearerTokenRequestBuilderFactory(final BearerTokenRefresher tokenRefresh) {
    this.tokenRefresh = tokenRefresh;
  }

  protected String getAccessToken() {
    final var token = getBearerToken();
    if (token == null) {
      return null;
    } else {
      return token.getAccessToken();
    }
  }

  protected String getAuthorizationHeader() {
    final String accessToken = getAccessToken();
    return "Bearer " + accessToken;
  }

  protected BearerToken getBearerToken() {
    if (this.token == null || this.token.isExpired()) {
      this.token = this.tokenRefresh.refreshBearerToken(this.token);
    }
    if (this.token == null) {
      return null;
    } else {
      return this.token;
    }
  }

  public String getClaim(final String key) {
    final var token = getBearerToken();
    if (token == null) {
      return null;
    } else {
      return token.getStringClaim(key);
    }
  }

  public boolean hasValidToken() {
    final var token = getBearerToken();
    if (token == null) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public void preBuild(final HttpRequestBuilder requestBuilder) {
    final String authorization = getAuthorizationHeader();
    requestBuilder.addHeader("Authorization", authorization);
  }

  protected BearerToken refreshTokenDo(final BearerToken token) {
    return null;
  }
}
