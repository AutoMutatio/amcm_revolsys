package com.revolsys.http;

import java.util.function.Function;

import com.revolsys.net.oauth.BearerToken;

public class BearerTokenRequestBuilderFactory extends HttpRequestBuilderFactory {

  private final Function<BearerToken, BearerToken> tokenRefresh;

  private BearerToken token;

  public BearerTokenRequestBuilderFactory() {
    this.tokenRefresh = this::refreshTokenDo;
  }

  public BearerTokenRequestBuilderFactory(final Function<BearerToken, BearerToken> tokenRefresh) {
    this.tokenRefresh = tokenRefresh;
  }

  protected String getAccessToken() {
    if (this.token == null || this.token.isExpired()) {
      this.token = this.tokenRefresh.apply(this.token);
    }
    if (this.token == null) {
      return null;
    } else {
      return this.token.getAccessToken();
    }
  }

  protected String getAuthorizationHeader() {
    final String accessToken = getAccessToken();
    return "Bearer " + accessToken;
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
