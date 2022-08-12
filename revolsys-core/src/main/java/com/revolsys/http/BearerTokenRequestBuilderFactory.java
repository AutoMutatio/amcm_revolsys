package com.revolsys.http;

import java.util.function.Function;

import com.revolsys.net.oauth.BearerToken;

import reactor.netty.Connection;
import reactor.netty.http.client.HttpClientRequest;

public class BearerTokenRequestBuilderFactory extends ApacheHttpRequestBuilderFactory {

  private final Function<BearerToken, BearerToken> tokenRefesh;

  private BearerToken token;

  public BearerTokenRequestBuilderFactory() {
    this.tokenRefesh = this::refreshTokenDo;
  }

  public BearerTokenRequestBuilderFactory(final Function<BearerToken, BearerToken> tokenRefesh) {
    this.tokenRefesh = tokenRefesh;
  }

  protected String getAccessToken() {
    if (this.token == null || this.token.isExpired()) {
      this.token = this.tokenRefesh.apply(this.token);
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
  public ApacheHttpRequestBuilder newRequestBuilder() {
    return new BearerTokenRequestBuilder(this);
  }

  @Override
  protected void onNettyRequest(final HttpClientRequest request, final Connection connection) {
    super.onNettyRequest(request, connection);
    final String authorization = getAuthorizationHeader();
    request.header("Authorization", authorization);
  }

  protected BearerToken refreshTokenDo(final BearerToken token) {
    return null;
  }
}
