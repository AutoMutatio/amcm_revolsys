package com.revolsys.net.oauth;

import com.revolsys.http.HttpRequestBuilderFactory;

public interface BearerTokenFactory {
  default HttpRequestBuilderFactory newHttpRequestBuilderFactory(final OpenIdScope scope) {
    return newTokenRefresh(scope).newHttpRequestBuilderFactory();
  }

  BearerTokenRefresher newTokenRefresh(OpenIdScope scope);

}
