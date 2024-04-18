package com.revolsys.net.oauth;

import com.revolsys.collection.value.ValueHolder;
import com.revolsys.http.HttpRequestBuilderFactory;

public interface BearerTokenFactory {
  default ValueHolder<BearerToken> lazyValue(final OpenIdScope scope) {
    return ValueHolder.<BearerToken>lazy().valueRefresh(token -> refreshBearerToken(scope, token))
        .validator(BearerToken::isValid).build();
  }

  default HttpRequestBuilderFactory newHttpRequestBuilderFactory(final OpenIdScope scope) {
    return newTokenRefresh(scope).newHttpRequestBuilderFactory();
  }

  BearerToken newToken(OpenIdScope resource);

  BearerTokenRefresher newTokenRefresh(OpenIdScope scope);

  default BearerToken refreshBearerToken(final OpenIdScope scope, final BearerToken token) {
    return newToken(scope);
  }
}
