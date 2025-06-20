package com.revolsys.net.oauth;

import com.revolsys.collection.value.ValueHolder;
import com.revolsys.http.BearerTokenRequestBuilderFactory;

public interface BearerTokenFactory {
  default ValueHolder<BearerToken> lazyValue(final OpenIdScope scope) {
    return ValueHolder.<BearerToken> lazy()
      .valueRefresh(token -> refreshBearerToken(scope, token))
      .validator(BearerToken::isValid)
      .build();
  }

  default BearerTokenRequestBuilderFactory newHttpRequestBuilderFactory(final OpenIdScope scope) {
    return newTokenRefresh(scope).newHttpRequestBuilderFactory();
  }

  BearerToken newToken(OpenIdScope resource);

  default BearerTokenRefresher newTokenRefresh(final OpenIdScope scope) {
    return t -> newToken(scope);
  }

  default BearerToken refreshBearerToken(final OpenIdScope scope, final BearerToken token) {
    return newToken(scope);
  }
}
