package com.revolsys.net.oauth;

import com.revolsys.http.BearerTokenRequestBuilderFactory;

public interface BearerTokenRefresher {
  default BearerTokenRequestBuilderFactory newHttpRequestBuilderFactory() {
    return new BearerTokenRequestBuilderFactory(this);
  }

  BearerToken refreshBearerToken(BearerToken token);

}
