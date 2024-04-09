package com.revolsys.net.oauth;

import com.revolsys.http.BearerTokenRequestBuilderFactory;
import com.revolsys.http.HttpRequestBuilderFactory;

public interface BearerTokenRefresher {
  default HttpRequestBuilderFactory newHttpRequestBuilderFactory() {
    return new BearerTokenRequestBuilderFactory(this);
  }

  BearerToken refreshBearerToken(BearerToken token);

}
