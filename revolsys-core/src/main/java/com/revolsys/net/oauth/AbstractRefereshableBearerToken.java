package com.revolsys.net.oauth;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.http.HttpRequestBuilderFactory;

public abstract class AbstractRefereshableBearerToken extends BearerToken {

  public AbstractRefereshableBearerToken(final JsonObject config, final String scope) {
    super(config, scope);
  }

  public abstract HttpRequestBuilderFactory newHttpRequestBuilderFactory();

  public abstract AbstractRefereshableBearerToken refreshToken();

}
