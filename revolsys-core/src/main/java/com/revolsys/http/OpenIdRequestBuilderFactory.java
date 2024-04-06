package com.revolsys.http;

import com.revolsys.net.oauth.OpenIdConnectClient;
import com.revolsys.net.oauth.OpenIdScope;

public class OpenIdRequestBuilderFactory extends BearerTokenRequestBuilderFactory {

  public OpenIdRequestBuilderFactory(final OpenIdConnectClient client, final OpenIdScope scope) {
    this(client, scope.getScope());
  }

  public OpenIdRequestBuilderFactory(final OpenIdConnectClient client, final String scope) {
    super(token -> client.tokenClientCredentials(scope));
  }
}
