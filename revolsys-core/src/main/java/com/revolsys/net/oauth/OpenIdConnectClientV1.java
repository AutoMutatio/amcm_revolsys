package com.revolsys.net.oauth;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.http.HttpRequestBuilder;
import com.revolsys.record.io.format.json.JsonIo;
import com.revolsys.spring.resource.Resource;

public class OpenIdConnectClientV1 extends OpenIdConnectClient {

  public static OpenIdConnectClientV1 microsoftV1(final String tenantId) {
    final String url = String
      .format("https://login.microsoftonline.com/%s/.well-known/openid-configuration", tenantId);
    return newClientV1(url);
  }

  public static OpenIdConnectClientV1 microsoftV1Common() {
    return microsoftV1("common");
  }

  private static OpenIdConnectClientV1 newClientV1(final String url) {
    final Resource resource = Resource.getResource(url);
    final JsonObject config = JsonIo.read((Object)resource);
    if (config == null || config.isEmpty()) {
      throw new IllegalArgumentException("Not a valid .well-known/openid-configuration");
    } else {
      final OpenIdConnectClientV1 client = new OpenIdConnectClientV1(config);
      client.setUrl(url);
      return client;
    }
  }

  public OpenIdConnectClientV1(final JsonObject config) {
    super(config);
  }

  public BearerTokenRefresher newTokenRefresh(
    final OpenIdScope resource) {
    return bearerToken -> tokenClientCredentials(resource);
  }

  private OpenIdBearerToken getOpenIdBearerToken(final HttpRequestBuilder requestBuilder,
    final OpenIdScope resource) {
    final var response = requestBuilder.responseAsJson();
    return new OpenIdBearerToken(this, response, resource);
  }

  public OpenIdBearerToken tokenClientCredentials(final OpenIdScope resource) {
    final var requestBuilder = tokenBuilder("client_credentials", true);
    if (resource != null) {
      requestBuilder.addParameter("resource", resource.getScope());
    }
    return getOpenIdBearerToken(requestBuilder, resource);
  }

  public OpenIdBearerToken tokenRefresh(final String refreshToken, final OpenIdScope resource) {
    final var requestBuilder = tokenBuilder("refresh_token", true);
    requestBuilder.addParameter("refresh_token", refreshToken);
    if (resource != null) {
      requestBuilder.addParameter("resource", resource.getScope());
    }
    return getOpenIdBearerToken(requestBuilder, resource);
  }
}
