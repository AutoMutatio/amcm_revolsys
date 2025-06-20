package com.revolsys.http;

import java.util.function.Consumer;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.io.map.ObjectFactoryConfig;
import com.revolsys.net.http.exception.AuthenticationException;
import com.revolsys.net.oauth.BearerToken;
import com.revolsys.net.oauth.OpenIdBearerToken;
import com.revolsys.net.oauth.OpenIdConnectClient;

public class OpenIdRefreshTokenRequestBuilderFactory extends BearerTokenRequestBuilderFactory {

  public static final String TYPE = "oidcRefreshToken";

  public static OpenIdRefreshTokenRequestBuilderFactory newFactory(
    final ObjectFactoryConfig factoryConfig, final JsonObject config) {
    final OpenIdConnectClient oauthClient = OpenIdConnectClient.newClient(factoryConfig, config,
      "common");
    final String scope = config.getString("scope");
    String refreshToken;
    final String refreshTokenId = config.getString("refreshTokenId");
    if (refreshTokenId != null) {
      refreshToken = SecretStore.getSecretValue(factoryConfig, refreshTokenId);
    } else {
      refreshToken = SecretStore.getSecretValue(factoryConfig, "secretId", "refreshToken");
    }
    if (refreshToken == null) {
      throw new AuthenticationException("Refresh token not found");
    }
    return new OpenIdRefreshTokenRequestBuilderFactory(oauthClient, refreshToken, scope,
      newRefreshToken -> {
        if (refreshTokenId != null) {
          SecretStore.setSecretValue(factoryConfig, refreshTokenId, newRefreshToken);
        } else {
          SecretStore.setSecretValue(factoryConfig, "secretId", "refreshToken", newRefreshToken);
        }
      });
  }

  public static OpenIdRefreshTokenRequestBuilderFactory newFactory(final OpenIdBearerToken token) {
    final var client = token.getClient();
    final var scope = token.getScope();
    final var refreshToken = token.getRefreshToken();
    if (refreshToken == null) {
      throw new AuthenticationException("Refresh token not found");
    }
    return new OpenIdRefreshTokenRequestBuilderFactory(client, refreshToken, scope, null);
  }

  private String refreshToken;

  private final OpenIdConnectClient client;

  private final String scope;

  private final Consumer<String> tokenChanged;

  private OpenIdRefreshTokenRequestBuilderFactory(final OpenIdConnectClient client,
    final String refreshToken, final String scope, final Consumer<String> tokenChanged) {
    super();
    this.client = client;
    this.refreshToken = refreshToken;
    this.scope = scope;
    this.tokenChanged = tokenChanged;
  }

  public String claim(final String key) {
    return getBearerToken().getStringClaim(key);
  }

  @Override
  protected BearerToken refreshTokenDo(final BearerToken token) {
    final OpenIdBearerToken newToken = this.client.tokenRefresh(this.refreshToken, this.scope);
    this.refreshToken = newToken.getRefreshToken();
    if (this.tokenChanged != null) {
      this.tokenChanged.accept(this.refreshToken);
    }
    return newToken;
  }

  public String tenantId() {
    return getBearerToken().getStringClaim("tid");
  }
}
