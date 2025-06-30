package com.revolsys.net.oauth;

import java.awt.Desktop;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.swing.SwingUtilities;

import org.apache.http.client.methods.RequestBuilder;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.JsonParser;
import com.revolsys.http.HttpRequestBuilder;
import com.revolsys.http.HttpRequestBuilderFactory;
import com.revolsys.http.SecretStore;
import com.revolsys.io.map.ObjectFactoryConfig;
import com.revolsys.net.http.ApacheHttpException;
import com.revolsys.net.http.exception.AuthenticationException;
import com.revolsys.properties.BaseObjectWithProperties;
import com.revolsys.record.io.format.json.JsonIo;
import com.revolsys.spring.resource.Resource;
import com.revolsys.util.Strings;

public class OpenIdConnectClient extends BaseObjectWithProperties implements BearerTokenFactory {
  public static final String TYPE_CLIENT_ID = "oidcClientId";

  public static OpenIdConnectClient google() {
    return newClient("https://accounts.google.com/.well-known/openid-configuration");
  }

  public static OpenIdConnectClient microsoft(final String tenantId) {
    final String url = String.format(
      "https://login.microsoftonline.com/%s/v2.0/.well-known/openid-configuration", tenantId);
    return newClient(url);
  }

  public static OpenIdConnectClient microsoftCommon() {
    return microsoft("common");
  }

  public static OpenIdConnectClient newClient(final ObjectFactoryConfig factoryConfig,
    final JsonObject config, final String defaultPrefix) {
    final String url = config.getString("wellKnownUrl");
    OpenIdConnectClient client = null;
    if (url == null) {
      final String oidcTenantKey = config.getString("oidcTenantKey");
      if (oidcTenantKey != null) {
        final Function<String, OpenIdConnectClient> oidcClientFactory = factoryConfig
          .getValue("oidcClientFactory");
        if (oidcClientFactory != null) {
          client = oidcClientFactory.apply(oidcTenantKey);
        }
      }
      if (client == null) {
        client = factoryConfig.getValue(defaultPrefix + "OidcClient");
      }
      if (client == null) {
        final String tenantId = config.getString("tenantId");
        if (tenantId != null) {
          client = OpenIdConnectClient.microsoft(tenantId);
        } else {
          client = OpenIdConnectClient.microsoftCommon();
        }
      }
    } else {
      client = newClient(url);
    }
    if (client == null) {
      throw new IllegalArgumentException("Cannot create OpenIDConnect client for: " + config);
    }
    final String clientId = config.getString("clientId");
    if (clientId != null) {
      client.setClientId(clientId);
    }
    final String clientSecret = config.getString("clientSecret");
    if (clientSecret != null) {
      client.setClientSecret(clientSecret);
    }
    return client;
  }

  public static OpenIdConnectClient newClient(final String url) {
    final Resource resource = Resource.getResource(url);
    final JsonObject config = JsonIo.read((Object)resource);
    if (config == null || config.isEmpty()) {
      throw new IllegalArgumentException("Not a valid .well-known/openid-configuration");
    } else {
      final OpenIdConnectClient client = new OpenIdConnectClient(config);
      client.setUrl(url);
      return client;
    }
  }

  public static HttpRequestBuilderFactory newHttpRequestBuilder(
    final ObjectFactoryConfig factoryConfig, final JsonObject config) {
    final var scope = OpenIdScope.forString(config.getString("scope"));
    final var secretId = factoryConfig.getString("secretId");
    final JsonObject authConfig = SecretStore.getSecretJsonObject(factoryConfig, secretId);
    final var client = OpenIdConnectClient.newClient(factoryConfig, authConfig, null);
    if (authConfig.hasValue("clientSecret")) {
      return client.newHttpRequestBuilderFactory(scope);
    } else {
      final BearerTokenFactory federatedFactory = factoryConfig.getValue("oidcFederatedFactory");
      final BearerTokenFactory tokenFactory = s -> {
        final var token = federatedFactory.newToken(s);
        return client.tokenClientCredentialsAssertion(token, s.getScope());
      };
      return tokenFactory.newHttpRequestBuilderFactory(scope);
    }
  }

  private final String issuer;

  private final String authorizationEndpoint;

  private final String deviceAuthorizationEndpoint;

  private final String tokenEndpoint;

  private final String userinfoEndpoint;

  private final String revocationEndpoint;

  private String clientId;

  private String clientSecret;

  private String url;

  private final String endSessionEndpoint;

  private Supplier<String> clientAssertionSupplier;

  public OpenIdConnectClient(final JsonObject config) {
    this.issuer = config.getString("issuer");
    this.authorizationEndpoint = config.getString("authorization_endpoint");
    this.deviceAuthorizationEndpoint = config.getString("device_authorization_endpoint");
    this.tokenEndpoint = config.getString("token_endpoint");
    this.userinfoEndpoint = config.getString("userinfo_endpoint");
    this.revocationEndpoint = config.getString("revocation_endpoint");
    this.endSessionEndpoint = config.getString("end_session_endpoint");
  }

  protected void addAuthentication(final HttpRequestBuilder builder) {
    addClientId(builder);
    if (this.clientAssertionSupplier != null) {
      final var clientAssertion = this.clientAssertionSupplier.get();
      if (clientAssertion != null) {
        builder
          .addParameter("client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
          .addParameter("client_assertion", clientAssertion);
      }
    } else if (this.clientSecret != null) {
      builder.addParameter("client_secret", this.clientSecret);
    }
  }

  protected void addClientId(final HttpRequestBuilder requestBuilder) {
    if (this.clientId != null) {
      requestBuilder.addParameter("client_id", this.clientId);
    }
  }

  protected void addScopes(final RequestBuilder builder, final Collection<String> scopes) {
    final String scope = Strings.toString(" ", scopes);
    builder.addParameter("scope", scope);
  }

  public URI authorizationUrl(final String scope, final String redirectUri, final String state,
    final String nonce, final String prompt) {
    final RequestBuilder builder = authorizationUrlBuilder(scope, redirectUri, state, nonce,
      prompt);
    return builder.build()
      .getURI();
  }

  public RequestBuilder authorizationUrlBuilder(final String scope, final String redirectUri,
    final String state, final String nonce, final String prompt) {
    final var builder = RequestBuilder//
      .get(this.authorizationEndpoint)
      .addParameter("response_type", "code")
      .addParameter("response_mode", "query")
      .addParameter("client_id", this.clientId)
      .addParameter("scope", scope)
      .addParameter("redirect_uri", redirectUri)
      .addParameter("state", state)
      .addParameter("nonce", nonce);
    if (prompt != null) {
      builder.addParameter("prompt", prompt);
    }
    return builder;
  }

  public DeviceCodeResponse deviceCode(final String scope) {
    final var requestBuilder = HttpRequestBuilder//
      .post(this.deviceAuthorizationEndpoint);
    addClientId(requestBuilder);
    if (scope != null) {
      requestBuilder.addParameter("scope", scope);
    }
    final JsonObject response = requestBuilder.responseAsJson();
    return new DeviceCodeResponse(this, response, scope);
  }

  public URI endSessionUrl(final String redirectUrl) {
    final RequestBuilder builder = RequestBuilder//
      .get(this.endSessionEndpoint)
      .addParameter("post_logout_redirect_uri", redirectUrl);
    return builder.build()
      .getURI();
  }

  public OpenIdConnectClient forTenant(final String tenantKey) {
    return this;
  }

  public String getAuthorizationEndpoint() {
    return this.authorizationEndpoint;
  }

  public String getClientId() {
    return this.clientId;
  }

  public String getDeviceAuthorizationEndpoint() {
    return this.deviceAuthorizationEndpoint;
  }

  public String getEndSessionEndpoint() {
    return this.endSessionEndpoint;
  }

  public String getIssuer() {
    return this.issuer;
  }

  private OpenIdBearerToken getOpenIdBearerToken(final HttpRequestBuilder requestBuilder,
    final String scope) {
    try {
      final JsonObject response = requestBuilder.responseAsJson();
      return new OpenIdBearerToken(this, response, scope);
    } catch (final ApacheHttpException e) {
      if (e.getStatusCode() == 400) {
        JsonObject error = null;
        try {
          final String content = e.getContent();
          error = JsonParser.read(content);
        } catch (final Exception e2) {
        }
        if (error != null) {
          String errorDescription = error.getString("error_description");
          if (errorDescription != null) {
            final int index = errorDescription.indexOf("Trace ID:");
            if (index != -1) {
              errorDescription = errorDescription.substring(0, index)
                .strip();
            }
            throw new AuthenticationException(errorDescription);
          }
        }
      }
      throw e;
    }
  }

  public String getRevocationEndpoint() {
    return this.revocationEndpoint;
  }

  public String getTokenEndpoint() {
    return this.tokenEndpoint;
  }

  public String getUrl() {
    return this.url;
  }

  public String getUserinfoEndpoint() {
    return this.userinfoEndpoint;
  }

  @Override
  public BearerToken newToken(final OpenIdScope scope) {
    return tokenClientCredentials(scope.getScope());
  }

  @Override
  public BearerTokenRefresher newTokenRefresh(final OpenIdScope scope) {
    final var s = scope.getScope();
    return bearerToken -> tokenClientCredentials(s);
  }

  public OpenIdConnectClient setClientAssertionSupplier(
    final Supplier<String> clientAssertionSupplier) {
    this.clientAssertionSupplier = clientAssertionSupplier;
    return this;
  }

  public OpenIdConnectClient setClientId(final String clientId) {
    this.clientId = clientId;
    return this;
  }

  public OpenIdConnectClient setClientSecret(final String clientSecret) {
    this.clientSecret = clientSecret;
    return this;
  }

  protected void setUrl(final String url) {
    this.url = url;
  }

  public OpenIdBearerToken tokenAuthorizationCode(final String code, final String redirectUri,
    final String scope) {
    final var builder = HttpRequestBuilder//
      .post(this.tokenEndpoint)
      .addParameter("grant_type", "authorization_code")
      .addParameter("redirect_uri", redirectUri)
      .addParameter("code", code);
    addAuthentication(builder);
    return getOpenIdBearerToken(builder, scope);
  }

  protected HttpRequestBuilder tokenBuilder(final String grantType, final boolean useClientSecret) {
    final HttpRequestBuilder builder = HttpRequestBuilder//
      .post(this.tokenEndpoint)
      .addParameter("grant_type", grantType);
    if (useClientSecret) {
      addAuthentication(builder);
    }
    return builder;
  }

  public OpenIdBearerToken tokenClientCredentials(final String scope) {
    final var requestBuilder = tokenBuilder("client_credentials", true);
    if (scope != null) {
      requestBuilder.addParameter("scope", scope);
    }
    return getOpenIdBearerToken(requestBuilder, scope);
  }

  public OpenIdBearerToken tokenClientCredentialsAssertion(final BearerToken token,
    final String scope) {
    final var requestBuilder = tokenBuilder("client_credentials", true);
    requestBuilder
      .addParameter("client_assertion_type",
        "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
      .addParameter("client_assertion", token.getAccessToken());
    if (scope != null) {
      requestBuilder.addParameter("scope", scope);
    }
    return getOpenIdBearerToken(requestBuilder, scope);
  }

  public OpenIdBearerToken tokenDeviceCode(final String deviceCode, final String scope) {
    final String grantType = "urn:ietf:params:oauth:grant-type:device_code";
    final var requestBuilder = tokenBuilder(grantType, true) //
      .addParameter("device_code", deviceCode);

    return getOpenIdBearerToken(requestBuilder, scope);
  }

  public BearerTokenFactory tokenFactoryDeviceCode() {
    return s -> {
      final var deviceCodeResponse = deviceCode(s.toString());
      SwingUtilities.invokeLater(() -> {
        try {
          final Toolkit toolkit = Toolkit.getDefaultToolkit();
          final Clipboard clipboard = toolkit.getSystemClipboard();
          final var transferable = new StringSelection(deviceCodeResponse.getUserCode());
          clipboard.setContents(transferable, null);
          Desktop.getDesktop()
            .browse(URI.create(deviceCodeResponse.getVerificationUri()));
        } catch (final IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      });
      return deviceCodeResponse.getToken();
    };
  }

  public OpenIdBearerToken tokenPassword(final String username, final String password,
    final String scope) {
    final var requestBuilder = tokenBuilder("password", true)//
      .addParameter("username", username)
      .addParameter("password", password);
    if (scope != null) {
      requestBuilder.addParameter("scope", scope);
    }
    return getOpenIdBearerToken(requestBuilder, scope);
  }

  public OpenIdBearerToken tokenRefresh(final String refreshToken, final String scope) {
    final var requestBuilder = tokenBuilder("refresh_token", true);
    requestBuilder //
      .addParameter("refresh_token", refreshToken);
    if (scope != null) {
      requestBuilder.addParameter("scope", scope);
    }
    return getOpenIdBearerToken(requestBuilder, scope);
  }
}
