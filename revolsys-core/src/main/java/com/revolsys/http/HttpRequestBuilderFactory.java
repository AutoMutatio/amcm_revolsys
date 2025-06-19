package com.revolsys.http;

import java.net.URI;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.http.HttpRequest;
import org.apache.http.impl.client.HttpClientBuilder;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.io.map.ObjectFactoryConfig;
import com.revolsys.net.oauth.OpenIdConnectClient;
import com.revolsys.util.LazyMap;
import com.revolsys.util.UriBuilder;

public class HttpRequestBuilderFactory {
  public record Builder(HttpRequestBuilderFactory factory, URI uri) {

    public final HttpRequestBuilder httpDelete() {
      return request(HttpMethod.DELETE);
    }

    public final HttpRequestBuilder httpGet() {
      return request(HttpMethod.GET);
    }

    public final Builder parent() {
      final URI newUri = new UriBuilder(this.uri).removeQuery()
        .removeLastPathSegment()
        .build();
      return new Builder(this.factory, newUri);
    }

    public final Builder child(final String segment) {
      final URI newUri = new UriBuilder(this.uri).removeQuery()
        .appendPathSegments(segment)
        .build();
      return new Builder(this.factory, newUri);
    }

    public final Builder descendent(final String... segments) {
      final URI newUri = new UriBuilder(this.uri).removeQuery()
        .appendPathSegments(segments)
        .build();
      return new Builder(this.factory, newUri);
    }

    public final HttpRequestBuilder httpHead() {
      return request(HttpMethod.HEAD);
    }

    public final HttpRequestBuilder httpPatch() {
      return request(HttpMethod.PATCH);
    }

    public final HttpRequestBuilder httpOptions() {
      return request(HttpMethod.OPTIONS);
    }

    public final HttpRequestBuilder httpPost() {
      return request(HttpMethod.POST);
    }

    public final HttpRequestBuilder httpPut() {
      return request(HttpMethod.PUT);
    }

    public final HttpRequestBuilder httpTrace() {
      final HttpMethod method = HttpMethod.TRACE;
      return request(method);
    }

    public final HttpRequestBuilder request(final HttpMethod method) {
      return this.factory.create(method, this.uri);
    }
  }

  public static final HttpRequestBuilderFactory FACTORY = new HttpRequestBuilderFactory();

  private static LazyMap<BiFunction<ObjectFactoryConfig, JsonObject, HttpRequestBuilderFactory>> FACTORY_BY_NAME = new LazyMap<>(
    HttpRequestBuilderFactory::initFactories);

  public static HttpRequestBuilderFactory getDefault(final ObjectFactoryConfig factoryConfig) {
    return factoryConfig.getValue("defaultHttpRequestBuilderFactory");
  }

  private static void initFactories(
    final Map<String, BiFunction<ObjectFactoryConfig, JsonObject, HttpRequestBuilderFactory>> factories) {
    factories.put(OpenIdRefreshTokenRequestBuilderFactory.TYPE,
      OpenIdRefreshTokenRequestBuilderFactory::newFactory);
    factories.put(OpenIdConnectClient.TYPE_CLIENT_ID, OpenIdConnectClient::newHttpRequestBuilder);
  }

  public static HttpRequestBuilderFactory newFactory(final ObjectFactoryConfig factoryConfig,
    final JsonObject config) {
    final String type = config.getString("type");
    if (type != null && !"application".equals(type)) {
      final BiFunction<ObjectFactoryConfig, JsonObject, HttpRequestBuilderFactory> factory = FACTORY_BY_NAME
        .get(type);
      if (factory == null) {
        throw new IllegalArgumentException("Unknown factory: " + type);
      } else {
        return factory.apply(factoryConfig, config);
      }
    }
    return getDefault(factoryConfig);
  }

  public Builder builder(final URI uri) {
    return new Builder(this, uri);
  }

  public void configureClient(final HttpClientBuilder builder) {
  }

  public final HttpRequestBuilder copy(final HttpRequest request) {
    return newRequestBuilder().setRequest(request);
  }

  public final HttpRequestBuilder create(final HttpMethod method, final String uri) {
    return create(method, URI.create(uri));
  }

  public final HttpRequestBuilder create(final HttpMethod method, final URI uri) {
    return newRequestBuilder().setMethod(method)
      .setUri(uri);
  }

  public final HttpRequestBuilder delete(final String uri) {
    return create(HttpMethod.DELETE, uri);
  }

  public final HttpRequestBuilder delete(final URI uri) {
    return create(HttpMethod.DELETE, uri);
  }

  public final HttpRequestBuilder get(final String uri) {
    return create(HttpMethod.GET, uri);
  }

  public final HttpRequestBuilder get(final URI uri) {
    return create(HttpMethod.GET, uri);
  }

  public final HttpRequestBuilder head(final String uri) {
    return create(HttpMethod.HEAD, uri);
  }

  public final HttpRequestBuilder head(final URI uri) {
    return create(HttpMethod.HEAD, uri);
  }

  public HttpRequestBuilder newRequestBuilder() {
    return new HttpRequestBuilder(this);
  }

  public final HttpRequestBuilder patch(final URI uri) {
    return create(HttpMethod.PATCH, uri);
  }

  public final HttpRequestBuilder post(final String uri) {
    return create(HttpMethod.POST, uri);
  }

  public final HttpRequestBuilder post(final URI uri) {
    return create(HttpMethod.POST, uri);
  }

  public void preBuild(final HttpRequestBuilder requestBuilder) {
  }

  public final HttpRequestBuilder put(final String uri) {
    return create(HttpMethod.PUT, uri);
  }

  public final HttpRequestBuilder put(final URI uri) {
    return create(HttpMethod.PUT, uri);
  }

}
