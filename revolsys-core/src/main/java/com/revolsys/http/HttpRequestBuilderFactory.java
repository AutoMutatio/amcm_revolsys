package com.revolsys.http;

import java.io.InputStream;
import java.net.URI;

import org.apache.http.HttpRequest;

public class HttpRequestBuilderFactory {
  public static final HttpRequestBuilderFactory FACTORY = new HttpRequestBuilderFactory();

  public HttpRequestBuilder copy(final HttpRequest request) {
    return newRequestBuilder().setRequest(request);
  }

  public HttpRequestBuilder create(final HttpMethod method, final String uri) {
    return create(method, URI.create(uri));
  }

  public HttpRequestBuilder create(final HttpMethod method, final URI uri) {
    return newRequestBuilder().setMethod(method).setUri(uri);
  }

  public HttpRequestBuilder delete(final String uri) {
    return create(HttpMethod.DELETE, uri);
  }

  public HttpRequestBuilder delete(final URI uri) {
    return create(HttpMethod.DELETE, uri);
  }

  public HttpRequestBuilder get(final String uri) {
    return create(HttpMethod.GET, uri);
  }

  public HttpRequestBuilder get(final URI uri) {
    return create(HttpMethod.GET, uri);
  }

  public InputStream getInputStream(final String uri) {
    final HttpRequestBuilder requestBuilder = get(uri);
    return requestBuilder.newInputStream();
  }

  public InputStream getInputStream(final URI uri) {
    final HttpRequestBuilder requestBuilder = get(uri);
    return requestBuilder.newInputStream();
  }

  public HttpRequestBuilder head(final String uri) {
    return create(HttpMethod.HEAD, uri);
  }

  public HttpRequestBuilder head(final URI uri) {
    return create(HttpMethod.HEAD, uri);
  }

  public HttpRequestBuilder newRequestBuilder() {
    return new HttpRequestBuilder(this);
  }

  public HttpRequestBuilder patch(final URI uri) {
    return create(HttpMethod.PATCH, uri);
  }

  public HttpRequestBuilder post(final String uri) {
    return create(HttpMethod.POST, uri);
  }

  public HttpRequestBuilder post(final URI uri) {
    return create(HttpMethod.POST, uri);
  }

  public HttpRequestBuilder put(final String uri) {
    return create(HttpMethod.PUT, uri);
  }

  public HttpRequestBuilder put(final URI uri) {
    return create(HttpMethod.PUT, uri);
  }

}
