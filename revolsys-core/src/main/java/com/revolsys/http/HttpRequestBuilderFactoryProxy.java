package com.revolsys.http;

import java.io.InputStream;
import java.net.URI;

public interface HttpRequestBuilderFactoryProxy {

  default HttpRequestBuilder delete(final String uri) {
    final HttpRequestBuilder requestBuilder = getRequestBuilderFactory().delete(uri);
    return initRequestBuilder(requestBuilder);
  }

  default HttpRequestBuilder delete(final URI uri) {
    final HttpRequestBuilder requestBuilder = getRequestBuilderFactory().delete(uri);
    return initRequestBuilder(requestBuilder);
  }

  default HttpRequestBuilder get(final String uri) {
    final HttpRequestBuilder requestBuilder = getRequestBuilderFactory().get(uri);
    return initRequestBuilder(requestBuilder);
  }

  default HttpRequestBuilder get(final URI uri) {
    final HttpRequestBuilder requestBuilder = getRequestBuilderFactory().get(uri);
    return initRequestBuilder(requestBuilder);
  }

  default InputStream getInputStream(final String uri) {
    final HttpRequestBuilder requestBuilder = get(uri);
    return requestBuilder.newInputStream();
  }

  default InputStream getInputStream(final URI uri) {
    final HttpRequestBuilder requestBuilder = get(uri);
    return requestBuilder.newInputStream();
  }

  HttpRequestBuilderFactory getRequestBuilderFactory();

  default HttpRequestBuilder head(final String uri) {
    final HttpRequestBuilder requestBuilder = getRequestBuilderFactory().head(uri);
    return initRequestBuilder(requestBuilder);
  }

  default HttpRequestBuilder head(final URI uri) {
    final HttpRequestBuilder requestBuilder = getRequestBuilderFactory().head(uri);
    return initRequestBuilder(requestBuilder);
  }

  default HttpRequestBuilder initRequestBuilder(final HttpRequestBuilder requestBuilder) {
    return requestBuilder;
  }

  default HttpRequestBuilder post(final String uri) {
    final HttpRequestBuilder requestBuilder = getRequestBuilderFactory().post(uri);
    return initRequestBuilder(requestBuilder);
  }

  default HttpRequestBuilder post(final URI uri) {
    final HttpRequestBuilder requestBuilder = getRequestBuilderFactory().post(uri);
    return initRequestBuilder(requestBuilder);
  }

  default HttpRequestBuilder put(final String uri) {
    final HttpRequestBuilder requestBuilder = getRequestBuilderFactory().put(uri);
    return initRequestBuilder(requestBuilder);
  }

  default HttpRequestBuilder put(final URI uri) {
    final HttpRequestBuilder requestBuilder = getRequestBuilderFactory().put(uri);
    return initRequestBuilder(requestBuilder);
  }

}
