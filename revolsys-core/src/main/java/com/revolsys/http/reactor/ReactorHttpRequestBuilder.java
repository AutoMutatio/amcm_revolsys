package com.revolsys.http.reactor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import com.revolsys.util.UriBuilder;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClient.RequestSender;
import reactor.netty.http.client.HttpClient.ResponseReceiver;

public class ReactorHttpRequestBuilder {

  private final DefaultHttpHeaders headers = new DefaultHttpHeaders();

  private final Map<String, ReactorHttpHeaderProvider> headerProviders = new LinkedHashMap<>();

  private final UriBuilder uriBuilder = new UriBuilder();

  public ReactorHttpRequestBuilder addHeader(final CharSequence name, final Object value) {
    this.headers.add(name, value);
    return this;
  }

  public ReactorHttpRequestBuilder addParameter(final CharSequence name, final Object value) {
    this.uriBuilder.addParameter(name, value);
    return this;
  }

  public final RequestSender delete() {
    return request(HttpMethod.DELETE);
  }

  public final ResponseReceiver<?> get() {
    return request(HttpMethod.GET);
  }

  public final ResponseReceiver<?> head() {
    return request(HttpMethod.HEAD);
  }

  public ReactorHttpRequestBuilder headers(final Consumer<HttpHeaders> action) {
    action.accept(this.headers);
    return this;
  }

  public ReactorHttpRequestBuilder host(final String host) {
    this.uriBuilder.setHost(host);
    return this;
  }

  public final ResponseReceiver<?> options() {
    return request(HttpMethod.OPTIONS);
  }

  public final RequestSender patch() {
    return request(HttpMethod.PATCH);
  }

  public ReactorHttpRequestBuilder path(final String path) {
    this.uriBuilder.setPath(path);
    return this;
  }

  public ReactorHttpRequestBuilder port(final int port) {
    this.uriBuilder.setPort(port);
    return this;
  }

  public final RequestSender post() {
    return request(HttpMethod.POST);
  }

  public final RequestSender put() {
    return request(HttpMethod.PUT);
  }

  public ReactorHttpRequestBuilder removeHeader(final CharSequence name) {
    this.headers.remove(name);
    return this;
  }

  public ReactorHttpRequestBuilder removeParameter(final CharSequence name, final Object value) {
    this.uriBuilder.removeParameter(name);
    return this;
  }

  public RequestSender request(final HttpMethod method) {
    final UriBuilder uriBuilder = this.uriBuilder;
    final DefaultHttpHeaders headers = this.headers;
    final String uri = uriBuilder.buildString();
    return HttpClient.create().headers(requestHeaders -> {
      requestHeaders.add(headers);
      for (final Entry<String, ReactorHttpHeaderProvider> entry : this.headerProviders.entrySet()) {
        final String name = entry.getKey();
        final ReactorHttpHeaderProvider provider = entry.getValue();
        requestHeaders.add(name, provider.provide(this, uri, method, requestHeaders));
      }
    }).request(method).uri(uri);
  }

  public ReactorHttpRequestBuilder setHeader(final CharSequence name, final Object value) {
    this.headers.set(name, value);
    return this;
  }

  public ReactorHttpRequestBuilder setHeader(final String name,
    final ReactorHttpHeaderProvider provider) {
    this.headerProviders.put(name, provider);
    return this;
  }

  public ReactorHttpRequestBuilder setParameter(final CharSequence name, final Object value) {
    this.uriBuilder.setParameter(name, value);
    return this;
  }

  public String uri() {
    return this.uriBuilder.buildString();
  }

}
