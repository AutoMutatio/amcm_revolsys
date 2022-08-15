package com.revolsys.http.reactor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.http.NameValuePair;

import com.revolsys.util.UriBuilder;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClient.RequestSender;
import reactor.netty.http.client.HttpClient.ResponseReceiver;

public class ReactorHttpRequestBuilder {

  private final HttpClient client = HttpClient.create();

  private final DefaultHttpHeaders headers = new DefaultHttpHeaders();

  private final UriBuilder uriBuilder = new UriBuilder();

  private final Map<String, List<NameValuePair>> parameters = new LinkedHashMap<>();

  public ReactorHttpRequestBuilder addHeader(final CharSequence name, final Object value) {
    this.headers.add(name, value);
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
    this.client.headers(action);
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

  public RequestSender request(final HttpMethod method) {
    final String uri = uri();
    return HttpClient.create()
      .headers(headers -> headers.add(this.headers))
      .request(method)
      .uri(uri);
  }

  public ReactorHttpRequestBuilder setHeader(final CharSequence name, final Object value) {
    this.headers.set(name, value);
    return this;
  }

  public String uri() {
    return this.uriBuilder.buildString();
  }
}
