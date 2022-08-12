package com.revolsys.http.reactor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.http.NameValuePair;

import com.revolsys.util.UriBuilder;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import reactor.netty.http.client.HttpClient;

public class ReactorHttpRequestBuilder {

  private final HttpClient client = HttpClient.create();

  private final UriBuilder uriBuilder = new UriBuilder();

  private HttpMethod method;

  private final Map<String, List<NameValuePair>> parameters = new LinkedHashMap<>();

  public ReactorHttpRequestBuilder headers(final Consumer<HttpHeaders> action) {
    this.client.headers(action);
    return this;
  }

  public ReactorHttpRequestBuilder host(final String host) {
    this.uriBuilder.setHost(host);
    return this;
  }

  public ReactorHttpRequestBuilder path(final String path) {
    this.uriBuilder.setPath(path);
    return this;
  }

  public ReactorHttpRequestBuilder port(final int port) {
    this.uriBuilder.setPort(port);
    return this;
  }

  public String uri() {
    return this.uriBuilder.buildString();
  }

}
