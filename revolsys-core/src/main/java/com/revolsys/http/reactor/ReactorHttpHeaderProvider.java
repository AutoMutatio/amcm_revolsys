package com.revolsys.http.reactor;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

public interface ReactorHttpHeaderProvider {

  public String provide(ReactorHttpRequestBuilder builder, String uri, HttpMethod method,
    HttpHeaders headers);
}
