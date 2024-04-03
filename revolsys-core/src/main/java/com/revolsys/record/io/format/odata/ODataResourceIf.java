package com.revolsys.record.io.format.odata;

import java.io.InputStream;
import java.net.URI;

import com.revolsys.http.HttpMethod;
import com.revolsys.http.HttpRequestBuilder;
import com.revolsys.http.HttpRequestBuilderFactory;

public interface ODataResourceIf {

  default ODataRequestBuilder deleteBuilder() {
    return request(HttpMethod.DELETE);
  }

  default ODataRequestBuilder get() {
    return request(HttpMethod.GET);
  }

  HttpRequestBuilderFactory getFactory();

  default InputStream getInputStream() {
    return get().newInputStream();
  }

  URI getUri();

  default ODataRequestBuilder head() {
    return request(HttpMethod.HEAD);
  }

  default ODataRequestBuilder options() {
    return request(HttpMethod.OPTIONS);
  }

  default ODataRequestBuilder patch() {
    return request(HttpMethod.PATCH);
  }

  default ODataRequestBuilder post() {
    return request(HttpMethod.POST);
  }

  default ODataRequestBuilder put() {
    return request(HttpMethod.PUT);
  }

  default ODataRequestBuilder request(final HttpMethod method) {
    return new ODataRequestBuilder(createHttpRequest(method));
  }

  default HttpRequestBuilder createHttpRequest(final HttpMethod method) {
    return getFactory().create(method, getUri());
  }

  default ODataRequestBuilder trace() {
    final HttpMethod method = HttpMethod.TRACE;
    return request(method);
  }
}