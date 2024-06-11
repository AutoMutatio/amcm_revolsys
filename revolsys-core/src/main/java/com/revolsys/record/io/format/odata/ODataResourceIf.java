package com.revolsys.record.io.format.odata;

import java.io.InputStream;
import java.net.URI;

import com.revolsys.http.HttpMethod;
import com.revolsys.http.HttpRequestBuilder;
import com.revolsys.http.HttpRequestBuilderFactory;
import com.revolsys.util.UriBuilder;

public interface ODataResourceIf {
  <V extends ODataResourceIf> V child(final String segment);

  default HttpRequestBuilder createHttpRequest(final HttpMethod method) {
    return getFactory().create(method, getUri());
  }

  HttpRequestBuilderFactory getFactory();

  default InputStream getInputStream() {
    return odataGet().newInputStream();
  }

  URI getUri();

  default String lastSegment() {
    return new UriBuilder(getUri()).lastPathSegment();
  }

  default ODataRequestBuilder odataDelete() {
    return odataRequest(HttpMethod.DELETE);
  }

  default ODataRequestBuilder odataGet() {
    return odataRequest(HttpMethod.GET);
  }

  default ODataRequestBuilder odataHead() {
    return odataRequest(HttpMethod.HEAD);
  }

  default ODataRequestBuilder odataOptions() {
    return odataRequest(HttpMethod.OPTIONS);
  }

  default ODataRequestBuilder odataPatch() {
    return odataRequest(HttpMethod.PATCH);
  }

  default ODataRequestBuilder odataPost() {
    return odataRequest(HttpMethod.POST);
  }

  default ODataRequestBuilder odataPut() {
    return odataRequest(HttpMethod.PUT);
  }

  default ODataRequestBuilder odataRequest(final HttpMethod method) {
    return new ODataRequestBuilder(createHttpRequest(method));
  }

  default ODataRequestBuilder odataTrace() {
    return odataRequest(HttpMethod.TRACE);
  }

  ODataResourceIf parent();

  default ODataQuery query() {
    return new ODataQuery(this);
  }
}
