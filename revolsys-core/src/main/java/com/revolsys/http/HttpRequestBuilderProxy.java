package com.revolsys.http;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;

public interface HttpRequestBuilderProxy {
  default HttpRequestBuilderProxy addHeader(final Header header) {
    getRequestBuilder().addHeader(header);
    return this;
  }

  default HttpRequestBuilderProxy addHeader(final String name, final String value) {
    getRequestBuilder().addHeader(name, value);
    return this;
  }

  default HttpRequestBuilderProxy addParameter(final NameValuePair nvp) {
    getRequestBuilder().addParameter(nvp);
    return this;
  }

  default HttpRequestBuilderProxy addParameter(final String name, final Object value) {
    getRequestBuilder().addParameter(name, value);
    return this;
  }

  default HttpRequestBuilderProxy addParameters(final NameValuePair... nvps) {
    getRequestBuilder().getParameters();
    return this;
  }

  default HttpUriRequest build() {
    return getRequestBuilder().build();
  }

  default Charset getCharset() {
    return getRequestBuilder().getCharset();
  }

  default RequestConfig getConfig() {
    return getRequestBuilder().getConfig();
  }

  default HttpEntity getEntity() {
    return getRequestBuilder().getEntity();
  }

  default Header getFirstHeader(final String name) {
    return getRequestBuilder().getFirstHeader(name);
  }

  default Header[] getHeaders(final String name) {
    return getRequestBuilder().getHeaders(name);
  }

  default Header getLastHeader(final String name) {
    return getRequestBuilder().getLastHeader(name);
  }

  default String getMethod() {
    return getRequestBuilder().getMethod();
  }

  default List<NameValuePair> getParameters() {
    return getRequestBuilder().getParameters();
  }

  HttpRequestBuilder getRequestBuilder();

  default URI getUri() {
    return getRequestBuilder().getUri();
  }

  default ProtocolVersion getVersion() {
    return getRequestBuilder().getVersion();
  }

  default HttpRequestBuilderProxy removeHeader(final Header header) {
    getRequestBuilder().removeHeader(header);
    return this;
  }

  default HttpRequestBuilderProxy removeHeaders(final String name) {
    getRequestBuilder().removeHeaders(name);
    return this;
  }

  default HttpRequestBuilderProxy setConfig(final RequestConfig config) {
    getRequestBuilder().setConfig(config);
    return this;
  }

  default HttpRequestBuilderProxy setEntity(final HttpEntity entity) {
    getRequestBuilder().setEntity(entity);
    return this;
  }

  default HttpRequestBuilderProxy setParameter(final NameValuePair nvp) {
    getRequestBuilder().setParameter(nvp);
    return this;
  }

  default HttpRequestBuilderProxy setParameter(final String name, final Object value) {
    getRequestBuilder().setParameter(name, value);
    return this;
  }

}
