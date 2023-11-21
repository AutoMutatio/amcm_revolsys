package com.revolsys.record.io.format.odata;

import java.io.InputStream;
import java.net.URI;

import com.revolsys.http.HttpMethod;
import com.revolsys.http.HttpRequestBuilderFactory;
import com.revolsys.util.UriBuilder;

public record ODataResource(HttpRequestBuilderFactory factory, URI uri) {

  public final ODataRequestBuilder delete() {
    return request(HttpMethod.DELETE);
  }

  public final ODataRequestBuilder get() {
    return request(HttpMethod.GET);
  }

  public final ODataResource parent() {
    final URI newUri = new UriBuilder(this.uri).removeQuery().removeLastPathSegment().build();
    return new ODataResource(this.factory, newUri);
  }

  public final ODataResource child(final String segment) {
    final URI newUri = new UriBuilder(this.uri).removeQuery().appendPathSegments(segment).build();
    return new ODataResource(this.factory, newUri);
  }

  public final ODataResource appendString(final String string) {
    final URI newUri = new UriBuilder(this.uri).removeQuery().appendPathString(string).build();
    return new ODataResource(this.factory, newUri);
  }

  public final ODataResource descendent(final String... segments) {
    final URI newUri = new UriBuilder(this.uri).removeQuery().appendPathSegments(segments).build();
    return new ODataResource(this.factory, newUri);
  }

  public final String lastSegment() {
    return new UriBuilder(this.uri).lastPathSegment();
  }

  public InputStream getInputStream() {
    return get().newInputStream();
  }

  public URI getUri() {
    return this.uri;
  }

  public final ODataRequestBuilder head() {
    return request(HttpMethod.HEAD);
  }

  public final ODataRequestBuilder patch() {
    return request(HttpMethod.PATCH);
  }

  public final ODataRequestBuilder options() {
    return request(HttpMethod.OPTIONS);
  }

  public final ODataRequestBuilder post() {
    return request(HttpMethod.POST);
  }

  public final ODataRequestBuilder put() {
    return request(HttpMethod.PUT);
  }

  public final ODataRequestBuilder trace() {
    final HttpMethod method = HttpMethod.TRACE;
    return request(method);
  }

  public ODataRequestBuilder request(final HttpMethod method) {
    return new ODataRequestBuilder(this.factory.create(method, this.uri));
  }

  public HttpRequestBuilderFactory getFactory() {
    return this.factory;
  }

}
