package com.revolsys.record.io.format.odata;

import java.net.URI;

import com.revolsys.http.HttpRequestBuilderFactory;
import com.revolsys.util.UriBuilder;

public record ODataResource(HttpRequestBuilderFactory factory, URI uri) implements ODataResourceIf {

  public final ODataResourceIf parent() {
    final URI newUri = new UriBuilder(this.uri).removeQuery().removeLastPathSegment().build();
    return new ODataResource(this.factory, newUri);
  }

  public final ODataResourceIf child(final String segment) {
    final URI newUri = new UriBuilder(this.uri).removeQuery().appendPathSegments(segment).build();
    return new ODataResource(this.factory, newUri);
  }

  public final ODataResourceIf appendString(final String string) {
    final URI newUri = new UriBuilder(this.uri).removeQuery().appendPathString(string).build();
    return new ODataResource(this.factory, newUri);
  }

  public final ODataResourceIf descendent(final String... segments) {
    final URI newUri = new UriBuilder(this.uri).removeQuery().appendPathSegments(segments).build();
    return new ODataResource(this.factory, newUri);
  }

  public final String lastSegment() {
    return new UriBuilder(this.uri).lastPathSegment();
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public HttpRequestBuilderFactory getFactory() {
    return this.factory;
  }

}
