package com.revolsys.record.io.format.odata;

import java.net.URI;

import com.revolsys.http.HttpRequestBuilderFactory;
import com.revolsys.util.UriBuilder;

public record ODataResource(HttpRequestBuilderFactory factory, URI uri) implements ODataResourceIf {

  @Override
  public final ODataResourceIf parent() {
    final URI newUri = new UriBuilder(this.uri).removeQuery()
      .removeLastPathSegment()
      .build();
    return new ODataResource(this.factory, newUri);
  }

  @Override
  @SuppressWarnings("unchecked")
  public final <V extends ODataResourceIf> V child(final String segment) {
    final URI newUri = new UriBuilder(this.uri).removeQuery()
      .appendPathSegments(segment)
      .build();
    return (V)new ODataResource(this.factory, newUri);
  }

  public final ODataResourceIf appendString(final String string) {
    final URI newUri = new UriBuilder(this.uri).removeQuery()
      .appendPathString(string)
      .build();
    return new ODataResource(this.factory, newUri);
  }

  public final ODataResourceIf descendent(final String... segments) {
    final URI newUri = new UriBuilder(this.uri).removeQuery()
      .appendPathSegments(segments)
      .build();
    return new ODataResource(this.factory, newUri);
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
