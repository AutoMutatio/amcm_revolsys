package com.revolsys.net.http;

import java.net.URI;
import java.util.Arrays;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpUriRequest;

import com.revolsys.exception.ExceptionWithProperties;
import com.revolsys.http.HttpRequestBuilder;

public class ApacheHttpException extends ExceptionWithProperties {
  private static final long serialVersionUID = 1L;

  public static ApacheHttpException create(final HttpUriRequest request,
    final HttpResponse response) {
    final StatusLine statusLine = response.getStatusLine();
    String content;
    try {
      content = HttpRequestBuilder.getString(response);
    } catch (final Exception e) {
      content = null;
    }
    return new ApacheHttpException(request.getURI(), statusLine, content, response.getAllHeaders());
  }

  private final int statusCode;

  private final String reasonPhrase;

  private final String content;

  private final URI requestUri;

  private final Header[] headers;

  public ApacheHttpException(final URI requestUri, final StatusLine statusLine,
    final String content, final Header[] headers) {
    super(requestUri + "\n" + statusLine + "\n" + content);
    this.requestUri = requestUri;
    this.statusCode = statusLine.getStatusCode();
    this.reasonPhrase = statusLine.getReasonPhrase();
    this.content = content;
    this.headers = headers;
    property("uri", requestUri).property("headers", Arrays.asList(headers))
      .property("content", content);
  }

  public String getContent() {
    return this.content;
  }

  public String getHeader(final String name) {
    for (final Header header : this.headers) {
      if (header.getName()
        .equalsIgnoreCase(name)) {
        return header.getValue();
      }
    }
    return null;
  }

  public String getReasonPhrase() {
    return this.reasonPhrase;
  }

  public URI getRequestUri() {
    return this.requestUri;
  }

  public int getStatusCode() {
    return this.statusCode;
  }

}
