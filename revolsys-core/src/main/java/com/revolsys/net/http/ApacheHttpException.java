package com.revolsys.net.http;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

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

  public ApacheHttpException(final URI requestUri, final StatusLine statusLine,
    final String content, final Header[] headers) {
    super(statusLine.toString());
    final var statusCode = statusLine.getStatusCode();
    final var reasonPhrase = statusLine.getReasonPhrase();
    property("uri", requestUri);
    property("statusCode", statusCode);
    property("statusMessage", reasonPhrase);
    property("headers", Arrays.asList(headers));
    property("content", content);
  }

  public String getContent() {
    return property("content");
  }

  public String getHeader(final String name) {
    for (final Header header : this.<List<Header>> property("headers")) {
      if (header.getName()
        .equalsIgnoreCase(name)) {
        return header.getValue();
      }
    }
    return null;
  }

  public String getReasonPhrase() {
    return property("reasonPhrase");
  }

  public URI getRequestUri() {
    return property("uri");
  }

  public int getStatusCode() {
    return property("statusCode");
  }

}
