package com.revolsys.http;

import java.net.http.HttpRequest;

public class HttpResponseException extends RuntimeException {

  private final int statusCode;

  private final String reasonPhrase;

  private final HttpRequest request;

  public HttpResponseException(final int statusCode, final String reasonPhrase,
    final HttpRequest request) {
    super(statusCode + " " + reasonPhrase + " " + request.uri());
    this.statusCode = statusCode;
    this.reasonPhrase = reasonPhrase;
    this.request = request;
  }

  public String getReasonPhrase() {
    return this.reasonPhrase;
  }

  public HttpRequest getRequest() {
    return this.request;
  }

  public int getStatusCode() {
    return this.statusCode;
  }

}
