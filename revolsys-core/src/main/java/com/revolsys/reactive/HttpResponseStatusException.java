package com.revolsys.reactive;

public class HttpResponseStatusException extends RuntimeException {

  private final int code;

  private final String reasonPhrase;

  public HttpResponseStatusException(final int code, final String reasonPhrase) {
    super(code + "\t" + reasonPhrase);
    this.code = code;
    this.reasonPhrase = reasonPhrase;
  }

  public int getCode() {
    return this.code;
  }

  public String getReasonPhrase() {
    return this.reasonPhrase;
  }
}
