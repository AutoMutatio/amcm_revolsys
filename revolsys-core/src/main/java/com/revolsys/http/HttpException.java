package com.revolsys.http;

public class HttpException extends RuntimeException {

  private int code;

  private String reason;

  public HttpException(int code, String reason, Throwable e) {
    super(code + " " + reason, e);
    this.code = code;
    this.reason = reason;
  }

  public HttpException(int code, String reason) {
    this(code, reason, null);
  }

  public int getCode() {
    return code;
  }

  public String getReason() {
    return reason;
  }
}
