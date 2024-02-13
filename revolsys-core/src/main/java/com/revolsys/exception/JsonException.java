package com.revolsys.exception;

import com.revolsys.collection.json.JsonObject;

public class JsonException extends RuntimeException {

  private final JsonObject data;

  public JsonException(final String message) {
    super(message);
    this.data = JsonObject.EMPTY;
  }

  public JsonException(final String message, final JsonObject data) {
    super(message);
    this.data = data;
  }

  public JsonException(final String message, final Throwable cause, final JsonObject data) {
    super(message, cause);
    this.data = data;
  }

  public JsonObject getData() {
    return this.data;
  }
}
