package com.revolsys.record.io.format.odata;

import java.io.InputStream;
import java.util.Optional;
import java.util.function.Consumer;

import org.jeometry.common.json.JsonObject;

import com.revolsys.http.HttpRequestBuilder;

public record ODataRequestBuilder(HttpRequestBuilder request) {
  public InputStream newInputStream() {
    return this.request.newInputStream();
  }

  public JsonObject getJsonObject() {
    return this.request.getJson();
  }

  public Optional<JsonObject> getJsonObjectOptional() {
    return this.request.getJsonObjectOptional();
  }

  public HttpRequestBuilder setJsonEntity(final JsonObject message) {
    return this.request.setJsonEntity(message);
  }

  public ODataRequestBuilder withBuilder(final Consumer<HttpRequestBuilder> action) {
    action.accept(this.request);
    return this;
  }
}
