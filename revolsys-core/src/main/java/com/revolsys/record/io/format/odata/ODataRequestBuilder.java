package com.revolsys.record.io.format.odata;

import java.io.InputStream;
import java.util.function.Consumer;

import org.jeometry.common.json.JsonObject;
import org.jeometry.common.util.Single;

import com.revolsys.http.HttpRequestBuilder;

public record ODataRequestBuilder(HttpRequestBuilder request) {
  public InputStream newInputStream() {
    return this.request.newInputStream();
  }

  public JsonObject getJsonObject() {
    return this.request.getJson();
  }

  public Single<JsonObject> jsonObject() {
    return this.request.jsonObject();
  }

  public HttpRequestBuilder setJsonEntity(final JsonObject message) {
    return this.request.setJsonEntity(message);
  }

  public ODataRequestBuilder withBuilder(final Consumer<HttpRequestBuilder> action) {
    action.accept(this.request);
    return this;
  }
}
