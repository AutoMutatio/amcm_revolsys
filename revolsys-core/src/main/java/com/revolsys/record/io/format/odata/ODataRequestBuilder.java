package com.revolsys.record.io.format.odata;

import java.io.InputStream;
import java.util.function.Consumer;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.value.Single;
import com.revolsys.http.HttpRequestBuilder;

public record ODataRequestBuilder(HttpRequestBuilder request) {
  public InputStream newInputStream() {
    return this.request.responseAsInputStream();
  }

  public JsonObject getJsonObject() {
    return this.request.responseAsJson();
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
