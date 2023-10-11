package com.revolsys.record.io.format.odata;

import java.io.InputStream;

import org.jeometry.common.json.JsonObject;

import com.revolsys.http.HttpRequestBuilder;

public record ODataRequestBuilder(HttpRequestBuilder request) {
  public InputStream newInputStream() {
    return this.request.newInputStream();
  }

  public JsonObject getJsonObject() {
    return this.request.getJson();
  }
}
