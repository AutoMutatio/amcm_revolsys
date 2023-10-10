package com.revolsys.http;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.jeometry.common.json.JsonType;

public class JsonEntity extends StringEntity {

  public JsonEntity(final JsonType json) {
    super(json.toJsonString(false), ContentType.APPLICATION_JSON);
  }

}
