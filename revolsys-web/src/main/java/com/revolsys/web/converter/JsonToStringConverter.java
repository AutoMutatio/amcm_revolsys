package com.revolsys.web.converter;

import org.springframework.core.convert.converter.Converter;

import com.revolsys.collection.json.JsonObject;

public class JsonToStringConverter implements Converter<JsonObject, String>{

  @Override
  public String convert(JsonObject source) {
    if (source == null) {
      return "null";
      } else {
      return source.toJsonString();
    }
  }

}
