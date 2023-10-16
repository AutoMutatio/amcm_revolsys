package com.revolsys.io;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.map.MapEx;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.ObjectWithProperties;

public interface Writer<T> extends ObjectWithProperties, BaseCloseable {
  @Override
  default void close() {
    ObjectWithProperties.super.close();
  }

  default void flush() {
  }

  @Override
  default MapEx getProperties() {
    return JsonObject.EMPTY;
  }

  default void open() {
  }

  void write(T object);
}
