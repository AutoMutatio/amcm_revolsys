package com.revolsys.io;

import org.jeometry.common.collection.map.MapEx;
import org.jeometry.common.json.JsonObject;
import org.jeometry.common.util.BaseCloseable;
import org.jeometry.common.util.ObjectWithProperties;

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
