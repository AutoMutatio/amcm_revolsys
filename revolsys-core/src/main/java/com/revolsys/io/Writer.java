package com.revolsys.io;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.map.MapEx;
import com.revolsys.properties.ObjectWithProperties;
<<<<<<< HEAD
=======
import com.revolsys.record.io.format.json.JsonObject;
>>>>>>> upstream/main
import com.revolsys.util.BaseCloseable;

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
