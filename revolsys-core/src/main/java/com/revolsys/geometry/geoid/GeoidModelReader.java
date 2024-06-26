package com.revolsys.geometry.geoid;

import com.revolsys.geometry.model.BoundingBoxProxy;
import com.revolsys.io.IoFactory;
import com.revolsys.properties.ObjectWithProperties;
import com.revolsys.util.BaseCloseable;

public interface GeoidModelReader extends BaseCloseable, BoundingBoxProxy, ObjectWithProperties {
  static boolean isReadable(final Object source) {
    return IoFactory.isAvailable(GeoidModelReaderFactory.class, source);
  }

  @Override
  default void close() {
    ObjectWithProperties.super.close();
  }

  GeoidModel read();
}
