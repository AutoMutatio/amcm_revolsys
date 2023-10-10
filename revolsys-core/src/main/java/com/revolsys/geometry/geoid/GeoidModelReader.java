package com.revolsys.geometry.geoid;

import org.jeometry.common.util.BaseCloseable;
import org.jeometry.common.util.ObjectWithProperties;

import com.revolsys.geometry.model.BoundingBoxProxy;
import com.revolsys.io.IoFactory;

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
