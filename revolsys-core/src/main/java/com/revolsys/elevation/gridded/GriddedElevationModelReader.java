package com.revolsys.elevation.gridded;

import org.jeometry.common.collection.map.MapEx;
import org.jeometry.common.json.JsonObject;
import org.jeometry.common.util.BaseCloseable;
import org.jeometry.common.util.ObjectWithProperties;

import com.revolsys.geometry.model.BoundingBoxProxy;
import com.revolsys.io.IoFactory;
import com.revolsys.spring.resource.Resource;

public interface GriddedElevationModelReader
  extends BaseCloseable, BoundingBoxProxy, ObjectWithProperties {
  static boolean isReadable(final Object source) {
    return IoFactory.isAvailable(GriddedElevationModelReaderFactory.class, source);
  }

  static <R extends GriddedElevationModelReader> R newGriddedElevationModelReader(
    final Object source) {
    final MapEx properties = JsonObject.EMPTY;
    return newGriddedElevationModelReader(source, properties);
  }

  @SuppressWarnings("unchecked")
  static <R extends GriddedElevationModelReader> R newGriddedElevationModelReader(
    final Object source, final MapEx properties) {
    final GriddedElevationModelReaderFactory factory = IoFactory
      .factory(GriddedElevationModelReaderFactory.class, source);
    if (factory == null) {
      return null;
    } else {
      final Resource resource = Resource.getResource(source);
      return (R)factory.newGriddedElevationModelReader(resource, properties);
    }
  }

  @Override
  default void close() {
    ObjectWithProperties.super.close();
  }

  double getGridCellHeight();

  double getGridCellWidth();

  GriddedElevationModel read();
}
