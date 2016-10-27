package com.revolsys.geometry.model.impl;

import com.revolsys.geometry.model.GeometryFactory;

public class PointDoubleXYGeometryFactory extends PointDoubleXY {
  private static final long serialVersionUID = 1L;

  private final GeometryFactory geometryFactory;

  public PointDoubleXYGeometryFactory(final GeometryFactory geometryFactory, final double x,
    final double y) {
    super(geometryFactory, x, y);
    this.geometryFactory = geometryFactory;
  }

  @Override
  public GeometryFactory getGeometryFactory() {
    return this.geometryFactory;
  }
}