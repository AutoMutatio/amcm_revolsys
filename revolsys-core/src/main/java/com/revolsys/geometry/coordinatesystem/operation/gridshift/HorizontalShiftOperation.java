package com.revolsys.geometry.coordinatesystem.operation.gridshift;

import com.revolsys.geometry.coordinatesystem.operation.CoordinatesOperation;
import com.revolsys.geometry.coordinatesystem.operation.CoordinatesOperationPoint;

public interface HorizontalShiftOperation extends CoordinatesOperation {

  boolean horizontalShift(CoordinatesOperationPoint point);

  @Override
  default void perform(final CoordinatesOperationPoint point) {
    horizontalShift(point);
  }
}
