package com.revolsys.geometry.coordinatesystem.operation.gridshift;

import com.revolsys.geometry.coordinatesystem.operation.CoordinatesOperation;
import com.revolsys.geometry.coordinatesystem.operation.CoordinatesOperationPoint;

public interface VerticalShiftOperation extends CoordinatesOperation {

  @Override
  default void perform(final CoordinatesOperationPoint point) {
    verticalShift(point);
  }

  boolean verticalShift(CoordinatesOperationPoint point);
}
