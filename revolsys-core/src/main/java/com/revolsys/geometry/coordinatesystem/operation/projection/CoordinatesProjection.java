package com.revolsys.geometry.coordinatesystem.operation.projection;

import com.revolsys.geometry.coordinatesystem.operation.CoordinatesOperation;
import com.revolsys.geometry.coordinatesystem.operation.CoordinatesOperationPoint;

public interface CoordinatesProjection {
  CoordinatesOperation getInverseOperation();

  CoordinatesOperation getProjectOperation();

  void inverse(CoordinatesOperationPoint point);

  void project(CoordinatesOperationPoint point);
}
