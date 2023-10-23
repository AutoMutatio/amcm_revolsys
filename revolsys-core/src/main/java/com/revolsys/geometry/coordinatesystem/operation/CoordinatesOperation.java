package com.revolsys.geometry.coordinatesystem.operation;

import com.revolsys.function.BiConsumerDouble;

public interface CoordinatesOperation {

  void perform(CoordinatesOperationPoint point);

  default void perform2d(final CoordinatesOperationPoint point, final double x, final double y,
    final BiConsumerDouble action) {
    point.setPoint(x, y);
    perform(point);
    point.apply2d(action);
  }
}
