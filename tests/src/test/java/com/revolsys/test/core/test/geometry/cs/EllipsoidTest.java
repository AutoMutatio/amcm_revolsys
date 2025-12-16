package com.revolsys.test.core.test.geometry.cs;

import org.junit.Test;

import com.revolsys.geometry.coordinatesystem.model.Ellipsoid;
import com.revolsys.geometry.coordinatesystem.model.GeographicCoordinateSystem;
import com.revolsys.geometry.coordinatesystem.model.systems.EpsgCoordinateSystems;
import com.revolsys.geometry.coordinatesystem.model.systems.EpsgId;
import com.revolsys.geometry.coordinatesystem.operation.CoordinatesOperationPoint;

public class EllipsoidTest {
  private static final GeographicCoordinateSystem NAD83 = EpsgCoordinateSystems
    .getCoordinateSystem(EpsgId.NAD83);

  private static final Ellipsoid NAD83_ELLIPSOID = NAD83.getEllipsoid();

  @Test
  public void testCartesianToGeodetic() {
    final CoordinatesOperationPoint point = new CoordinatesOperationPoint(-123, 49, 50);
    NAD83_ELLIPSOID.geodeticToCartesian(point);

  }
}
