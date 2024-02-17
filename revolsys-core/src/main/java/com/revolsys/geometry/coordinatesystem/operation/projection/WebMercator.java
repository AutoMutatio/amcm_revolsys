package com.revolsys.geometry.coordinatesystem.operation.projection;

import com.revolsys.geometry.coordinatesystem.model.Ellipsoid;
import com.revolsys.geometry.coordinatesystem.model.NormalizedParameterNames;
import com.revolsys.geometry.coordinatesystem.model.ProjectedCoordinateSystem;
import com.revolsys.geometry.coordinatesystem.operation.CoordinatesOperationPoint;
import com.revolsys.math.Angle;

public class WebMercator extends AbstractCoordinatesProjection {

  private final double xo;

  private final double yo;

  private final double λo;

  private final double a;

  private final String name;

  public WebMercator(final ProjectedCoordinateSystem cs) {
    this.name = cs.getCoordinateSystemName();
    final double centralMeridian = cs.getDoubleParameter(NormalizedParameterNames.CENTRAL_MERIDIAN);
    this.xo = cs.getDoubleParameter(NormalizedParameterNames.FALSE_EASTING);
    this.yo = cs.getDoubleParameter(NormalizedParameterNames.FALSE_NORTHING);
    this.λo = Math.toRadians(centralMeridian);
    final Ellipsoid ellipsoid = cs.getEllipsoid();
    this.a = ellipsoid.getSemiMajorAxis();
  }

  @Override
  public void inverse(final CoordinatesOperationPoint point) {
    final double x = point.x;
    final double y = point.y;
    final double a = this.a;
    point.x = this.λo + (x - this.xo) / a;
    point.y = Angle.PI_OVER_2 - 2 * Math.atan(Math.exp((this.yo - y) / a));
  }

  @Override
  public void project(final CoordinatesOperationPoint point) {
    final double λ = point.x;
    final double φ = point.y;
    final double a = this.a;
    point.x = this.xo + a * (λ - this.λo);
    point.y = this.yo + a * Math.log(Math.tan(Angle.PI_OVER_4 + φ / 2));
  }

  @Override
  public String toString() {
    return this.name;
  }
}
