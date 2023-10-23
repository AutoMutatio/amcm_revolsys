package com.revolsys.geometry.coordinatesystem.model.unit;

import com.revolsys.geometry.coordinatesystem.model.Authority;

public class Grad extends AngularUnit {

  public Grad(final String name, final AngularUnit baseUnit, final double conversionFactor,
    final Authority authority, final boolean deprecated) {
    super(name, baseUnit, conversionFactor, authority, deprecated);
  }

  @Override
  public double toDegrees(final double value) {
    return value / 400.0 * 360.0;
  }

  @Override
  public double toNormal(final double value) {
    return value / 400.0 * 360.0;
  }
}
