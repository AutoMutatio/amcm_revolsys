package com.revolsys.geometry.coordinatesystem.model.unit;

import java.util.List;

import com.revolsys.geometry.coordinatesystem.model.Authority;
import com.revolsys.geometry.coordinatesystem.operation.CoordinatesOperation;
import com.revolsys.geometry.coordinatesystem.operation.CoordinatesOperationPoint;

public class Metre extends LinearUnit {

  public Metre(final String name, final LinearUnit baseUnit, final double conversionFactor,
    final Authority authority, final boolean deprecated) {
    super(name, baseUnit, conversionFactor, authority, deprecated);
  }

  @Override
  public void addFromMetresOperation(final List<CoordinatesOperation> operations) {
  }

  @Override
  public void addToMetresOperation(final List<CoordinatesOperation> operations) {
  }

  @Override
  public void fromMetres(final CoordinatesOperationPoint point) {
  }

  @Override
  public double fromMetres(final double value) {
    return value;
  }

  @Override
  public void toMetres(final CoordinatesOperationPoint point) {
  }

  @Override
  public double toMetres(final double value) {
    return value;
  }

  @Override
  public double toNormal(final double value) {
    return value;
  }

}
