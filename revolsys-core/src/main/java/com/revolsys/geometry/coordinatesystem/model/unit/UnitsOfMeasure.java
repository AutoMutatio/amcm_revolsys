package com.revolsys.geometry.coordinatesystem.model.unit;

import com.revolsys.geometry.coordinatesystem.model.systems.EpsgCoordinateSystems;

public interface UnitsOfMeasure {
  static Degree DEGREE = EpsgCoordinateSystems.getUnit(9102);

  static Metre METRE = EpsgCoordinateSystems.getUnit(9001);
}
