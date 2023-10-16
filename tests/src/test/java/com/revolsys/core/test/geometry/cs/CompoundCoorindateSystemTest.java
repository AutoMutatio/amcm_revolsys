package com.revolsys.core.test.geometry.cs;

import org.junit.Assert;
import org.junit.Test;

import com.revolsys.geometry.coordinatesystem.model.CompoundCoordinateSystem;
import com.revolsys.geometry.coordinatesystem.model.systems.EpsgCoordinateSystems;
import com.revolsys.geometry.coordinatesystem.model.systems.EpsgId;

public class CompoundCoorindateSystemTest {

  @Test
  public void testStandard() {
    final CompoundCoordinateSystem compoundCoordinateSystem = EpsgCoordinateSystems
      .getCompound(EpsgId.NAD83, 5703);
    Assert.assertEquals(5498, compoundCoordinateSystem.getCoordinateSystemId());
  }
}
