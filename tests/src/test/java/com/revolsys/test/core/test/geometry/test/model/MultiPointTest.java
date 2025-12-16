package com.revolsys.test.core.test.geometry.test.model;

import org.junit.Test;

import com.revolsys.testapi.GeometryAssert;

public class MultiPointTest {

  @Test
  public void testFromFile() {
    GeometryAssert.doTestGeometry(getClass(), "MultiPoint.csv");
  }
}
