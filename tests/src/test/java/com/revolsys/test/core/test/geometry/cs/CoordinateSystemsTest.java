package com.revolsys.test.core.test.geometry.cs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.revolsys.test.core.test.geometry.cs.projection.CoordinatesProjectionTest;

@RunWith(Suite.class)
@SuiteClasses({
  EllipsoidTest.class, //
  CompoundCoorindateSystemTest.class, //
  CoordinatesProjectionTest.class
})
public class CoordinateSystemsTest {

}
