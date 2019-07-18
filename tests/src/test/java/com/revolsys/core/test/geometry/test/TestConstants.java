package com.revolsys.core.test.geometry.test;

import org.jeometry.coordinatesystem.model.systems.EpsgId;

import com.revolsys.geometry.model.GeometryFactory;

public interface TestConstants {

  GeometryFactory UTM10_GF_2_FLOATING = GeometryFactory.floating2d(EpsgId.nad83Utm(10));

  int UTM10_X_START = 500000;

  int UTM10_Y_START = 6000000;
}
