package com.revolsys.geometry.coordinatesystem.model;

import java.security.MessageDigest;

public interface ParameterValue {

  <V> V getOriginalValue();

  <V> V getValue();

  boolean isSame(ParameterValue parameterValue);

  void updateDigest(MessageDigest digest);
}
