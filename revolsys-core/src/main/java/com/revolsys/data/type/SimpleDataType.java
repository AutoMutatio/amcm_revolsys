package com.revolsys.data.type;

public class SimpleDataType extends AbstractDataType {
  public SimpleDataType(final String name, final Class<?> javaClass) {
    super(name, javaClass, true);
  }
}
