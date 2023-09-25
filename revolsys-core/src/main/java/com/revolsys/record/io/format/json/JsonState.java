package com.revolsys.record.io.format.json;

public interface JsonState {
  void append(StringBuilder s);

  default int getArrayIndex() {
    return -1;
  }

  default String getLabel() {
    return null;
  }
}
