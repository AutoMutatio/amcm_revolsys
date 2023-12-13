package com.revolsys.record.io.format.json;

public class JsonStateArray implements JsonState {

  private int index = -1;

  @Override
  public void append(final StringBuilder s) {
    s.append('[');
    if (this.index != -1) {
      s.append(this.index);
    }
    s.append(']');
  }

  @Override
  public int getArrayIndex() {
    return this.index;
  }

  public void increment() {
    this.index++;
  }

  @Override
  public String toString() {
    final StringBuilder s = new StringBuilder();
    append(s);
    return s.toString();
  }
}
