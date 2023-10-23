package com.revolsys.record.io.format.json;

public class JsonStateObject implements JsonState {

  private String label;

  @Override
  public void append(final StringBuilder s) {
    s.append('{');
    if (this.label != null) {
      s.append(this.label);
    }
    s.append('}');
  }

  @Override
  public String getLabel() {
    return this.label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  @Override
  public String toString() {
    final StringBuilder s = new StringBuilder();
    append(s);
    return s.toString();
  }
}
