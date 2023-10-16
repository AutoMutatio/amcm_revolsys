package org.jeometry.common.data.refresh;

public interface Refreshable {

  default String getLabel() {
    return toString();
  }

  void refresh();

  void refreshIfNeeded();

}
