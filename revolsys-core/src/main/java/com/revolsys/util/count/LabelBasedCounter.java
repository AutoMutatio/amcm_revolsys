package com.revolsys.util.count;

public interface LabelBasedCounter {
  void addCount(CharSequence label);

  Long getCount(CharSequence label);
}
