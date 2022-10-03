package com.revolsys.parallel.channel;

public interface SelectableInput {
  int AVAILABLE = 1;

  int CLOSED = 2;

  int EMPTY = 0;

  boolean disable(AbstractMultiInputSelector selector);

  boolean enable(AbstractMultiInputSelector selector);

  boolean isClosed();
}
