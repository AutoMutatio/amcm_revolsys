package com.revolsys.parallel.channel;

public abstract class AbstractMultiInputSelector {
  abstract void closeInput(SelectableInput input);

  abstract void schedule(SelectableInput input);
}
