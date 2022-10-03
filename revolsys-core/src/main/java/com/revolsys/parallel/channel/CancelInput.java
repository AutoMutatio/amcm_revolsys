package com.revolsys.parallel.channel;

public class CancelInput implements SelectableInput {
  private boolean cancelled;

  private AbstractMultiInputSelector selector;

  public void cancel() {
    this.cancelled = true;
    final AbstractMultiInputSelector selector = this.selector;
    if (selector != null) {
      selector.schedule(this);
    }
  }

  @Override
  public boolean disable(AbstractMultiInputSelector selector) {
    this.selector = null;
    return this.cancelled;
  }

  @Override
  public boolean enable(final AbstractMultiInputSelector selector) {
    this.selector = selector;
    return this.cancelled;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

}
