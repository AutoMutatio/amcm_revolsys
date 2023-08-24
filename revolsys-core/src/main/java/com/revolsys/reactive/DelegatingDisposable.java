package com.revolsys.reactive;

import reactor.core.Disposable;

public class DelegatingDisposable implements Disposable {

  private boolean disposed;

  private Disposable disposable;

  @Override
  public void dispose() {
    final Disposable disposable = this.disposable;
    this.disposable = null;
    if (disposable != null) {
      disposable.dispose();
    }
    this.disposed = true;
  }

  @Override
  public boolean isDisposed() {
    final Disposable disposable = this.disposable;
    if (disposable == null) {
      return this.disposed;
    } else {
      return disposable.isDisposed();
    }
  }

  public void setDisposable(final Disposable disposable) {
    this.disposable = disposable;
    if (disposable == null) {
      this.disposed = true;
    } else {
      this.disposed = disposable.isDisposed();
    }
  }

  @Override
  public String toString() {
    final Disposable disposable = this.disposable;
    if (disposable == null) {
      return "null";
    } else {
      return disposable.toString();
    }
  }
}
