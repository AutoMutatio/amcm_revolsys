package com.revolsys.util;

public interface CancellableProxy extends Cancellable {
  @Override
  default void cancel() {
    final Cancellable cancellable = getCancellable();
    if (cancellable != null) {
      cancellable.cancel();
    }
  }

  Cancellable getCancellable();

  @Override
  default boolean isCancelled() {
    final Cancellable cancellable = getCancellable();
    if (cancellable == null) {
      return false;
    } else {
      return cancellable.isCancelled();
    }
  }
}
