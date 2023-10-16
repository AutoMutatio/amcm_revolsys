package org.jeometry.common.util;

import java.util.function.Consumer;

public interface Cancellable {
  static Cancellable FALSE = () -> false;

  default void cancel() {
  }

  /**
   *
   * @param iterable
   * @param action
   * @return true if cancelled, false otherwise
   */
  default <V> boolean forCancel(final Iterable<V> iterable, final Consumer<V> action) {
    for (final V value : iterable) {
      if (isCancelled()) {
        return true;
      } else {
        action.accept(value);
      }
    }
    return isCancelled();
  }

  boolean isCancelled();
}
