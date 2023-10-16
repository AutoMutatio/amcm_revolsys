package org.jeometry.common.data.refresh;

import java.util.function.Supplier;

public interface RefreshableValue<V> extends Refreshable {
  static <T> SupplierRefreshableValue<T> supplier(final Supplier<T> supplier) {
    return new SupplierRefreshableValue<>(supplier);
  }

  static <T> RefreshableValue<T> supplier(final Supplier<T> supplier, final long expireTime) {
    return new ExpireSupplierRefreshableValue<>(supplier, expireTime);
  }

  void clearValue();

  V getValue();

  @Override
  default void refresh() {
    refreshValue();
  }

  @Override
  default void refreshIfNeeded() {
    refreshValueIfNeeded();
  }

  V refreshValue();

  V refreshValueIfNeeded();
}
