package org.jeometry.common.collection.map;

import java.util.Map;
import java.util.function.Supplier;

import org.jeometry.common.data.refresh.Refreshable;

public interface RefreshableMap<K, V> extends Refreshable, Map<K, V> {
  static <K1, T1> SupplierRefreshableMap<K1, T1> supplier(final Supplier<Map<K1, T1>> supplier) {
    return new SupplierRefreshableMap<>(supplier, true);
  }

  static <K1, T1> SupplierRefreshableMap<K1, T1> supplier(final Supplier<Map<K1, T1>> supplier,
    final boolean editable) {
    return new SupplierRefreshableMap<>(supplier, editable);
  }

  void clearValue();

}
