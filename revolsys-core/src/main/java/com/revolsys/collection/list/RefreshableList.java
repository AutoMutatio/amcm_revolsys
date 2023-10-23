package com.revolsys.collection.list;

import java.util.List;
import java.util.function.Supplier;

import com.revolsys.collection.collection.RefreshableCollection;

public interface RefreshableList<V> extends RefreshableCollection<V>, List<V> {
  static <V1> SupplierRefreshableList<V1> supplier(final Supplier<List<V1>> supplier) {
    return new SupplierRefreshableList<>(supplier, true);
  }

  static <V1> SupplierRefreshableList<V1> supplier(final Supplier<List<V1>> supplier,
    final boolean editable) {
    return new SupplierRefreshableList<>(supplier, editable);
  }

}
