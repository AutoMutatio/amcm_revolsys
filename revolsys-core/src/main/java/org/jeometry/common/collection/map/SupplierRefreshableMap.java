package org.jeometry.common.collection.map;

import java.util.Map;
import java.util.function.Supplier;

public class SupplierRefreshableMap<K, V> extends AbstractRefreshableMap<K, V> {

  private final Supplier<Map<K, V>> supplier;

  SupplierRefreshableMap(final Supplier<Map<K, V>> supplier, final boolean editable) {
    super(editable);
    this.supplier = supplier;
  }

  @Override
  protected Map<K, V> loadValue() {
    return this.supplier.get();
  }

}
