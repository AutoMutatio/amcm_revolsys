package org.jeometry.common.collection.list;

import java.util.List;
import java.util.function.Supplier;

public class SupplierRefreshableList<V> extends AbstractRefreshableList<V> {

  private final Supplier<List<V>> supplier;

  SupplierRefreshableList(final Supplier<List<V>> supplier, final boolean editable) {
    super(editable);
    this.supplier = supplier;
  }

  @Override
  protected List<V> loadValue() {
    return this.supplier.get();
  }

}
