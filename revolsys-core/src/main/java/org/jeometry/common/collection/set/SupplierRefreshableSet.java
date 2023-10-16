package org.jeometry.common.collection.set;

import java.util.Set;
import java.util.function.Supplier;

public class SupplierRefreshableSet<V> extends AbstractRefreshableSet<V> {

  private final Supplier<Set<V>> supplier;

  SupplierRefreshableSet(final Supplier<Set<V>> supplier, final boolean editable) {
    super(editable);
    this.supplier = supplier;
  }

  @Override
  protected Set<V> loadValue() {
    return this.supplier.get();
  }

}
