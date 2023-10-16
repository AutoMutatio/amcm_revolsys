package org.jeometry.common.data.refresh;

import java.util.function.Supplier;

public class SupplierRefreshableValue<V> extends AbstractRefreshableValue<V> {

  private final Supplier<V> supplier;

  SupplierRefreshableValue(final Supplier<V> supplier) {
    this.supplier = supplier;
  }

  @Override
  protected V loadValue() {
    return this.supplier.get();
  }

}
