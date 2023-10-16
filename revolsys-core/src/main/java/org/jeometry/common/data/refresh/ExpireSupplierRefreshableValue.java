package org.jeometry.common.data.refresh;

import java.util.function.Supplier;

public class ExpireSupplierRefreshableValue<V> extends SupplierRefreshableValue<V> {

  private final long expireTime;

  private long loadTime = 0;

  public ExpireSupplierRefreshableValue(final Supplier<V> supplier, final long expireTime) {
    super(supplier);
    this.expireTime = expireTime;
  }

  @Override
  public V getValue() {
    if (!hasValue() || System.currentTimeMillis() > this.loadTime + this.expireTime) {
      clearValue();
    }
    return super.getValue();
  }

  @Override
  protected V loadValue() {
    try {
      return super.loadValue();
    } finally {
      this.loadTime = System.currentTimeMillis();
    }
  }
}
