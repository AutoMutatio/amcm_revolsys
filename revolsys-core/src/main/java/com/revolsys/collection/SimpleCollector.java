package com.revolsys.collection;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class SimpleCollector<I, O> implements Collector<I, O> {
  private final BiConsumer<O, I> collect;

  private final Supplier<O> supplier;

  public SimpleCollector(final Supplier<O> supplier, final BiConsumer<O, I> collect) {
    super();
    this.supplier = supplier;
    this.collect = collect;
  }

  @Override
  public void collect(final O collector, final I value) {
    this.collect.accept(collector, value);
  }

  @Override
  public O newResult() {
    return this.supplier.get();
  }

}
