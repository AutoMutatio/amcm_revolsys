package com.revolsys.collection;

public interface Collector<I, O> {

  void collect(O collector, I value);

  O newResult();
}
