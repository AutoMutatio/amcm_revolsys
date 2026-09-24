package com.revolsys.util.concurrent;

import java.util.function.Consumer;

import com.revolsys.collection.iterator.ForEachHandler;
import com.revolsys.collection.iterator.ForEachMethods;
import com.revolsys.collection.iterator.RunnableMethods;

public class Sequential implements ForEachMethods<Sequential>, RunnableMethods<Sequential> {

  @Override
  public <V> Sequential forEach(final ForEachHandler<V> values, final Consumer<? super V> action) {
    values.forEach(value -> action.accept(value));
    return this;
  }

  @Override
  public Sequential run(final Runnable runnable) {
    runnable.run();
    return this;
  }

}
