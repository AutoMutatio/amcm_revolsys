package com.revolsys.collection.iterator;

import java.util.stream.Stream;

public interface RunnableMethods<T extends RunnableMethods<T>> {
  T run(ForEachHandler<Runnable> values);

  @SuppressWarnings("unchecked")
  default T run(final Runnable... values) {
    final ForEachHandler<Runnable> iterable = Iterables.fromValues(values);
    run(iterable);
    return (T)this;
  }

  @SuppressWarnings("unchecked")
  default T run(final Stream<Runnable> values) {
    if (values != null) {
      run(values::forEach);
    }
    return (T)this;
  }
}
