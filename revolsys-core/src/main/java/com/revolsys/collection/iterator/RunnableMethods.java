package com.revolsys.collection.iterator;

import java.util.stream.Stream;

public interface RunnableMethods<T extends RunnableMethods<T>> {
  @SuppressWarnings("unchecked")
  default T run(final ForEachHandler<Runnable> values) {
    values.forEach(this::run);
    return (T)this;
  }

  T run(Runnable runnable);

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
