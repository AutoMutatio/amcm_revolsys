package com.revolsys.collection.iterator;

import java.util.stream.Stream;

public interface RunableMethods {
  <V> void run(ForEachHandler<Runnable> values);

  default void run(final Runnable... values) {
    final ForEachHandler<Runnable> iterable = Iterables.fromValues(values);
    run(iterable);
  }

  default <V> void run(final Stream<Runnable> values) {
    if (values != null) {
      run(values::forEach);
    }
  }
}
