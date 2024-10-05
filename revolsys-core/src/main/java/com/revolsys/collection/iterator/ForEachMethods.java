package com.revolsys.collection.iterator;

import java.util.function.Consumer;
import java.util.stream.Stream;

public interface ForEachMethods {
  <V> void forEach(Consumer<? super V> action, ForEachHandler<V> values);

  default <V> void forEach(final Consumer<? super V> action, final Stream<V> values) {
    if (values != null) {
      forEach(action, values::forEach);
    }
  }

  @SuppressWarnings("unchecked")
  default <V> void forEach(final Consumer<? super V> action, final V... values) {
    final ForEachHandler<V> iterable = Iterables.fromValues(values);
    forEach(action, iterable);
  }
}
