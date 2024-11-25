package com.revolsys.collection.iterator;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface ForEachMethods {
  <V> void forEach(ForEachHandler<V> values, Consumer<? super V> action);

  default <V> void forEach(final Stream<V> values, final Consumer<? super V> action) {
    if (values != null) {
      final ForEachHandler<V> handler = values::forEach;
      forEach(handler, action);
    }
  }

  @SuppressWarnings("unchecked")
  default <V> void forEach(final Consumer<? super V> action, final V... values) {
    final ForEachHandler<V> iterable = Iterables.fromValues(values);
    forEach(iterable, action);
  }

  default <V> long forEachCount(final ForEachHandler<V> values, final Consumer<? super V> action) {
    final AtomicLong count = new AtomicLong();
    if (values != null) {
      forEach(values, v -> {
        count.incrementAndGet();
        action.accept(v);
      });
    }
    return count.longValue();
  }

  default <V> long forEachCount(final Stream<V> values, final Consumer<? super V> action) {
    if (values != null) {
      final ForEachHandler<V> handler = values::forEach;
      return forEachCount(handler, action);
    }
    return 0;
  }

  @SuppressWarnings("unchecked")
  default <V> long forEachCount(final Consumer<? super V> action, final V... values) {
    final ForEachHandler<V> iterable = Iterables.fromValues(values);
    return forEachCount(iterable, action);
  }
}
