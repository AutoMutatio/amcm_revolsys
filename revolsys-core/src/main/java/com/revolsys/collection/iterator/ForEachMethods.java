package com.revolsys.collection.iterator;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface ForEachMethods {
  <V> void forEach(Consumer<? super V> action, ForEachHandler<V> values);

  default <V> void forEach(final Consumer<? super V> action, final Stream<V> values) {
    if (values != null) {
      final ForEachHandler<V> handler = values::forEach;
      forEach(action, handler);
    }
  }

  @SuppressWarnings("unchecked")
  default <V> void forEach(final Consumer<? super V> action, final V... values) {
    final ForEachHandler<V> iterable = Iterables.fromValues(values);
    forEach(action, iterable);
  }

  default <V> long forEachCount(final Consumer<? super V> action, final ForEachHandler<V> values) {
    final AtomicLong count = new AtomicLong();
    if (values != null) {
      forEach(v -> {
        count.incrementAndGet();
        action.accept(v);
      }, values);
    }
    return count.longValue();
  }

  default <V> long forEachCount(final Consumer<? super V> action, final Stream<V> values) {
    if (values != null) {
      final ForEachHandler<V> handler = values::forEach;
      return forEachCount(action, handler);
    }
    return 0;
  }

  @SuppressWarnings("unchecked")
  default <V> long forEachCount(final Consumer<? super V> action, final V... values) {
    final ForEachHandler<V> iterable = Iterables.fromValues(values);
    return forEachCount(action, iterable);
  }
}
