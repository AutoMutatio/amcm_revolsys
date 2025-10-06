package com.revolsys.collection.iterator;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface ForEachMethods<SELF> {
  @SuppressWarnings("unchecked")
  default <V> SELF forEach(final Consumer<? super V> action, final V... values) {
    final ForEachHandler<V> iterable = Iterables.fromValues(values);
    return forEach(iterable, action);
  }

  <V> SELF forEach(ForEachHandler<V> values, Consumer<? super V> action);

  @SuppressWarnings("unchecked")
  default <V> SELF forEach(final Stream<V> values, final Consumer<? super V> action) {
    if (values != null) {
      final ForEachHandler<V> handler = values::forEach;
      return forEach(handler, action);
    }
    return (SELF)this;
  }

  @SuppressWarnings("unchecked")
  default <V> long forEachCount(final Consumer<? super V> action, final V... values) {
    final ForEachHandler<V> iterable = Iterables.fromValues(values);
    return forEachCount(iterable, action);
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
}
