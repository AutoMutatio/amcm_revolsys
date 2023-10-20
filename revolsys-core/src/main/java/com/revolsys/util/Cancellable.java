package com.revolsys.util;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.revolsys.collection.iterator.BaseIterable;
import com.revolsys.collection.iterator.Iterables;

public interface Cancellable {
  static Cancellable FALSE = () -> false;

  default void cancel() {
  }

  default <V> BaseIterable<V> cancellable(final Iterable<V> iterable) {
    return Iterables.fromIterable(iterable).cacellable(this);
  }

  default <V> BaseIterable<V> cancellable(final Iterable<V> iterable, final Predicate<V> filter) {
    return Iterables.fromIterable(iterable).cacellable(this).filter(filter);
  }

  default <V> Iterator<V> cancellable(final Iterator<V> iterator) {
    return Iterables.fromIterator(iterator).cacellable(this).iterator();
  }

  default <V> Iterator<V> cancellable(final Iterator<V> iterator, final Predicate<V> filter) {
    return Iterables.fromIterator(iterator).cacellable(this).filter(filter).iterator();
  }

  /**
   *
   * @param iterable
   * @param action
   * @return true if cancelled, false otherwise
   */
  default <V> boolean forCancel(final Iterable<V> iterable, final Consumer<V> action) {
    for (final V value : iterable) {
      if (isCancelled()) {
        return true;
      } else {
        action.accept(value);
      }
    }
    return isCancelled();
  }

  default boolean isActive() {
    return !isCancelled();
  }

  boolean isCancelled();
}
