package com.revolsys.collection.iterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Supplier;

public interface Iterables {
  static BaseIterable<?> EMPTY = Collections::emptyIterator;

  @SuppressWarnings({
    "unchecked", "rawtypes"
  })
  static <V> BaseIterable<V> empty() {
    return (BaseIterable)EMPTY;
  }

  /**
   * Create an iterable that returns the {@link Iterable#iterator()}.
   *
   * @param <V> The type of data returned.
   * @param iterable The iterator.
   * @return The iterable.
   */
  static <V> BaseIterable<V> fromIterable(final Iterable<V> iterable) {
    if (iterable instanceof final BaseIterable<V> base) {
      return base;
    } else {
      return () -> iterable.iterator();
    }
  }

  /**
   * Create an iterable that returns the iterator.
   *
   * @param <V> The type of data returned.
   * @param iterator The iterator.
   * @return The iterable.
   */
  static <V> BaseIterable<V> fromIterator(final Iterator<V> iterator) {
    return () -> iterator;
  }

  /**
   * Create an iterable for the supplier. Each call it {@link #iterator()} will call the supplier.
   *
   * @param <V> The type of data returned.
   * @param supplier The iterator supplier.
   * @return The iterable.
   */
  static <V> BaseIterable<V> fromSupplier(final Supplier<Iterator<V>> supplier) {
    return supplier::get;
  }

  /**
   * Create an iterable that returns the single value.
   *
   * @param <V> The type of data returned.
   * @param value The value.
   * @return The iterable.
   */
  static <V> BaseIterable<V> fromValue(final V value) {
    return () -> new SingleIterator<>(value);
  }

  /**
   * Create an iterable that returns the single result from the supplier.
   * The supplier will be called on the each {@link #iterator()} call.
   *
   * @param <V> The type of data returned.
   * @param supplier The iterator supplier.
   * @return The iterable.
   */
  static <V> BaseIterable<V> fromValueSupplier(final Supplier<V> supplier) {
    return () -> new SingleIterator<>(supplier.get());
  }

  static <V> BaseIterable<V> multiple(final Iterable<Iterable<V>> iterables) {
    final var iterator = new MultipleIterator<>(iterables);
    return fromIterator(iterator);
  }

  static <V> V next(final Iterator<V> iterator) {
    if (iterator == null || !iterator.hasNext()) {
      return null;
    } else {
      return iterator.next();
    }
  }

  static <V> BaseIterable<V> paging(final Supplier<Iterable<V>> supplier) {
    final var iterator = new PagingIterator<>(supplier);
    return fromIterator(iterator);
  }
}
