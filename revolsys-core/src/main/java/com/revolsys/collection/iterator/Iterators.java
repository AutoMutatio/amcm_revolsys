package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.revolsys.io.Reader;

public interface Iterators {
  static <V> Reader<V> filter(final Iterable<V> iterable, final Predicate<? super V> filter) {
    if (filter == null) {
      return (Reader<V>)iterable;
    } else {
      final Iterator<V> iterator = iterable.iterator();
      return new FilterIterator<>(filter, iterator);
    }
  }

  static <V> Iterator<V> filter(final Iterator<V> iterator, final Predicate<? super V> filter) {
    if (filter == null) {
      return iterator;
    } else {
      return new FilterIterator<>(filter, iterator);
    }
  }

  static <I, O> MapIterator<I, O> map(final Iterator<I> iterator, final Function<I, O> converter) {
    if (converter == null) {
      throw new IllegalArgumentException("Converter cannot be null");
    } else {
      return new MapIterator<>(iterator, converter);
    }
  }

  static <V> V next(final Iterator<V> iterator) {
    if (iterator == null || !iterator.hasNext()) {
      return null;
    } else {
      return iterator.next();
    }
  }

  static <V> Iterator<V> wrap(final Supplier<V> supplier) {
    return new SupplierIterator<>(supplier);
  }
}
