/*
 * Copyright 2004-2007 Revolution Systems Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.revolsys.collection.iterator;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.revolsys.collection.Collector;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.value.Single;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.Cancellable;
import com.revolsys.util.ExitLoopException;
import com.revolsys.util.StringBuilders;

/**
 * <p>
 * The Reader interface defines methods for reading objects of type T. Objects
 * can either by read as a {@link List} or using an {@link Iterator} or visited
 * using a {@link Consumer}.
 * </p>
 * <p>
 * The simplest and most effecient way to loop through all objects in the reader
 * is to use the following loop.
 * </p>
 *
 * <pre>
 * Reader&lt;T&gt; reader = ...
 * for (T object : reader) {
 *   // Do something with the object.
 * }
 * </pre>
 *
 * @author Paul Austin
 * @param <T> The type of the item to read.
 */
public interface BaseIterable<T> extends Iterable<T>, ForEachHandler<T> {

  default BaseIterable<T> cancellable(final Cancellable cancellable) {
    if (cancellable == null) {
      return this;
    } else {
      return filter(v -> cancellable.isActive());
    }
  }

  default BaseCloseable closeable() {
    return BaseCloseable.of(this);
  }

  default <O> O collect(final Collector<T, O> collector) {
    final var result = collector.newResult();
    forEach(value -> collector.collect(result, value));
    return result;
  }

  default <O> O collect(final Supplier<O> resultSupplier, final BiConsumer<O, T> action) {
    final var result = resultSupplier.get();
    forEach(value -> action.accept(result, value));
    return result;
  }

  default BaseIterable<T> filter(final Predicate<? super T> filter) {
    if (filter == null) {
      return this;
    } else {
      return () -> new FilterIterator<>(filter, iterator());
    }
  }

  default Single<T> first() {
    return Single.ofNullable(getFirst());
  }

  default Single<T> first(final Predicate<T> filter) {
    for (final T value : this) {
      if (value != null && filter.test(value)) {
        return Single.of(value);
      }
    }
    return Single.empty();
  }

  @Override
  default void forEach(final Consumer<? super T> action) {
    try (
      var c = closeable()) {
      final Iterator<T> iterator = iterator();
      if (iterator != null) {
        try (
          var ic = BaseCloseable.of(iterator)) {
          while (iterator.hasNext()) {
            final T item = iterator.next();
            if (item != null) {
              action.accept(item);
            }
          }
        } catch (final ExitLoopException e) {
        }
      }
    }
  }

  default int forEachCount(final Consumer<? super T> action) {
    int i = 0;
    try (
      var c = closeable()) {
      final Iterator<T> iterator = iterator();
      if (iterator != null) {
        try (
          var ic = BaseCloseable.of(iterator)) {
          while (iterator.hasNext()) {
            final T item = iterator.next();
            if (item != null) {
              action.accept(item);
              i++;
            }
          }
        } catch (final ExitLoopException e) {
        }
      }
    }
    return i;
  }

  default void forEachIndex(final BiConsumer<Integer, ? super T> action) {
    int i = 0;
    try (
      var c = closeable()) {
      final Iterator<T> iterator = iterator();
      if (iterator != null) {
        try (
          var ic = BaseCloseable.of(iterator)) {
          while (iterator.hasNext()) {
            final T item = iterator.next();
            if (item != null) {
              action.accept(i++, item);
            }
          }
        } catch (final ExitLoopException e) {
        }
      }
    }
  }

  default T getFirst() {
    try (
      var c = closeable()) {
      final Iterator<T> iterator = iterator();
      if (iterator.hasNext()) {
        return iterator.next();
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  default <V extends T> Iterable<V> i() {
    return (Iterable<V>)this;
  }

  @SuppressWarnings("unchecked")
  default <V> BaseIterable<V> instanceOf(final Class<? super V> clazz) {
    return filter(v -> {
      if (clazz.isInstance(v)) {
        return true;
      } else {
        return false;
      }
    }).map(v -> (V)v);
  }

  default String join(final String separator) {
    final StringBuilder string = new StringBuilder();
    StringBuilders.append(string, this, separator);
    return string.toString();
  }

  default <V> BaseIterable<V> map(final Function<? super T, V> converter) {
    if (converter == null) {
      throw new IllegalArgumentException("Converter must not be null");
    } else {
      return () -> new MapIterator<>(iterator(), converter);
    }
  }

  default Stream<T> parallelStream() {
    return StreamSupport.stream(spliterator(), true);
  }

  default Stream<T> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  /**
   * Read all items and return a List containing the items.
   *
   * @return The list of items.
   */
  default ListEx<T> toList() {
    return collect(Lists.factoryArray(), ListEx::add);
  }

  default BaseIterable<T> walkTree(final Function<T, Iterable<T>> treeWalk) {
    if (treeWalk == null) {
      throw new IllegalArgumentException("Tree walk function must not be null");
    } else {
      return () -> new TreeIterator<>(iterator(), treeWalk);
    }
  }

  default <C> BaseIterable<C> walkTreeChildren(final Function<T, Iterable<C>> treeWalk) {
    if (treeWalk == null) {
      throw new IllegalArgumentException("Tree walk function must not be null");
    } else {
      return () -> new ChildrenTreeIterator<>(iterator(), treeWalk);
    }
  }
}
