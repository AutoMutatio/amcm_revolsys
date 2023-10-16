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
package com.revolsys.io;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.jeometry.common.collection.iterator.BaseIterable;
import org.jeometry.common.util.ExitLoopException;

import com.revolsys.collection.iterator.FilterIterator;
import com.revolsys.properties.ObjectWithProperties;
import com.revolsys.util.Cancellable;

import reactor.core.publisher.Flux;

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
public interface Reader<T>
  extends Iterable<T>, ObjectWithProperties, BaseCloseable, Cancellable, BaseIterable<T> {
  Reader<?> EMPTY = wrap(Collections.emptyIterator());

  @SuppressWarnings("unchecked")
  static <V> Reader<V> empty() {
    return (Reader<V>)EMPTY;
  }

  static <V> Reader<V> wrap(final Iterator<V> iterator) {
    return new IteratorReader<>(iterator);
  }

  /**
   * Close the reader and all resources associated with it.
   */
  @Override
  default void close() {
  }

  @Override
  default BaseIterable<T> filter(final Predicate<? super T> filter) {
    final Iterator<T> iterator = iterator();
    return new FilterIterator<>(filter, iterator);
  }

  default <O> BaseIterable<O> filter(final Predicate<T> filter, final Function<T, O> converter) {
    return filter(filter).map(converter);
  }

  default Flux<T> flux() {
    return Flux.fromIterable(this);
  }

  default void forEach(final BiConsumer<Cancellable, ? super T> action) {
    forEach(this, action);
  }

  default void forEach(final Cancellable cancellable,
    final BiConsumer<Cancellable, ? super T> action) {
    try (
      Reader<?> reader = this) {
      if (iterator() != null) {
        try {
          for (final T item : this) {
            if (cancellable.isCancelled()) {
              return;
            } else {
              action.accept(cancellable, item);
            }
          }
        } catch (final ExitLoopException e) {
        }
      }
    }
  }

  default void forEach(final Cancellable cancellable, final Consumer<? super T> action) {
    try (
      Reader<?> reader = this) {
      if (iterator() != null) {
        try {
          for (final T item : this) {
            if (cancellable.isCancelled()) {
              return;
            } else {
              action.accept(item);
            }
          }
        } catch (final ExitLoopException e) {
        }
      }
    }
  }

  /**
   * Visit each item returned from the reader until all items have been visited
   * or the visit method returns false.
   *
   * @param visitor The visitor.
   */
  @Override
  default void forEach(final Consumer<? super T> action) {
    try (
      Reader<?> reader = this) {
      if (iterator() != null) {
        try {
          for (final T item : this) {
            action.accept(item);
          }
        } catch (final ExitLoopException e) {
        }
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  default T getFirst() {
    try (
      Reader<?> reader = this) {
      final Iterator<T> iterator = iterator();
      if (iterator.hasNext()) {
        return iterator.next();
      }
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  default <V extends T> Iterable<V> i() {
    return (Iterable<V>)this;
  }

  /**
   * Open the reader so that it is ready to be read from.
   */
  default void open() {
  }

  @Override
  default Stream<T> parallelStream() {
    return StreamSupport.stream(spliterator(), true);
  }

  @Override
  default void skipAll() {
    for (final Iterator<T> iterator = iterator(); iterator.hasNext();) {
    }
  }

  @Override
  default Stream<T> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

}
