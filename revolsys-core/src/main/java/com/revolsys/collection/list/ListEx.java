package com.revolsys.collection.list;

import java.util.AbstractList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import javax.measure.Quantity;
import javax.measure.Unit;

import com.revolsys.collection.iterator.BaseIterable;
import com.revolsys.collection.json.Json;
import com.revolsys.collection.json.JsonType;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.util.Property;

import tech.units.indriya.quantity.Quantities;

public interface ListEx<T> extends List<T>, Cloneable, BaseIterable<T> {

  static class EmptyList<E> extends AbstractList<E> implements RandomAccess, ListEx<E> {

    @Override
    public void clear() {
    }

    @Override
    public ListEx<E> clone() {
      return Lists.newArray();
    }

    @Override
    public boolean contains(final Object obj) {
      return false;
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
      return c.isEmpty();
    }

    @Override
    public boolean equals(final Object o) {
      return o instanceof List && ((List<?>) o).isEmpty();
    }

    @Override
    public void forEach(final Consumer<? super E> action) {
    }

    @Override
    public E get(final int index) {
      throw new IndexOutOfBoundsException("Index: " + index);
    }

    @Override
    public int hashCode() {
      return 1;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public Iterator<E> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public ListIterator<E> listIterator() {
      return Collections.emptyListIterator();
    }

    @Override
    public boolean removeIf(final Predicate<? super E> filter) {
      return false;
    }

    @Override
    public void replaceAll(final UnaryOperator<E> operator) {
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public void sort(final Comparator<? super E> c) {
    }

    @Override
    public Spliterator<E> spliterator() {
      return Spliterators.emptySpliterator();
    }

    @Override
    public ListEx<E> subList(final int fromIndex, final int toIndex) {
      return this;
    }

    @Override
    public Object[] toArray() {
      return new Object[0];
    }

    @Override
    public <T> T[] toArray(final T[] a) {
      if (a.length > 0) {
        a[0] = null;
      }
      return a;
    }

  }

  @SuppressWarnings("rawtypes")
  static ListEx EMPTY = new EmptyList<>();

  @SuppressWarnings("unchecked")
  static <T> ListEx<T> empty() {
    return EMPTY;
  }

  @SuppressWarnings("unchecked")
  default ListEx<T> addAll(final T... values) {
    for (final T v : values) {
      addValue(v);
    }
    return this;
  }

  default ListEx<T> addAllIterable(final Iterable<? extends T> values) {
    for (final T v : values) {
      addValue(v);
    }
    return this;
  }

  default ListEx<T> addNotEmpty(final T value) {
    if (!Property.isEmpty(value)) {
      add(value);
    }
    return this;
  }

  default ListEx<T> addValue(final T value) {
    add(value);
    return this;
  }

  ListEx<T> clone();

  default Double getDouble(final int index) {
    final var value = getValue(index);
    if (value == null) {
      return null;
    } else if (value instanceof final Number number) {
      return number.doubleValue();
    } else {
      return DataTypes.DOUBLE.toObject(value);
    }
  }

  default Double getDouble(final int index, final double defaultValue) {
    final Double value = getDouble(index);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  @Override
  default T getFirst() {
    if (isEmpty()) {
      return null;
    } else {
      return get(0);
    }
  }

  default Integer getInteger(final int index) {
    final var value = getValue(index);
    if (value == null) {
      return null;
    } else if (value instanceof final Number number) {
      return number.intValue();
    } else {
      return DataTypes.INT.toObject(value);
    }
  }

  default int getInteger(final int index, final int defaultValue) {
    final Integer value = getInteger(index);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default <Q extends Quantity<Q>> Quantity<Q> getQuantity(final int index, final Unit<Q> unit) {
    final Double value = getDouble(index);
    if (value == null) {
      return null;
    } else {
      return Quantities.getQuantity(value, unit);
    }
  }

  default <Q extends Quantity<Q>> Quantity<Q> getQuantity(final int index, final Unit<Q> unit,
      final Quantity<Q> defaultValue) {
    final Quantity<Q> value = getQuantity(index, unit);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default String getString(final int index) {
    final var value = getValue(index);
    if (value == null) {
      return null;
    } else if (value instanceof final CharSequence string) {
      return string.toString();
    } else {
      return DataTypes.STRING.toObject(value);
    }
  }

  default String getString(final int index, final String defaultValue) {
    final String value = getString(index);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  @SuppressWarnings("unchecked")
  default <V> V getValue(final int index) {
    return (V) get(index);
  }

  default <V extends Object> V getValue(final int index, final DataType dataType) {
    final Object value = get(index);
    return dataType.toObject(value);
  }

  default <V extends Object> V getValue(final int index, final V defaultValue) {
    if (index < size()) {
      final V value = getValue(index);
      if (value != null) {
        return value;
      }
    }
    return defaultValue;
  }

  @Override
  default Stream<T> parallelStream() {
    return List.super.parallelStream();
  }

  default boolean removeEmptyProperties() {
    boolean removed = false;
    for (final Iterator<T> iterator = iterator(); iterator.hasNext();) {
      final Object value = iterator.next();
      if (value instanceof final JsonType jsonValue) {
        jsonValue.removeEmptyProperties();
        if (jsonValue.isEmpty()) {
          iterator.remove();
          removed = true;
        }
      } else if (value instanceof final ListEx list) {
        list.removeEmptyProperties();
        if (list.isEmpty()) {
          iterator.remove();
          removed = true;
        }
      } else if (!Property.hasValue(value)) {
        iterator.remove();
        removed = true;
      }
    }
    return removed;
  }

  @Override
  default T removeLast() {
    if (size() > 0) {
      return remove(size() - 1);
    } else {
      return null;
    }
  }

  default ListEx<T> sortThis(final Comparator<? super T> converter) {
    sort(converter);
    return this;
  }

  @Override
  default Stream<T> stream() {
    return List.super.stream();
  }

  default ListEx<T> subList(final int fromIndex) {
    if (fromIndex < 0) {
      throw new IllegalArgumentException("Index must be >=0");
    } else if (fromIndex == 0) {
      return this;
    } else if (fromIndex < size()) {
      return subList(fromIndex, size());
    } else {
      return empty();
    }
  }

  @Override
  ListEx<T> subList(final int fromIndex, final int toIndex);

  default int[] toIntArray() {
    final int[] array = new int[size()];
    for (int i = 0; i < size(); i++) {
      final int width = getInteger(i);
      array[i] = width;
    }
    return array;
  }

  default String toJsonString(final boolean indent) {
    return Json.toString(this, indent);
  }
}
