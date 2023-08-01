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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import javax.measure.Quantity;
import javax.measure.Unit;

import org.jeometry.common.data.type.DataType;
import org.jeometry.common.data.type.DataTypes;

import com.revolsys.util.Property;
import com.revolsys.util.StringBuilders;

import tech.units.indriya.quantity.Quantities;

public interface ListEx<V> extends List<V>, Cloneable {
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
      return o instanceof List && ((List<?>)o).isEmpty();
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
  default ListEx<V> addAll(final V... values) {
    for (final V v : values) {
      addValue(v);
    }
    return this;
  }

  default ListEx<V> addNotEmpty(final V value) {
    if (!Property.isEmpty(value)) {
      add(value);
    }
    return this;
  }

  default ListEx<V> addValue(final V value) {
    add(value);
    return this;
  }

  ListEx<V> clone();

  default ListEx<V> filter(final Predicate<? super V> filter) {
    final ListEx<V> newList = new ArrayListEx<>();
    for (final V value : this) {
      if (filter.test(value)) {
        newList.add(value);
      }
    }
    return newList;
  }

  default Double getDouble(final int index) {
    return getValue(index, DataTypes.DOUBLE);
  }

  default Double getDouble(final int index, final double defaultValue) {
    final Double value = getDouble(index);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default Integer getInteger(final int index) {
    return getValue(index, DataTypes.INT);
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
    return getValue(index, DataTypes.STRING);
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
  default <T> T getValue(final int index) {
    return (T)get(index);
  }

  default <T extends Object> T getValue(final int index, final DataType dataType) {
    final Object value = get(index);
    return dataType.toObject(value);
  }

  default <T extends Object> T getValue(final int index, final T defaultValue) {
    final T value = getValue(index);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default String join(final String separator) {
    final StringBuilder string = new StringBuilder();
    StringBuilders.append(string, this, separator);
    return string.toString();
  }

  default <OUT> ListEx<OUT> map(final Function<? super V, OUT> converter) {
    final ListEx<OUT> newList = new ArrayListEx<>();
    for (final V value : this) {
      final OUT newValue = converter.apply(value);
      newList.add(newValue);
    }
    return newList;
  }

  default V removeLast() {
    if (size() > 0) {
      return remove(size() - 1);
    } else {
      return null;
    }
  }

  default ListEx<V> sortThis(final Comparator<? super V> converter) {
    sort(converter);
    return this;
  }
}
