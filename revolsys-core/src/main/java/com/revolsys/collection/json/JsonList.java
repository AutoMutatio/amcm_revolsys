package com.revolsys.collection.json;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.revolsys.collection.iterator.Reader;
import com.revolsys.collection.list.ListEx;
import com.revolsys.data.type.DataType;
import com.revolsys.exception.Exceptions;
import com.revolsys.util.Property;

public interface JsonList extends ListEx<Object>, JsonType {

  JsonList EMPTY = new JsonList() {

    @Override
    public void add(final int index, final Object element) {
    }

    @Override
    public boolean add(final Object e) {
      return false;
    }

    @Override
    public boolean addAll(final Collection<? extends Object> c) {
      return false;
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends Object> c) {
      return false;
    }

    @Override
    public void clear() {
    }

    @Override
    public JsonList clone() {
      return JsonList.array();
    }

    @Override
    public boolean contains(final Object o) {
      return false;
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
      return false;
    }

    @Override
    public boolean equals(final Object object,
        final Collection<? extends CharSequence> excludeFieldNames) {
      return false;
    }

    @Override
    public Object get(final int index) {
      return null;
    }

    @Override
    public int indexOf(final Object o) {
      return -1;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public Iterator<Object> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public int lastIndexOf(final Object o) {
      return -1;
    }

    @Override
    public ListIterator<Object> listIterator() {
      return Collections.emptyListIterator();
    }

    @Override
    public ListIterator<Object> listIterator(final int index) {
      return Collections.emptyListIterator();
    }

    @Override
    public Object remove(final int index) {
      return null;
    }

    @Override
    public boolean remove(final Object o) {
      return false;
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
      return false;
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
      return false;
    }

    @Override
    public Object set(final int index, final Object element) {
      return null;
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public ListEx<Object> subList(final int fromIndex, final int toIndex) {
      return this;
    }

    @Override
    public Object[] toArray() {
      return new Object[0];
    }

    @Override
    public <T> T[] toArray(final T[] a) {
      return a;
    }

  };

  static Supplier<JsonList> ARRAY_SUPPLIER = JsonListArray::new;

  static JsonList array() {
    return new JsonListArray();
  }

  static JsonList array(final Collection<?> collection) {
    return new JsonListArray(collection);
  }

  static JsonList array(final Object value) {
    return new JsonListArray(value);
  }

  static JsonList array(final Object... values) {
    return new JsonListArray(values);
  }

  default boolean addIfNotContains(final Object value) {
    final boolean notContains = !contains(value);
    if (notContains) {
      add(value);
    }
    return notContains;
  }

  default boolean addIfNotContains(final Object value,
      final Collection<? extends CharSequence> excludeFieldNames) {
    final boolean notContains = !contains(value, excludeFieldNames);
    if (notContains) {
      add(value);
    }
    return notContains;
  }

  default JsonList addValuesClone(final Collection<?> values) {
    for (Object value : values) {
      if (value != null) {
        value = JsonType.toJsonClone(value);
      }
      add(value);
    }
    return this;
  }

  @Override
  default Appendable appendJson(final Appendable appendable) {
    try {
      appendable.append('[');
      boolean first = true;
      for (final Object value : this) {
        if (first) {
          first = false;
        } else {
          appendable.append(',');
        }
        JsonWriterUtil.appendValue(appendable, value);
      }
      appendable.append(']');
      return appendable;
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @Override
  default JsonList asJson() {
    return (JsonList) JsonType.super.asJson();
  }

  @Override
  JsonList clone();

  default boolean contains(final Object value,
      final Collection<? extends CharSequence> excludeFieldNames) {
    final int size = size();
    for (int i = 0; i < size; i++) {
      final Object listValue = get(i);
      if (DataType.equal(value, listValue, excludeFieldNames)) {
        return true;
      }
    }
    return false;
  }

  default boolean equals(final Object value1, final Object value2,
      final Collection<? extends CharSequence> excludeFieldNames) {
    final List<?> list1 = (List<?>) value1;
    final List<?> list2 = (List<?>) value2;
    if (list1.size() != list2.size()) {
      return false;
    } else {
      for (int i = 0; i < list1.size(); i++) {
        final Object value11 = list1.get(i);
        final Object value21 = list2.get(i);
        if (!DataType.equal(value11, value21, excludeFieldNames)) {
          return false;
        }
      }
    }
    return true;
  }

  default <T> void forEachType(final Consumer<T> action) {
    ListEx.super.forEach(value -> {
      action.accept((T) value);
    });
  }

  @SuppressWarnings({
      "unchecked", "rawtypes"
  })
  default <T> Iterable<T> iterable() {
    return (Iterable) this;
  }

  @SuppressWarnings({
      "unchecked", "rawtypes"
  })
  default Reader<JsonObject> jsonObjects() {
    return (Reader) Reader.wrap(iterator());
  }

  @Override
  default boolean removeEmptyProperties() {
    boolean removed = false;
    for (final Iterator<Object> iterator = iterator(); iterator.hasNext();) {
      final Object value = iterator.next();
      if (value instanceof JsonType) {
        final JsonType jsonValue = (JsonType) value;
        jsonValue.removeEmptyProperties();
        if (jsonValue.isEmpty()) {
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
  default JsonList toJson() {
    return (JsonList) JsonType.super.toJson();
  }

  @Override
  default String toJsonString(final boolean indent) {
    return Json.toString(this, indent);
  }
}
