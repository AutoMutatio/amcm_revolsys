package com.revolsys.collection.json;

import java.util.Collection;
import java.util.List;

import com.revolsys.collection.list.DelegatingList;
import com.revolsys.collection.list.ListEx;
import com.revolsys.data.type.DataType;

import com.revolsys.collection.list.ArrayListEx;

public class JsonListArray extends ArrayListEx<Object> implements JsonList {
  private static final long serialVersionUID = 1L;

  JsonListArray() {
  }

  JsonListArray(final Collection<? extends Object> c) {
    super(c);
  }

  JsonListArray(final int initialCapacity) {
    super(initialCapacity);
  }

  JsonListArray(final Object value) {
    add(value);
  }

  JsonListArray(final Object... values) {
    for (final Object value : values) {
      add(value);
    }
  }

  @Override
  public JsonList clone() {
    return new JsonListArray()//
      .addValuesClone(this);
  }

  @Override
  public boolean equals(final Object value2) {
    if (value2 instanceof List<?>) {
      final List<?> list2 = (List<?>)value2;
      if (size() != list2.size()) {
        return false;
      } else {
        for (int i = 0; i < size(); i++) {
          final Object value11 = get(i);
          final Object value21 = list2.get(i);
          if (!DataType.equal(value11, value21)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  @Override
  public boolean equals(final Object object,
    final Collection<? extends CharSequence> excludeFieldNames) {
    if (object instanceof List<?>) {
      final List<?> list2 = (List<?>)object;
      if (size() == list2.size()) {
        for (int i = 0; i < size(); i++) {
          final Object value11 = get(i);
          final Object value21 = list2.get(i);
          if (!DataType.equal(value11, value21, excludeFieldNames)) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public ListEx<Object> subList(final int fromIndex, final int toIndex) {
    return new DelegatingList<>(super.subList(fromIndex, toIndex));
  }

  @Override
  public String toString() {
    return Json.toString(this, false);
  }
}
