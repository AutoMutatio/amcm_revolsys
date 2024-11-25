package com.revolsys.data.type;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;

import com.revolsys.collection.list.ArrayListEx;
import com.revolsys.collection.list.ListEx;
import com.revolsys.exception.Exceptions;

public class ListDataType extends SimpleDataType {

  public static ListDataType of(final DataType contentType) {
    return new ListDataType(ListEx.class, contentType);
  }

  private final DataType contentType;

  public ListDataType(final Class<?> javaClass, final DataType contentType) {
    this("List", javaClass, contentType);
  }

  public ListDataType(final String name, final Class<?> javaClass, final DataType contentType) {
    super(name, javaClass);
    this.contentType = contentType;
  }

  @SuppressWarnings({
    "unchecked", "rawtypes"
  })
  private ListEx<Object> createList() {
    try {
      final Class<?> javaClass = getJavaClass();
      final ListEx<Object> newCollection;
      if (ListEx.class == javaClass) {
        newCollection = new ArrayListEx<>();
      } else {
        final Constructor<?> declaredConstructor = javaClass.getDeclaredConstructor();
        newCollection = (ListEx)declaredConstructor.newInstance();
      }
      return newCollection;
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2) {
    final ListEx<?> list1 = (ListEx<?>)value1;
    final ListEx<?> list2 = (ListEx<?>)value2;
    if (list1.size() != list2.size()) {
      return false;
    } else {
      for (int i = 0; i < list1.size(); i++) {
        final Object value11 = list1.get(i);
        final Object value21 = list2.get(i);
        if (!DataType.equal(value11, value21)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2,
    final Collection<? extends CharSequence> excludeFieldNames) {
    final ListEx<?> list1 = (ListEx<?>)value1;
    final ListEx<?> list2 = (ListEx<?>)value2;
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

  public DataType getContentType() {
    return this.contentType;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Object toObjectDo(final Object value) {
    if (value instanceof final Collection collection) {
      final ListEx<Object> list = createList();
      list.addAll(collection);
      return list;
    } else if (value instanceof final Iterable iterable) {
      final ListEx<Object> list = createList();
      iterable.forEach(list::add);
      return list;
    } else if (value instanceof CharSequence) {
      final String string = value.toString();
      final ListEx<Object> list = createList();
      CollectionDataType.parseStringCollection(list, this.contentType, string);
      return list;
    } else {
      return super.toObjectDo(value);
    }
  }

  @Override
  public String toString() {
    return super.toString() + "<" + this.contentType + ">";
  }
}
