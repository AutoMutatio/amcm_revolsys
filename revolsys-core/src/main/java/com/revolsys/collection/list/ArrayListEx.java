package com.revolsys.collection.list;

import java.util.ArrayList;
import java.util.Collection;

public class ArrayListEx<V> extends ArrayList<V> implements ListEx<V> {

  private static final long serialVersionUID = 1L;

  public ArrayListEx() {
    super();
  }

  public ArrayListEx(final Collection<? extends V> c) {
    super(c);
  }

  public ArrayListEx(final int initialCapacity) {
    super(initialCapacity);
  }

  @Override
  public ListEx<V> clone() {
    return Lists.toArray(this);
  }

  @Override
  public ListEx<V> subList(final int fromIndex, final int toIndex) {
    return new DelegatingList<>(super.subList(fromIndex, toIndex));
  }
  //
  // @Override
  // public ListEx<V> subList(final int fromIndex, final int toIndex) {
  // if (fromIndex < 0) {
  // throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
  // }
  // if (toIndex > size()) {
  // throw new IndexOutOfBoundsException("toIndex = " + toIndex);
  // }
  // if (fromIndex > toIndex) {
  // throw new IllegalArgumentException("fromIndex(" + fromIndex + ") >
  // toIndex(" + toIndex + ")");
  // }
  // final ArrayListEx<V> list = new ArrayListEx<>(toIndex - fromIndex);
  // for (int i = fromIndex; i < toIndex; i++) {
  // final V value = get(i);
  // list.add(i, value);
  // }
  // return list;
  // }
}
