package com.revolsys.collection.list;

import java.util.ArrayList;
import java.util.Collection;

public class ArrayListEx<V> extends ArrayList<V> implements ListEx<V> {

  public ArrayListEx() {
    super();
  }

  public ArrayListEx(final Collection<? extends V> c) {
    super(c);
  }

  public ArrayListEx(final int initialCapacity) {
    super(initialCapacity);
  }

}
