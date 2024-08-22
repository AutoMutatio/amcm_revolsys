package com.revolsys.collection.iterator;

import java.util.Collections;
import java.util.Iterator;

public interface IterableWithCount<V> extends BaseIterable<V> {
  public static class Empty<T> implements IterableWithCount<T> {
    private static Empty<?> EMPTY = new Empty<>();

    private Empty() {
    }

    @Override
    public long getCount() {
      return 0;
    }

    @Override
    public Iterator<T> iterator() {
      return Collections.emptyIterator();
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> Empty<T> empty() {
    return (Empty<T>)Empty.EMPTY;
  }

  public long getCount();
}
