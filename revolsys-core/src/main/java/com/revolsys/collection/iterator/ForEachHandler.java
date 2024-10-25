package com.revolsys.collection.iterator;

import java.util.function.Consumer;

public interface ForEachHandler<V> {
  void forEach(Consumer<? super V> action);
}
