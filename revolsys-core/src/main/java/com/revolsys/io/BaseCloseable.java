package com.revolsys.io;

import java.io.Closeable;
import java.util.function.Consumer;

@FunctionalInterface
public interface BaseCloseable extends Closeable {
  public Consumer<? extends BaseCloseable> CLOSER = BaseCloseable::close;

  @SuppressWarnings({
    "unchecked", "rawtypes"
  })
  static <C extends BaseCloseable> Consumer<? super C> closer() {
    return (Consumer)CLOSER;
  }

  @Override
  void close();

  default BaseCloseable wrap() {
    return new CloseableWrapper(this);
  }
}
