package com.revolsys.io;

import java.io.Closeable;
import java.util.function.Consumer;

@FunctionalInterface
public interface BaseCloseable extends Closeable {
  public Consumer<? extends AutoCloseable> CLOSER = closeable -> {
    try {
      closeable.close();
    } catch (final Exception e) {
      // Ignore
    }
  };

  @SuppressWarnings({
    "unchecked", "rawtypes"
  })
  static <C extends BaseCloseable> Consumer<? super C> closer() {
    return (Consumer)CLOSER;
  }

  @Override
  void close();

  default void closeSilent() {
    try {
      close();
    } catch (final Exception e) {
    }
  }

  default BaseCloseable wrap() {
    return new CloseableWrapper(this);
  }
}
