package com.revolsys.util;

import java.io.Closeable;
import java.util.function.Consumer;

import com.revolsys.exception.Exceptions;

@FunctionalInterface
public interface BaseCloseable extends Closeable {

  static Consumer<AutoCloseable> CLOSER = resource -> {
    try {
      resource.close();
    } catch (final Exception e) {
      throw Exceptions.wrap(e);
    }
  };

  static BaseCloseable EMPTY = () -> {
  };

  static <C extends BaseCloseable> Consumer<? super C> closer() {
    return CLOSER;
  }

  @Override
  void close();

}
