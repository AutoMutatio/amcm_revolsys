package com.revolsys.reactive.chars;

public interface ValueProcessor<V> {
  default void onCancel() {
  }

  default void onComplete() {
  }

  boolean process(V value);

}
