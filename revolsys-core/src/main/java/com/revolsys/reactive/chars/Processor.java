package com.revolsys.reactive.chars;

public interface Processor {

  default void onCancel() {
  }

  default void onComplete() {
  }

  default void onError(Throwable e) {
  }

}
