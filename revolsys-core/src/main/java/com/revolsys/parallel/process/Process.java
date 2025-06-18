package com.revolsys.parallel.process;

import org.springframework.beans.factory.BeanNameAware;

public interface Process extends Runnable, BeanNameAware {
  default void close() {
  }

  String getBeanName();

  default void initialize() {
  }

}
