package com.revolsys.parallel.process;

import org.springframework.beans.factory.BeanNameAware;

public abstract class AbstractProcess implements Process, BeanNameAware {
  private String beanName;

  public AbstractProcess() {
    this(null);
  }

  public AbstractProcess(final String beanName) {
    if (beanName == null) {
      this.beanName = getClass().getName();
    } else {
      this.beanName = beanName;
    }
  }

  @Override
  public String getBeanName() {
    return this.beanName;
  }

  @Override
  public void setBeanName(final String beanName) {
    this.beanName = beanName;
  }

  public void stop() {
  }

  @Override
  public String toString() {
    final String className = getClass().getSimpleName();
    if (this.beanName == null) {
      return className;
    } else {
      return this.beanName + " (" + className + ")";
    }
  }
}
