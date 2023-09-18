package com.revolsys.transaction;

import org.jeometry.common.exception.Exceptions;
import org.springframework.lang.Nullable;

public class TransactionDefinition
  implements org.springframework.transaction.TransactionDefinition {

  public static TransactionDefinition NONE = new TransactionDefinition();

  public static TransactionDefinition DEFAULT = new TransactionDefinition(Propagation.REQUIRES_NEW,
    Isolation.DEFAULT);

  public static TransactionDefinition REQUIRED_READONLY = new TransactionDefinition(
    Propagation.REQUIRED, TransactionOptions.READ_ONLY, TransactionOptions.ROLLBACK_ONLY);

  public static TransactionDefinition REQUIRES_NEW_READONLY = new TransactionDefinition(
    Propagation.REQUIRES_NEW, TransactionOptions.READ_ONLY, TransactionOptions.ROLLBACK_ONLY);

  public static TransactionDefinition REQUIRED = new TransactionDefinition(Propagation.REQUIRED);

  private Propagation propagation = Propagation.REQUIRES_NEW;

  private Isolation isolation = Isolation.DEFAULT;

  private int timeout = TIMEOUT_DEFAULT;

  private boolean readOnly;

  @Nullable
  private String name;

  private boolean rollbackOnly;

  private final TransactionOption[] options;

  public TransactionDefinition(final TransactionOption... options) {
    this.options = options;
    for (final TransactionOption option : options) {
      option.initialize(this);
    }
  }

  @Override
  public int getIsolationLevel() {
    return this.isolation.value();
  }

  TransactionOption[] getOptions() {
    return this.options;
  }

  @Override
  public int getPropagationBehavior() {
    return this.propagation.value();
  }

  @Override
  public int getTimeout() {
    return this.timeout;
  }

  public boolean isPropagation(final Propagation propagation) {
    return propagation == this.propagation;
  }

  @Override
  public boolean isReadOnly() {
    return this.readOnly;
  }

  public boolean isRollbackOnly() {
    return this.rollbackOnly;
  }

  public void setIsolation(final Isolation isolation) {
    this.isolation = isolation;
  }

  void setPropagation(final Propagation propagation) {
    this.propagation = propagation;
  }

  void setReadOnly(final boolean readOnly) {
    this.readOnly = readOnly;
  }

  public TransactionDefinition setRollbackOnly() {
    this.rollbackOnly = true;
    return this;
  }

  public RuntimeException setRollbackOnly(final Throwable e) {
    setRollbackOnly();
    return Exceptions.throwUncheckedException(e);
  }

  void setTimeout(final int timeout) {
    this.timeout = timeout;
  }

  @Override
  public String toString() {
    final StringBuilder s = new StringBuilder().append(this.propagation)
      .append(' ')
      .append(this.isolation);
    if (isRollbackOnly()) {
      s.append(" readOnly");
    }
    if (isRollbackOnly()) {
      s.append(" rollbackOnly");
    }
    if (this.timeout != TIMEOUT_DEFAULT) {
      s.append(" timeout=").append(this.timeout);
    }
    return s.toString();
  }
}
