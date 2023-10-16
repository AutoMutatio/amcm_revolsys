package com.revolsys.transaction;

class TransactionDefinition<V extends TransactionDefinition<V>> {
  static final int TIMEOUT_DEFAULT = -1;

  private Isolation isolation = Isolation.DEFAULT;

  private int timeout = TIMEOUT_DEFAULT;

  private boolean readOnly;

  private String name;

  public TransactionDefinition() {
  }

  public TransactionDefinition(final TransactionDefinition<?> definition) {
    this.isolation = definition.isolation;
    this.name = definition.name;
    this.readOnly = definition.readOnly;
    this.timeout = definition.timeout;

  }

  public Isolation getIsolation() {
    return this.isolation;
  }

  public String getName() {
    return this.name;
  }

  public int getTimeout() {
    return this.timeout;
  }

  public boolean isIsolation(final Isolation isolation) {
    return isolation == this.isolation;
  }

  public boolean isReadOnly() {
    return this.readOnly;
  }

  @SuppressWarnings("unchecked")
  public V readOnly() {
    this.readOnly = true;
    return (V)this;
  }

  @SuppressWarnings("unchecked")
  public V setIsolation(final Isolation isolation) {
    this.isolation = isolation;
    return (V)this;
  }

  @SuppressWarnings("unchecked")
  public V setName(final String name) {
    this.name = name;
    return (V)this;
  }

  @SuppressWarnings("unchecked")
  public V setTimeout(final int timeout) {
    if (timeout < -1) {
      throw new IllegalArgumentException("Invalid timeout: " + timeout);
    }
    this.timeout = timeout;
    return (V)this;
  }

}
