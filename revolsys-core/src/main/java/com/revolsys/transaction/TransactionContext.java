package com.revolsys.transaction;

import com.revolsys.exception.Exceptions;
import com.revolsys.util.BaseCloseable;

public interface TransactionContext extends BaseCloseable {

  @Override
  default void close() {
  }

  default Isolation getIsolation() {
    return Isolation.DEFAULT;
  }

  default TransactionStatus getStatus() {
    return TransactionStatus.ACTIVE;
  }

  default boolean isActive() {
    return false;
  }

  default boolean isReadOnly() {
    return false;
  }

  default boolean isRollbackOnly() {
    return false;
  }

  default void setRollbackOnly() {
  }

  default <V> V setRollbackOnly(final Throwable e) {
    setRollbackOnly();
    return Exceptions.throwUncheckedException(e);
  }

}
