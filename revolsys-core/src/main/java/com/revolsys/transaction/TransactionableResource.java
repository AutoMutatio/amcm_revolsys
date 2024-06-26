package com.revolsys.transaction;

public interface TransactionableResource {

  default void close() {
  }

  void commit();

  default void flush() {
  }

  default void resume() {
  }

  void rollback();

  void setHasError();

  default void suspend() {
  }
}
