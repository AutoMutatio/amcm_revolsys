package com.revolsys.transaction;

public interface TransactionableResource {

  default void afterCommit() {
  }

  default void afterCompletion() {
  }

  default void beforeCommit() {
  }

  default void beforeCompletion() {
  }

  default void close() {
  }

  void commit();

  default void flush() {
  }

  default void resume() {
  }

  void rollback();

  default void suspend() {
  }
}
