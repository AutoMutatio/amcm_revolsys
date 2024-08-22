package com.revolsys.transaction;

import java.util.concurrent.Callable;

import com.revolsys.transaction.Transaction.RunAction;

public interface Transactionable {
  default TransactionBuilder transaction() {
    return TransactionBuilder.BUILDER;
  }

  default <V> V transactionCall(final Callable<V> action) {
    return transaction().required().call(action);
  }

  default <V> V transactionNewCall(final Callable<V> action) {
    return transaction().requiresNew().call(action);
  }

  default void transactionNewRun(final RunAction action) {
    transaction().requiresNew().run(action);
  }

  default void transactionRun(final RunAction action) {
    transaction().required().run(action);
  }
}
