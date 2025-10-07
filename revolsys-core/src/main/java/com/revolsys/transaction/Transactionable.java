package com.revolsys.transaction;

import java.lang.ScopedValue.CallableOp;

import com.revolsys.transaction.Transaction.RunAction;

public interface Transactionable {
  default TransactionBuilder transaction() {
    return TransactionBuilder.BUILDER;
  }

  default <V, T extends Exception> V transactionCall(final CallableOp<V, T> action) {
    return transaction().required().call(action);
  }

  default <V, T extends Exception> V transactionNewCall(final CallableOp<V, T> action) {
    return transaction().requiresNew().call(action);
  }

  default void transactionNewRun(final RunAction action) {
    transaction().requiresNew().run(action);
  }

  default void transactionRun(final RunAction action) {
    transaction().required().run(action);
  }
}
