package com.revolsys.transaction;

import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionCallback;
import org.springframework.transaction.reactive.TransactionalOperator;

import reactor.core.publisher.Flux;

public interface Transactionable {
  private static <T> Flux<T> executeNoTransaction(final TransactionCallback<T> action) {
    final GenericReactiveTransaction status = new GenericReactiveTransaction(null, false, false,
      false, false, null);
    return Flux.from(action.doInTransaction(status));
  }

  default ReactiveTransactionManager getReactiveTransactionManager() {
    return null;
  }

  PlatformTransactionManager getTransactionManager();

  /**
   * Construct a new {@link Transaction} with {@link TransactionOptions#DEFAULT}.
   * @return The transaction.
   */
  default Transaction newTransaction() {
    return newTransaction(TransactionOptions.DEFAULT);
  }

  /**
   * Construct a new {@link Transaction} with the specified {@link TransactionOption}.
   *
   * Default values are
   *
   * <dl>
   * <dt>{@link Propagation}</dt>
   * <dd>{@link Propagation#REQUIRES_NEW}</dd>
   * <dt>{@link Isolation}</dt>
   * <dd>{@link Isolation#DEFAULT}</dd>
   * </dl>
   *
   * @param options The transaction options.
   * @return The transaction.
   * @see TransactionOption
   * @see TransactionOptions
   * @see Propagation
   * @see Isolation
   */
  default Transaction newTransaction(final TransactionOption... options) {
    final PlatformTransactionManager transactionManager = getTransactionManager();
    return new Transaction(transactionManager, options);
  }

  default TransactionalOperator transaction() {
    return transaction(TransactionDefinition.DEFAULT);
  }

  default TransactionalOperator transaction(
    final org.springframework.transaction.TransactionDefinition transactionDefinition) {
    final ReactiveTransactionManager manager = getReactiveTransactionManager();
    if (manager == null) {
      return Transactionable::executeNoTransaction;
    } else {
      return TransactionalOperator.create(manager, transactionDefinition);
    }
  }

  default TransactionalOperator transaction(final TransactionOption... options) {
    return transaction(new TransactionDefinition(options));
  }

  default void transactionExecute(final Consumer<Transaction> action,
    final TransactionOption... options) {
    try (
      Transaction transaction = newTransaction(options)) {
      transaction.execute(action);
    }
  }

  default <V> V transactionExecute(final Function<Transaction, V> action,
    final TransactionOption... options) {
    try (
      Transaction transaction = newTransaction(options)) {
      return transaction.execute(action);
    }
  }
}
