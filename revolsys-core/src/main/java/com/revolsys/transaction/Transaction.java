package com.revolsys.transaction;

import java.util.function.Consumer;
import java.util.function.Function;

import org.jeometry.common.exception.Exceptions;
import org.springframework.lang.Nullable;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.revolsys.io.BaseCloseable;

public class Transaction extends TransactionDefinition implements BaseCloseable {

  private static ThreadLocal<Transaction> currentTransaction = new ThreadLocal<>();

  public static void afterCommit(final Runnable runnable) {
    if (runnable != null) {
      if (TransactionSynchronizationManager.isSynchronizationActive()) {
        final RunnableAfterCommit synchronization = new RunnableAfterCommit(runnable);
        TransactionSynchronizationManager.registerSynchronization(synchronization);
      } else {
        runnable.run();
      }
    }
  }

  public static void assertInTransaction() {
    assert currentTransaction.get() != null : "Must be called in a transaction";
  }

  public static Transaction getCurrentTransaction() {
    return currentTransaction.get();
  }

  public static boolean isHasCurrentTransaction() {
    return getCurrentTransaction() != null;
  }

  public static void setCurrentRollbackOnly() {
    final Transaction transaction = currentTransaction.get();
    if (transaction != null) {
      transaction.setRollbackOnly();
    }
  }

  private Transaction previousTransaction;

  private PlatformTransactionManager transactionManager;

  private DefaultTransactionStatus transactionStatus;

  @Nullable
  private String name;

  public Transaction(final PlatformTransactionManager transactionManager,
    final TransactionDefinition definition) {
    this(transactionManager, definition.getOptions());
  }

  public Transaction(final PlatformTransactionManager transactionManager,
    final TransactionOption... options) {
    super(options);
    this.transactionManager = transactionManager;
    if (transactionManager == null) {
      this.transactionStatus = null;
    } else {
      this.transactionStatus = (DefaultTransactionStatus)transactionManager.getTransaction(this);
      if (isRollbackOnly()) {
        this.transactionStatus.setRollbackOnly();
      }
    }
    this.previousTransaction = getCurrentTransaction();
    currentTransaction.set(this);
  }

  @Override
  public void close() throws RuntimeException {
    commit();
    currentTransaction.set(this.previousTransaction);
    this.transactionManager = null;
    this.previousTransaction = null;
    this.transactionStatus = null;
  }

  protected void commit() {
    final DefaultTransactionStatus transactionStatus = this.transactionStatus;
    if (this.transactionManager != null && transactionStatus != null) {
      if (!transactionStatus.isCompleted()) {
        if (transactionStatus.isRollbackOnly()) {
          rollback();
        } else {
          try {
            this.transactionManager.commit(transactionStatus);
          } catch (final Throwable e) {
            Exceptions.throwUncheckedException(e);
          }
        }
      }
    }
  }

  public void execute(final Consumer<Transaction> action) {
    try {
      action.accept(this);
    } catch (final Throwable e) {
      setRollbackOnly(e);
    }
  }

  public <V> V execute(final Function<Transaction, V> action) {
    try {
      return action.apply(this);
    } catch (final Throwable e) {
      throw setRollbackOnly(e);
    }
  }

  public PlatformTransactionManager getTransactionManager() {
    return this.transactionManager;
  }

  public DefaultTransactionStatus getTransactionStatus() {
    return this.transactionStatus;
  }

  public boolean isCompleted() {
    if (this.transactionStatus == null) {
      return true;
    } else {
      return this.transactionStatus.isCompleted();
    }
  }

  @Override
  public boolean isRollbackOnly() {
    if (this.transactionStatus == null) {
      return super.isRollbackOnly() || isReadOnly();
    } else {
      return this.transactionStatus.isRollbackOnly();
    }
  }

  protected void rollback() {
    if (this.transactionManager != null && this.transactionStatus != null) {
      this.transactionManager.rollback(this.transactionStatus);
    }
  }

  @Override
  public Transaction setRollbackOnly() {
    super.setRollbackOnly();
    if (this.transactionStatus != null) {
      this.transactionStatus.setRollbackOnly();
    }
    return this;
  }

}
