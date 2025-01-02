package com.revolsys.transaction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.revolsys.exception.Exceptions;
import com.revolsys.util.BaseCloseable;

public class ActiveTransactionContext implements TransactionContext {

  private final Map<Object, TransactionableResource> resources = new LinkedHashMap<>();

  private final List<Runnable> afterCommits = new ArrayList<>();

  private boolean readOnly;

  private boolean rollbackOnly;

  private Isolation isolation = Isolation.DEFAULT;

  private String name;

  private int timeout;

  private TransactionStatus status = TransactionStatus.ACTIVE;

  private Throwable exception;

  ActiveTransactionContext() {
  }

  ActiveTransactionContext(final Iterable<Consumer<ActiveTransactionContext>> initializers) {
    this(null, initializers);
  }

  ActiveTransactionContext(final TransactionDefinition<?> definition,
    final Iterable<Consumer<ActiveTransactionContext>> initializers) {
    if (initializers != null) {
      for (final Consumer<ActiveTransactionContext> initializer : initializers) {
        initializer.accept(this);
      }
    }
    if (definition != null) {
      this.name = definition.getName();
      this.isolation = definition.getIsolation();
      this.readOnly = definition.isReadOnly();
      this.timeout = definition.getTimeout();
    }
  }

  public void addAfterCommit(final Runnable action) {
    this.afterCommits.add(action);
  }

  private void addException(final Throwable e) {
    if (this.exception == null) {
      this.exception = e;
    }
  }

  @Override
  public void close() {
    try {
      if (isRollbackOnly()) {
        final boolean unexpectedRollback = false;

        rollbackDo();

        if (unexpectedRollback) {
          throw new IllegalStateException(
            "Transaction rolled back because it has been marked as rollback-only");
        }
      } else {
        try {
          for (final var r : this.resources.values()) {
            r.commit();
          }
        } catch (RuntimeException | Error e) {
          addException(e);
          rollbackDo();
          throw e;
        }

        try {

          this.afterCommits.forEach(action -> {
            try {
              action.run();
            } catch (final Exception e) {

            }
          });
        } finally {
          this.status = TransactionStatus.COMMITTED;
        }
      }
    } finally {
      for (final var r : this.resources.values()) {
        try {
          r.close();
        } catch (final Throwable e) {
          addException(e);
        }
      }
    }

  }

  public void flush() {
    for (final var r : this.resources.values()) {
      r.flush();
    }
  }

  @Override
  public Isolation getIsolation() {
    return this.isolation;
  }

  public String getName() {
    return this.name;
  }

  @SuppressWarnings("unchecked")
  public <R extends TransactionableResource> R getResource(final Object key,
    final Function<ActiveTransactionContext, R> constructor) {
    R resource = (R)this.resources.get(key);
    if (resource == null) {
      resource = constructor.apply(this);
      this.resources.put(key, resource);
    }
    return resource;
  }

  @Override
  public TransactionStatus getStatus() {
    return this.status;
  }

  @Override
  public boolean isActive() {
    return true;
  }

  /**
   * Return if this transaction is defined as read-only transaction.
   */
  @Override
  public boolean isReadOnly() {
    return this.readOnly;
  }

  @Override
  public boolean isRollbackOnly() {
    return this.rollbackOnly;
  }

  private void rollbackDo() {
    try {
      Exception firstException = null;
      for (final var resource : this.resources.values()) {
        try {
          resource.rollback();
        } catch (final Exception e) {
          if (firstException == null) {
            firstException = e;
          }
        }
      }
      if (firstException != null) {
        Exceptions.throwUncheckedException(firstException);
      }
      this.status = TransactionStatus.ROLLBACK;
    } catch (RuntimeException | Error ex) {
      this.status = TransactionStatus.UNKNWOWN;
      throw ex;
    }
  }

  @Override
  public void setRollbackOnly() {
    this.rollbackOnly = true;
  }

  @Override
  public <V> V setRollbackOnly(final Throwable e) {
    for (final var resource : this.resources.values()) {
      resource.setHasError();
    }
    return TransactionContext.super.setRollbackOnly(e);
  }

  BaseCloseable suspend() {
    for (final var r : this.resources.values()) {
      try {
        r.suspend();
      } catch (final Throwable e) {
        addException(e);
      }
    }
    return () -> 
      resume()
    ;
  }

  private void resume() {
    for (final var r : this.resources.values()) {
      try {
        r.resume();
      } catch (final Throwable e) {
        addException(e);
      }
    }
  }

}
