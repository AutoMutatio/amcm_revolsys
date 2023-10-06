package com.revolsys.transaction;

import java.lang.ScopedValue.Carrier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.jeometry.common.exception.Exceptions;

import com.revolsys.io.BaseCloseable;

public class Transaction {
  public interface Builder {

    <V> V call(Callable<V> action);

    void run(RunAction action);
  }

  static class MandatoryBuilder implements Builder {
    @Override
    public <V> V call(final Callable<V> action) {
      try {
        if (hasContext()) {
          return action.call();
        } else {
          throw new IllegalStateException(
            "Propagation Mandatory must run inside an existing transaction");
        }
      } catch (final Exception e) {
        return Exceptions.throwUncheckedException(e);
      }
    }

    @Override
    public void run(final RunAction action) {
      try {
        if (hasContext()) {
          action.run();
        } else {
          throw new IllegalStateException(
            "Propagation Mandatory must run inside an existing transaction");
        }
      } catch (final Exception e) {
        Exceptions.throwUncheckedException(e);
      }
    }
  }

  static class NeverBuilder implements Builder {
    @Override
    public <V> V call(final Callable<V> action) {
      try {
        if (hasContext()) {
          throw new IllegalStateException(
            "Propagation Mandatory must run inside an existing transaction");
        } else {
          return action.call();
        }
      } catch (final Exception e) {
        return Exceptions.throwUncheckedException(e);
      }
    }

    @Override
    public void run(final RunAction action) {
      try {
        if (hasContext()) {
          throw new IllegalStateException(
            "Propagation Mandatory must run inside an existing transaction");
        } else {
          action.run();
        }
      } catch (final Exception e) {
        Exceptions.throwUncheckedException(e);
      }
    }
  }

  static class NotSupportedBuilder implements Builder {
    @Override
    public <V> V call(final Callable<V> action) {
      try (
        var s = suspend()) {
        return whereCall(EMPTY, Collections.emptyList(), action);
      }
    }

    @Override
    public void run(final RunAction action) {
      try (
        var s = suspend()) {
        whereRun(EMPTY, Collections.emptyList(), action);
      }
    }
  }

  public static class RequiredBuilder implements Builder {
    private final List<Consumer<ActiveTransactionContext>> initializers = new ArrayList<>();

    public RequiredBuilder addInit(final Consumer<ActiveTransactionContext> initializer) {
      this.initializers.add(initializer);
      return this;
    }

    @Override
    public <V> V call(final Callable<V> action) {
      if (hasContext()) {
        try {
          return action.call();
        } catch (final Exception e) {
          return Exceptions.throwUncheckedException(e);
        }
      } else {
        return whereCall(new ActiveTransactionContext(), this.initializers, action);
      }
    }

    @Override
    public void run(final RunAction action) {
      if (hasContext()) {
        try {
          action.run();
        } catch (final Exception e) {
          Exceptions.throwUncheckedException(e);
        }
      } else {
        whereRun(new ActiveTransactionContext(), this.initializers, action);
      }
    }

  }

  public static class RequiresNewBuilder extends TransactionDefinition<RequiresNewBuilder>
    implements Builder {
    private final List<Consumer<ActiveTransactionContext>> initializers = new ArrayList<>();

    public RequiresNewBuilder addInit(final Consumer<ActiveTransactionContext> initializer) {
      this.initializers.add(initializer);
      return this;
    }

    @Override
    public <V> V call(final Callable<V> action) {
      try (
        var s = suspend()) {
        return whereCall(new ActiveTransactionContext(this), this.initializers, action);
      }
    }

    @Override
    public void run(final RunAction action) {
      try (
        var s = suspend()) {
        whereRun(new ActiveTransactionContext(this), this.initializers, action);
      }
    }

  }

  public interface RunAction {
    void run() throws Exception;
  }

  static class SupportsBuilder implements Builder {

    @Override
    public <V> V call(final Callable<V> action) {
      try {
        return action.call();
      } catch (final Exception e) {
        return Exceptions.throwUncheckedException(e);
      }
    }

    @Override
    public void run(final RunAction action) {
      try {
        action.run();
      } catch (final Exception e) {
        Exceptions.throwUncheckedException(e);
      }
    }
  }

  private static final TransactionContext EMPTY = new EmptyTransactionContext();

  static final ScopedValue<TransactionContext> CONTEXT = ScopedValue.newInstance();

  public static void afterCommit(final Runnable action) {
    final TransactionContext context = getContext();
    if (context instanceof final ActiveTransactionContext activeContext) {
      activeContext.addAfterCommit(action);
    } else {
      action.run();
    }
  }

  public static void assertInTransaction() {
    assert !isActive() : "Must be called in a transaction";
  }

  public static TransactionContext getContext() {
    return CONTEXT.orElse(EMPTY);
  }

  public static boolean hasContext() {
    return CONTEXT.isBound();
  }

  public static boolean isActive() {
    return hasContext() && CONTEXT.get().isActive();
  }

  public static void rollback() {
    getContext().setRollbackOnly();
  }

  public static <V> V rollback(final Throwable e) {
    getContext().setRollbackOnly(e);
    return null;
  }

  private static BaseCloseable suspend() {
    final TransactionContext context = getContext();
    if (context instanceof final ActiveTransactionContext mapContext) {
      return mapContext.suspend();
    } else {
      return () -> {
      };
    }
  }

  public static TransactionBuilder transaction() {
    return TransactionBuilder.BUILDER;
  }

  static Carrier where(final TransactionContext context) {
    return ScopedValue.where(CONTEXT, context);
  }

  private static <V, C extends TransactionContext> V whereCall(final C context,
    final Collection<Consumer<C>> initializers, final Callable<V> action) {
    try (
      var c = context) {
      initializers.forEach(i -> i.accept(context));
      try {
        return ScopedValue.where(CONTEXT, context).call(action);
      } catch (final Throwable t) {
        return context.setRollbackOnly(t);
      }
    }
  }

  private static <C extends TransactionContext> void whereRun(final C context,
    final Collection<Consumer<C>> initializers, final RunAction action) {
    try (
      var c = context) {
      initializers.forEach(i -> i.accept(context));
      ScopedValue.where(CONTEXT, context).run(() -> {
        try {
          action.run();
        } catch (final Throwable t) {
          context.setRollbackOnly(t);
        }
      });
    } catch (final Exception e) {
      Exceptions.throwUncheckedException(e);
    }
  }

}
