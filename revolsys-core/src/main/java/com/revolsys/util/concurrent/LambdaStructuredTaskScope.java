package com.revolsys.util.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.exception.Exceptions;
import com.revolsys.exception.MultipleException;

public final class LambdaStructuredTaskScope<V> extends StructuredTaskScopeEx<V> {
  public static class Builder<BV> {
    private final LambdaStructuredTaskScope<BV> scope;

    private final AtomicBoolean built = new AtomicBoolean(false);

    public Builder(final String name, final ThreadFactory factory) {
      this.scope = new LambdaStructuredTaskScope<>(name, factory);
    }

    private LambdaStructuredTaskScope<BV> build() {
      if (this.built.compareAndSet(false, true)) {
        return this.scope;
      } else {
        throw new IllegalStateException("Cannot build twice");
      }
    }

    public Builder<BV> close(final Runnable completeAction) {
      this.scope.closeAction = completeAction;
      return this;
    }

    public Builder<BV> error(final Consumer<Throwable> errorAction) {
      this.scope.errorAction = errorAction;
      return this;
    }

    public void join(final Consumer<StructuredTaskScopeEx<BV>> action) {
      try (
        var scope = build()) {
        try {
          action.accept(scope);
        } finally {
          if (!scope.isShutdown()) {
            scope.join();
          }
        }
      }
    }

    public <V> V join(final Function<StructuredTaskScopeEx<BV>, V> action) {
      try (
        var scope = build()) {
        try {
          return action.apply(scope);
        } finally {
          if (!scope.isShutdown()) {
            scope.join();
          }
        }
      }
    }

    public Builder<BV> success(final Consumer<BV> taskAction) {
      this.scope.taskAction = taskAction;
      return this;
    }

    public Builder<BV> throwErrors() {
      this.scope.throwErrors = true;
      return this;
    }
  }

  public interface RunnableWithException {
    void run() throws Exception;
  }

  private Runnable closeAction;

  private Consumer<V> taskAction;

  private boolean throwErrors;

  private Consumer<Throwable> errorAction;

  private final ListEx<Throwable> exceptions = Lists.newArray();

  private LambdaStructuredTaskScope(final String name, final ThreadFactory factory) {
    super(name, factory);
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      if (this.closeAction != null) {
        this.closeAction.run();
      }
      if (this.throwErrors && !this.exceptions.isEmpty()) {
        if (this.exceptions.size() == 1) {
          var exception = this.exceptions.get(0);
          throw Exceptions.toRuntimeException(exception);
        }
        throw new MultipleException(this.exceptions);
      }
    }
  }

  @Override
  protected void handleComplete(final Subtask<? extends V> subtask) {
    try {
      if (subtask != null) {
        switch (subtask.state()) {
          case FAILED -> {
            final var throwable = subtask.exception();
            if (this.errorAction != null) {
              this.errorAction.accept(throwable);
            }
            if (this.throwErrors) {
              this.exceptions.add(throwable);
            }
          }
          case SUCCESS -> {
            if (this.taskAction != null) {
              final V result = subtask.get();
              this.taskAction.accept(result);
            }
          }
          default -> {
          }
        }
      }
    } finally {
      super.handleComplete(subtask);
    }
  }

}
