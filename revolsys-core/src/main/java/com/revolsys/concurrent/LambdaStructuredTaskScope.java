package com.revolsys.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.exception.MultipleException;

public final class LambdaStructuredTaskScope<V> extends StructuredTaskScopeEx<V> {
  public static class Builder<BV> {
    private final LambdaStructuredTaskScope<BV> scope;

    public Builder() {
      this.scope = new LambdaStructuredTaskScope<>();
    }

    public Builder(final String name, final ThreadFactory factory) {
      this.scope = new LambdaStructuredTaskScope<>(name, factory);
    }

    public LambdaStructuredTaskScope<BV> build() {
      return this.scope;
    }

    public Builder<BV> close(final Runnable completeAction) {
      this.scope.closeAction = completeAction;
      return this;
    }

    public Builder<BV> error(final Consumer<Throwable> errorAction) {
      this.scope.errorAction = errorAction;
      return this;
    }

    public void join(final Consumer<LambdaStructuredTaskScope<BV>> action) {
      try (
        var scope = this.scope) {
        action.accept(scope);
        scope.join();
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

  private LambdaStructuredTaskScope() {
  }

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
        throw new MultipleException(this.exceptions);
      }
    }
  }

  @Override
  protected void handleComplete(final Subtask<? extends V> subtask) {
    if (subtask != null) {
      switch (subtask.state()) {
        case FAILED -> {
          final var throwable = subtask.exception();
          this.errorAction.accept(throwable);
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
  }

}
