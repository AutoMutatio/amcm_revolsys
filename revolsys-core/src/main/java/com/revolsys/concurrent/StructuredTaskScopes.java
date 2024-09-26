package com.revolsys.concurrent;

import java.util.function.Consumer;

import com.revolsys.concurrent.LambdaStructuredTaskScope.Builder;
import com.revolsys.concurrent.LambdaStructuredTaskScope.RunnableWithException;

public class StructuredTaskScopes {
  public static <V> Builder<V> builder() {
    return new LambdaStructuredTaskScope.Builder<>();
  }

  public static <V> void join(final Consumer<LambdaStructuredTaskScope<V>> action)
    throws InterruptedException {
    StructuredTaskScopes.<V> builder()
      .throwErrors()
      .join(action);
  }

  public static <V> void join(final RunnableWithException... tasks) {
    StructuredTaskScopes.<V> builder()
      .throwErrors()
      .join(scope -> scope.forkRunnable(tasks));
  }

  public static <V> Builder<V> virtual(final String name) {
    return new LambdaStructuredTaskScope.Builder<>(name + "-", Thread.ofVirtual()
      .name(name, 0)
      .factory());
  }
}
