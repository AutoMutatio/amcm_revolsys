package com.revolsys.transaction;

import java.util.function.Consumer;

public class TransationBuilderWitInitializer extends TransactionBuilder {

  private final Consumer<ActiveTransactionContext> initializer;

  private final TransactionBuilder parent;

  TransationBuilderWitInitializer(final TransactionBuilder parent,
    final Consumer<ActiveTransactionContext> initializer) {
    this.parent = parent;
    this.initializer = initializer;
  }

  @Override
  public void initTransactionContext(final ActiveTransactionContext context) {
    this.parent.initTransactionContext(context);
    this.initializer.accept(context);
  }
}
