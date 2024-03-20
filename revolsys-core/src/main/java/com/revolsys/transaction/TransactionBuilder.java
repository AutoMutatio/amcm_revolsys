package com.revolsys.transaction;

import java.util.function.Consumer;

import com.revolsys.transaction.Transaction.Builder;
import com.revolsys.transaction.Transaction.NeverBuilder;
import com.revolsys.transaction.Transaction.NotSupportedBuilder;
import com.revolsys.transaction.Transaction.RequiredBuilder;
import com.revolsys.transaction.Transaction.RequiresNewBuilder;
import com.revolsys.transaction.Transaction.SupportsBuilder;

public class TransactionBuilder implements Transactionable {

  public static final TransactionBuilder BUILDER = new TransactionBuilder();

  public void initTransactionContext(final ActiveTransactionContext context) {
  }

  /**
   * If a transaction exists run the action using that transaction, otherwise throw an illegal state exception.
   *
   * @param action
  * @return The transaction builder
    */
  public Builder mandatory() {
    return new Transaction.MandatoryBuilder();
  }

  public Builder nested() {
    throw new UnsupportedOperationException("Nested transactions not yet supported");
  }

  /**
   * If a transaction exists throw an illegal state exception, otherwise run the action.
   *
   * @param action
  * @return The transaction builder
    */
  public Builder never() {
    return new NeverBuilder();
  }

  /**
   * If a transaction exists suspend it and set the context to be the empty context.
   *
   * NOTE: you cannot configure the transaction using this method.
   * @param action
   */
  public Builder notSupported() {
    return new NotSupportedBuilder();
  }

  /**
   * If a transaction exists use that transaction, otherwise create a new transaction.
   *
   * NOTE: you cannot configure the transaction using this method.
   * @param action
   */
  public Builder required() {
    return new RequiredBuilder().addInit(this::initTransactionContext);
  }

  /**
   * Create a new transaction.
  * @return The transaction builder
    */
  public RequiresNewBuilder requiresNew() {
    return new RequiresNewBuilder().addInit(this::initTransactionContext);
  }

  /**
   * Execute in a transaction if it exists, otherwise execute without a transaction.
   *
   * <b>NOTE: This is included for completeness, it is in effect does nothing in this transactional system.
   * @return The transaction builder
   * @deprecated
   */
  @Deprecated
  public Builder supports() {
    return new SupportsBuilder();
  }

  @Override
  public TransactionBuilder transaction() {
    return this;
  }

  public TransactionBuilder withInitializer(final Consumer<ActiveTransactionContext> initializer) {
    return new TransationBuilderWitInitializer(this, initializer);
  }

}
