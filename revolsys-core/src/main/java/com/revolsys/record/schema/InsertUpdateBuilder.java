package com.revolsys.record.schema;

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.map.MapEx;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transactionable;

public abstract class InsertUpdateBuilder<R extends Record> {

  public record Result<R2 extends Record>(boolean inserted, R2 record) {
  }

  private final Query query;

  private final JsonObject searchValues = JsonObject.hash();

  private Consumer<Record> commonAction = r -> {
  };

  private Consumer<Query> queryAction;

  private Consumer<Record> insertAction = r -> {
  };

  private Consumer<Record> updateAction = r -> {
  };

  private Supplier<Record> newRecordSupplier;

  private boolean insert = true;

  private boolean update = true;

  public InsertUpdateBuilder(final Query query) {
    this.query = query;
    this.newRecordSupplier = query::newRecord;
  }

  /**
   * Callback to be applied for both insert and updated records. This will be
   * applied after the search values and insert action. It is recommended not
   * to use the same fields in all 3 locations.
   *
   * @param commonAction
   * @return
   */
  public InsertUpdateBuilder<R> common(final Consumer<Record> commonAction) {
    this.commonAction = commonAction;
    return this;
  }

  public InsertUpdateBuilder<R> common(final MapEx values) {
    this.commonAction = r -> r.addValues(values);
    return this;
  }

  public final Record execute() {
    return getTransactionable().transactionCall(() -> {
      preExecute();
      return executeDo();
    });
  }

  public abstract Record executeDo();

  public Query getQuery() {
    return this.query;
  }

  public Transactionable getTransactionable() {
    return this.query;
  }

  /**
   * Callback to be applied for inserted records.
   *
   * @param insertAction
   * @return
   */
  public InsertUpdateBuilder<R> insert(final Consumer<Record> insertAction) {
    this.insertAction = insertAction;
    return this;
  }

  public InsertUpdateBuilder<R> insert(final MapEx values) {
    this.insertAction = r -> r.addValues(values);
    return this;
  }

  /**
  * Insert the record in the database, applying the search values, insert action and common action.
  *
  * @param record The record to insert.
  * @return The record;
  */
  protected Record prepareInsertRecord(final Record record) {
    record.addValues(this.searchValues);
    this.insertAction.accept(record);
    this.commonAction.accept(record);
    return record;
  }

  public boolean isInsert() {
    return this.insert;
  }

  public boolean isUpdate() {
    return this.update;
  }

  protected Record newRecord() {
    return this.newRecordSupplier.get();
  }

  public InsertUpdateBuilder<R> newRecord(final Supplier<Record> newRecordSupplier) {
    this.newRecordSupplier = newRecordSupplier;
    return this;
  }

  protected void preExecute() {
    if (this.searchValues.isEmpty() && this.queryAction == null) {
      throw new IllegalStateException(
        "At least one search value or query modifier must be specfied");
    }
    if (this.queryAction != null) {
      this.queryAction.accept(this.query);
    }
    for (final String key : this.searchValues.keySet()) {
      final Object value = this.searchValues.getValue(key);
      this.query.and(key, value);

    }
  }

  /**
   * Full customization of query to find the record to update. {@link #search(Consumer)} is preferred
   * if it works as it also applies the values to an inserted record.
   *
   * @param queryAction
   * @return
   */
  public InsertUpdateBuilder<R> query(final Consumer<Query> queryAction) {
    this.queryAction = queryAction;
    return this;
  }

  /**
   * Configurer to be able to specify the fields and values to use in the search
   * for records. These values will also be added to inserted records.
   *
   * @param configurer
   * @return
   */
  public InsertUpdateBuilder<R> search(final Consumer<JsonObject> configurer) {
    configurer.accept(this.searchValues);
    return this;
  }

  public InsertUpdateBuilder<R> search(final String key, final Object value) {
    this.searchValues.addValue(key, value);
    return this;
  }

  public InsertUpdateBuilder<R> setInsert(final boolean insert) {
    this.insert = insert;
    return this;
  }

  public InsertUpdateBuilder<R> setUpdate(final boolean update) {
    this.update = update;
    return this;
  }

  /**
   * Callback to be applied for both updated records.
   *
   * @param commonAction
   * @return
   */
  public InsertUpdateBuilder<R> update(final Consumer<Record> updateAction) {
    this.updateAction = updateAction;
    return this;
  }

  public InsertUpdateBuilder<R> update(final MapEx values) {
    this.updateAction = r -> r.addValues(values);
    return this;
  }

  /**
   * Update the record in the database, applying the update action and common action.
   *
   * @param record The record to insert.
   * @return The record;
   */
  public Record prepareUpdateRecord(final Record record) {
    this.updateAction.accept(record);
    this.commonAction.accept(record);
    return record;
  }
}
