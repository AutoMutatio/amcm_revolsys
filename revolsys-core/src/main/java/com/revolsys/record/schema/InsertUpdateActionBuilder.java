package com.revolsys.record.schema;

import java.util.function.Consumer;

import com.revolsys.record.Record;
import com.revolsys.record.io.format.json.JsonObject;
import com.revolsys.record.query.Query;

import reactor.core.publisher.Mono;

public class InsertUpdateActionBuilder {

  private final Query query;

  private final JsonObject searchValues = JsonObject.hash();

  private Consumer<Record> commonAction = r -> {
  };

  private Consumer<Query> queryAction = q -> {
  };

  private Consumer<Record> insertAction = r -> {
  };

  private Consumer<Record> updateAction = r -> {
  };

  public InsertUpdateActionBuilder(final Query query) {
    this.query = query;
  }

  /**
   * Callback to be applied for both insert and updated records. This will be
   * applied after the search values and insert action. It is recommended not
   * to use the same fields in all 3 locations.
   *
   * @param commonAction
   * @return
   */
  public InsertUpdateActionBuilder common(final Consumer<Record> commonAction) {
    this.commonAction = commonAction;
    return this;
  }

  public Record execute() {
    if (this.searchValues.isEmpty()) {
      throw new IllegalStateException("At least one search value must be specfied");
    }
    this.queryAction.accept(this.query);
    for (final String key : this.searchValues.keySet()) {
      final Object value = this.searchValues.getValue(key);
      this.query.and(key, value);

    }
    return this.query.insertOrUpdateRecord(this::insertRecord, this::updateRecord);
  }

  public Mono<Record> executeMono() {
    // TODO this is a placeholder until full reactive is implemented
    return Mono.defer(() -> Mono.just(execute()));
  }

  /**
   * Callback to be applied for both updated records.
   *
   * @param insertAction
   * @return
   */
  public InsertUpdateActionBuilder insert(final Consumer<Record> insertAction) {
    this.insertAction = insertAction;
    return this;
  }

  /**
   * Insert the record in the database, applying the search values, insert action and common action.
   *
   * @param record The record to insert.
   * @return The record;
   */
  private Record insertRecord(final Record record) {
    record.addValues(this.searchValues);
    this.insertAction.accept(record);
    this.commonAction.accept(record);
    return record;
  }

  /**
   * Full customization of query to find the record to update. {@link #search(Consumer)} is preferred
   * if it works as it also applies the values to an inserted record.
   *
   * @param queryAction
   * @return
   */
  public InsertUpdateActionBuilder query(final Consumer<Query> queryAction) {
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
  public InsertUpdateActionBuilder search(final Consumer<JsonObject> configurer) {
    configurer.accept(this.searchValues);
    return this;
  }

  /**
   * Callback to be applied for both updated records.
   *
   * @param commonAction
   * @return
   */
  public InsertUpdateActionBuilder update(final Consumer<Record> updateAction) {
    this.updateAction = updateAction;
    return this;
  }

  /**
   * Update the record in the database, applying the update action and common action.
   *
   * @param record The record to insert.
   * @return The record;
   */
  private Record updateRecord(final Record record) {
    this.updateAction.accept(record);
    this.commonAction.accept(record);
    return record;
  }
}
