package com.revolsys.record.schema;

import java.util.function.Consumer;
import java.util.function.Function;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.io.PathName;
import org.jeometry.common.io.PathNameProxy;
import org.reactivestreams.Publisher;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.ReactiveTransactionManager;

import com.revolsys.record.Record;
import com.revolsys.record.io.format.json.JsonObject;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transactionable;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public interface TableRecordStoreConnection extends Transactionable, TableRecordStoreFactory {

  @SuppressWarnings("unchecked")
  default <V, C extends TableRecordStoreConnection> V apply(final Function<C, V> f) {
    return f.apply((C)this);
  }

  @Override
  default ReactiveTransactionManager getReactiveTransactionManager() {
    return getRecordStore()//
      .getReactiveTransactionManager();
  }

  default RecordDefinition getRecordDefinition(final CharSequence tableName) {
    final AbstractTableRecordStore recordStore = getTableRecordStore(tableName);
    if (recordStore == null) {
      return null;
    }
    return recordStore.getRecordDefinition();
  }

  RecordStore getRecordStore();

  @Override
  default <TRS extends AbstractTableRecordStore> TRS getTableRecordStore(
    final PathNameProxy pathNameProxy) {
    if (pathNameProxy != null) {
      final PathName pathName = pathNameProxy.getPathName();
      return getTableRecordStore(pathName);
    }
    return null;
  }

  @Override
  default PlatformTransactionManager getTransactionManager() {
    return getRecordStore()//
      .getTransactionManager();
  }

  default Context initTransactionContext(final Context context) {
    return context;
  }

  default Record insertRecord(final Record record) {
    return getTableRecordStore(record)//
      .insertRecord(this, record);
  }

  default <R extends Record> Mono<R> insertRecordMono(final R record) {
    return getTableRecordStore(record)//
      .insertRecordMono(this, record);
  }

  default Query newQuery(final CharSequence tablePath) {
    final AbstractTableRecordStore recordStore = getTableRecordStore(tablePath);
    return recordStore.newQuery(this);
  }

  default Record newRecord(final CharSequence tablePath) {
    final AbstractTableRecordStore tableRecordStore = getTableRecordStore(tablePath);
    return tableRecordStore.newRecord();
  }

  default Record newRecord(final CharSequence tablePath, final JsonObject json) {
    final AbstractTableRecordStore tableRecordStore = getTableRecordStore(tablePath);
    return tableRecordStore.newRecord(json);
  }

  default <V> Flux<V> transactionFlux(
    final Function<ReactiveTransaction, ? extends Publisher<V>> action) {
    return getRecordStore()//
      .transactionFlux(action)
      .contextWrite(this::initTransactionContext);
  }

  default <V> Mono<V> transactionMono(final Function<ReactiveTransaction, Mono<V>> action) {
    return getRecordStore()//
      .transactionMono(action)
      .contextWrite(this::initTransactionContext);
  }

  default Record updateRecord(final CharSequence tablePath, final Identifier id,
    final Consumer<Record> updateAction) {
    final AbstractTableRecordStore tableRecordStore = getTableRecordStore(tablePath);
    return tableRecordStore.updateRecord(this, id, updateAction);
  }

  default Record updateRecord(final CharSequence tablePath, final Identifier id,
    final JsonObject values) {
    final AbstractTableRecordStore tableRecordStore = getTableRecordStore(tablePath);
    return tableRecordStore.updateRecord(this, id, values);
  }

  default Record updateRecord(final CharSequence tablePath, final Object id,
    final Consumer<Record> updateAction) {
    final AbstractTableRecordStore tableRecordStore = getTableRecordStore(tablePath);
    return tableRecordStore.updateRecord(this, id, updateAction);
  }

  default Record updateRecord(final Record record, final Consumer<Record> updateAction) {
    final PathName tablePath = record.getPathName();
    final Identifier id = record.getIdentifier();
    return updateRecord(tablePath, id, updateAction);
  }
}
