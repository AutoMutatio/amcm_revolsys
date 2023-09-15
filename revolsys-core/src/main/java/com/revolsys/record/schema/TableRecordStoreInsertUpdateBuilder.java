package com.revolsys.record.schema;

import java.util.function.Supplier;

import org.jeometry.common.exception.Exceptions;

import com.revolsys.record.ArrayChangeTrackRecord;
import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.TransactionOptions;

import reactor.core.publisher.Mono;

public class TableRecordStoreInsertUpdateBuilder<R extends Record> extends InsertUpdateBuilder<R> {

  private final AbstractTableRecordStore recordStore;

  private final TableRecordStoreConnection connection;

  public TableRecordStoreInsertUpdateBuilder(final AbstractTableRecordStore recordStore,
    final TableRecordStoreConnection connection) {
    super(recordStore.newQuery(connection));
    this.recordStore = recordStore;
    this.connection = connection;
  }

  @Override
  public Record executeDo(final Supplier<Transaction> transactionSupplier) {
    final Query query = getQuery();
    query.setRecordFactory(ArrayChangeTrackRecord.FACTORY);
    try (
      Transaction transaction = transactionSupplier.get()) {
      final ChangeTrackRecord changeTrackRecord = query.getRecord();
      if (changeTrackRecord == null) {
        if (isInsert()) {
          final Record newRecord = newRecord();
          if (newRecord == null) {
            return null;
          } else {
            insertRecord(newRecord);
            try {
              return this.recordStore.insertRecord(this.connection, newRecord);
            } catch (final Exception e) {
              throw Exceptions.wrap("Unable to insert record:\n" + newRecord, e);
            }
          }
        } else {
          return null;
        }
      } else {
        if (isUpdate()) {
          try {
            updateRecord(changeTrackRecord);
            this.recordStore.updateRecordDo(this.connection, changeTrackRecord);
            return changeTrackRecord.newRecord();
          } catch (final Exception e) {
            throw Exceptions.wrap("Unable to update record:\n" + changeTrackRecord, e);
          }
        } else {
          return null;
        }
      }
    }
  }

  @Override
  protected Mono<R> insertRecordMonoDo(final R newRecord) {
    return this.recordStore.insertRecordMono(this.connection, newRecord);
  }

  @Override
  protected Transaction newTransaction() {
    return this.connection.newTransaction(TransactionOptions.REQUIRED);
  }

  @Override
  protected <V> Mono<V> transaction(final Supplier<Mono<V>> action) {
    return this.connection.transactionMono(t -> action.get());
  }

  @Override
  protected Mono<ChangeTrackRecord> updateRecordMonoDo(final ChangeTrackRecord record) {
    return this.recordStore.updateRecordMonoDo(this.connection, record);
  }
}
