package com.revolsys.record.schema;

import java.util.function.Supplier;

import com.revolsys.record.ArrayChangeTrackRecord;
import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.TransactionOptions;

import reactor.core.publisher.Mono;

public class RecordStoreInsertUpdateBuilder<R extends Record> extends InsertUpdateBuilder<R> {

  private final RecordStore recordStore;

  public RecordStoreInsertUpdateBuilder(final RecordStore recordStore,
    final TableRecordStoreConnection connection) {
    super(recordStore.newQuery());
    this.recordStore = recordStore;
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
            return this.recordStore.insertRecord(newRecord);
          }
        } else {
          return null;
        }
      } else {
        if (isUpdate()) {
          updateRecord(changeTrackRecord);
          if (changeTrackRecord.isModified()) {
            this.recordStore.updateRecord(changeTrackRecord);
          }
          return changeTrackRecord.newRecord();
        } else {
          return null;
        }
      }
    }
  }

  @Override
  protected Mono<R> insertRecordMonoDo(final R record) {
    return this.recordStore.insertRecordMono(record);
  }

  @Override
  protected Transaction newTransaction() {
    return this.recordStore.newTransaction(TransactionOptions.REQUIRED);
  }

  @Override
  protected <V> Mono<V> transaction(final Supplier<Mono<V>> action) {
    return this.recordStore.transactionMono(t -> action.get());
  }

  @Override
  protected Mono<ChangeTrackRecord> updateRecordMonoDo(final ChangeTrackRecord record) {
    return this.recordStore.updateRecordMono(record);
  }
}
