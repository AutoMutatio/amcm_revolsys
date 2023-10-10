package com.revolsys.record.schema;

import java.util.function.Supplier;

import com.revolsys.record.ArrayChangeTrackRecord;
import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.TransactionOptions;

public class RecordStoreInsertUpdateBuilder extends InsertUpdateBuilder {

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
        final Record newRecord = newRecord();
        if (newRecord == null) {
          return null;
        } else {
          insertRecord(newRecord);
          return this.recordStore.insertRecord(newRecord);
        }
      } else {
        updateRecord(changeTrackRecord);
        if (changeTrackRecord.isModified()) {
          this.recordStore.updateRecord(changeTrackRecord);
        }
        return changeTrackRecord.newRecord();
      }
    }
  }

  @Override
  protected Transaction newTransaction() {
    return this.recordStore.newTransaction(TransactionOptions.REQUIRED);
  }
}
