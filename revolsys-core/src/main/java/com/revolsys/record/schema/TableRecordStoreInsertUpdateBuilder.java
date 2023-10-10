package com.revolsys.record.schema;

import java.util.function.Supplier;

import org.jeometry.common.exception.Exceptions;

import com.revolsys.record.ArrayChangeTrackRecord;
import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.TransactionOptions;

public class TableRecordStoreInsertUpdateBuilder extends InsertUpdateBuilder {

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
        try {
          updateRecord(changeTrackRecord);
          this.recordStore.updateRecordDo(this.connection, changeTrackRecord);
          return changeTrackRecord.newRecord();
        } catch (final Exception e) {
          throw Exceptions.wrap("Unable to update record:\n" + changeTrackRecord, e);
        }
      }
    }
  }

  @Override
  protected Transaction newTransaction() {
    return this.connection.newTransaction(TransactionOptions.REQUIRED);
  }
}
