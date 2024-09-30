package com.revolsys.record.schema;

import org.springframework.dao.DataIntegrityViolationException;

import com.revolsys.exception.Exceptions;
import com.revolsys.record.ArrayChangeTrackRecord;
import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transactionable;

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
  public Record executeDo() {
    final Query query = getQuery();
    query.setRecordFactory(ArrayChangeTrackRecord.FACTORY);
    final int maxRetries = 2;
    for (int numRetries = 0; numRetries < maxRetries; numRetries++) {
      final ChangeTrackRecord changeTrackRecord = query.getRecord();
      if (changeTrackRecord == null) {
        if (isInsert()) {
          final Record newRecord = newRecord();
          if (newRecord == null) {
            return null;
          }
          insertRecord(newRecord);
          try {
            // savepoint = this.connection.setSavepoint();
            return this.recordStore.insertRecord(this.connection, newRecord);
          } catch (final RuntimeException e) {
            final var dataIntegrityException = Exceptions.getCause(e,
              DataIntegrityViolationException.class);
            if (dataIntegrityException != null && isUpdate()) {
              continue;
            }
            throw Exceptions.wrap("Unable to insert record:\n" + newRecord, e);
          }
        }
      } else if (isUpdate()) {
        try {
          updateRecord(changeTrackRecord);
          this.recordStore.updateRecordDo(this.connection, changeTrackRecord);
          return changeTrackRecord.newRecord();
        } catch (final Exception e) {
          throw Exceptions.wrap("Unable to update record:\n" + changeTrackRecord, e);
        }
      }
    }
    return null;
  }

  @Override
  public Transactionable getTransactionable() {
    return this.connection;
  }
}
