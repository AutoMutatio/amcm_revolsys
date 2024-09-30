package com.revolsys.record.schema;

import org.springframework.dao.DataIntegrityViolationException;

import com.revolsys.exception.Exceptions;
import com.revolsys.record.ArrayChangeTrackRecord;
import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transactionable;

public class RecordStoreInsertUpdateBuilder<R extends Record> extends InsertUpdateBuilder<R> {

  private final RecordStore recordStore;

  public RecordStoreInsertUpdateBuilder(final RecordStore recordStore, final Query query) {
    super(query);
    this.recordStore = recordStore;
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
            return this.recordStore.insertRecord(newRecord);
          } catch (final RuntimeException e) {
            final var dataIntegrityException = Exceptions.getCause(e,
              DataIntegrityViolationException.class);
            if (dataIntegrityException != null && isUpdate()) {
              continue;
            }
            throw e;
          }
        }
      } else if (isUpdate()) {
        updateRecord(changeTrackRecord);
        if (changeTrackRecord.isModified()) {
          this.recordStore.updateRecord(changeTrackRecord);
        }
        return changeTrackRecord.newRecord();
      }
    }
    return null;
  }

  @Override
  public Transactionable getTransactionable() {
    return this.recordStore;
  }
}
