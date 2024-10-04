package com.revolsys.record.schema;

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
    final ChangeTrackRecord changeTrackRecord = query.getRecord();
    if (changeTrackRecord == null) {
      if (isInsert()) {
        final Record newRecord = newRecord();
        if (newRecord == null) {
          return null;
        }
        prepareInsertRecord(newRecord);
        try {
          return this.recordStore.insertRecord(newRecord);
        } catch (final RuntimeException e) {
          throw Exceptions.wrap("Unable to insert record:\n" + newRecord, e);
        }
      }
    } else if (isUpdate()) {
      try {
        prepareUpdateRecord(changeTrackRecord);
        if (changeTrackRecord.isModified()) {
          this.recordStore.updateRecord(changeTrackRecord);
        }
        return changeTrackRecord.newRecord();
      } catch (final Exception e) {
        throw Exceptions.wrap("Unable to update record:\n" + changeTrackRecord, e);
      }
    }
    return null;
  }

  @Override
  public Transactionable getTransactionable() {
    return this.recordStore;
  }
}
