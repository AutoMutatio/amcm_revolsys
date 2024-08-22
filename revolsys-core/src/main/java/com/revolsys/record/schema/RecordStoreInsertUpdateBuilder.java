package com.revolsys.record.schema;

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

  @Override
  public Transactionable getTransactionable() {
    return this.recordStore;
  }
}
