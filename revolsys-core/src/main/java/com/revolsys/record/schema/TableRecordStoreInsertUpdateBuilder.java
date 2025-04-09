package com.revolsys.record.schema;

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
    final ChangeTrackRecord changeTrackRecord = query.getRecord();
    if (changeTrackRecord == null) {
      if (isInsert()) {
        final Record newRecord = newRecord();
        if (newRecord == null) {
          return null;
        }
        prepareInsertRecord(newRecord);
        try {
          return this.recordStore.insertRecord(this.connection, newRecord);
        } catch (final RuntimeException | Error e) {
          throw Exceptions.toWrapped(e)
            .property("record", changeTrackRecord);
        }
      }
    } else if (isUpdate()) {
      try {
        prepareUpdateRecord(changeTrackRecord);
        this.recordStore.updateRecordDo(this.connection, changeTrackRecord);
        return changeTrackRecord.newRecord();
      } catch (final RuntimeException | Error e) {
        throw Exceptions.toWrapped(e)
          .property("record", changeTrackRecord);
      }
    }
    return null;
  }

  @Override
  public Transactionable getTransactionable() {
    return this.connection;
  }
}
