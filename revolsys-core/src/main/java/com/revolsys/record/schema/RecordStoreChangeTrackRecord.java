package com.revolsys.record.schema;

import com.revolsys.io.BaseCloseable;
import com.revolsys.record.ArrayChangeTrackRecord;

public class RecordStoreChangeTrackRecord extends ArrayChangeTrackRecord {

  private final AbstractTableRecordStore recordStore;

  private TableRecordStoreConnection connection;

  private ChangeMode changeMode = ChangeMode.INSERT;

  public RecordStoreChangeTrackRecord(final AbstractTableRecordStore recordStore) {
    super(recordStore.getRecordDefinition());
    this.recordStore = recordStore;
  }

  @Override
  protected synchronized boolean setValue(final FieldDefinition field, final Object value) {
    if (this.connection != null
      && !this.recordStore.canSetField(this.connection, this.changeMode, field, value)) {
      // Don't update if the record store doesn't allow it
      return false;
    } else {
      return super.setValue(field, value);
    }
  }

  public BaseCloseable startUpdates(final TableRecordStoreConnection connection,
    final ChangeMode changeMode) {
    this.connection = connection;
    this.changeMode = changeMode;
    return () -> this.connection = null;
  }
}
