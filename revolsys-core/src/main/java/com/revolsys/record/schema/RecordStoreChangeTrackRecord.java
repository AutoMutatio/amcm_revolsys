package com.revolsys.record.schema;

import com.revolsys.io.BaseCloseable;
import com.revolsys.record.ArrayChangeTrackRecord;

public class RecordStoreChangeTrackRecord extends ArrayChangeTrackRecord {

  private RecordStoreAccessTypeSecurityPolicy policy = RecordStoreAccessTypeSecurityPolicy.ALLOW;

  public RecordStoreChangeTrackRecord(final AbstractTableRecordStore recordStore) {
    super(recordStore.getRecordDefinition());
  }

  @Override
  protected synchronized boolean setValue(final FieldDefinition field, final Object value) {
    if (this.policy.isFieldAllowed(field.getName())) {
      return super.setValue(field, value);
    } else {
      // Don't update if the policy doesn't allow it
      return false;
    }
  }

  public BaseCloseable startUpdates(final RecordStoreAccessTypeSecurityPolicy policy) {
    this.policy = policy;
    return () -> this.policy = RecordStoreAccessTypeSecurityPolicy.ALLOW;
  }
}
