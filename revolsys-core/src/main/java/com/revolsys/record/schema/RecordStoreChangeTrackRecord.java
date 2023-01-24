package com.revolsys.record.schema;

import com.revolsys.io.BaseCloseable;
import com.revolsys.record.ArrayChangeTrackRecord;

public class RecordStoreChangeTrackRecord extends ArrayChangeTrackRecord {

  private final RecordStoreSecurityPolicyForField allowPolicy;

  private RecordStoreSecurityPolicyForField policy;

  public RecordStoreChangeTrackRecord(final AbstractTableRecordStore recordStore) {
    super(recordStore.getRecordDefinition());
    this.allowPolicy = RecordStoreSecurityPolicyForField.allow(getRecordDefinition());
    this.policy = this.allowPolicy;
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

  public BaseCloseable startUpdates(final RecordStoreSecurityPolicyForField policy) {
    this.policy = policy;
    return () -> this.policy = this.allowPolicy;
  }
}
