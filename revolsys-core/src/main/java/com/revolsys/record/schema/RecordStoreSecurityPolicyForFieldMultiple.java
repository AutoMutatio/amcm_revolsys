package com.revolsys.record.schema;

import java.util.List;

import com.revolsys.collection.list.Lists;
import com.revolsys.record.Record;
import com.revolsys.record.RecordFactory;

public class RecordStoreSecurityPolicyForFieldMultiple
  implements RecordStoreSecurityPolicyForField, RecordFactory<Record> {

  private final List<RecordStoreSecurityPolicyForField> policies;

  private final RecordDefinition recordDefinition;

  private RecordDefinition permissionRecordDefinition;

  RecordStoreSecurityPolicyForFieldMultiple(final RecordDefinition recordDefinition,
    final List<RecordStoreSecurityPolicyForField> policies) {
    this.recordDefinition = recordDefinition;
    this.policies = policies;
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    if (this.permissionRecordDefinition == null) {
      final List<String> fieldNames = Lists.filter(this.recordDefinition.getFieldNames(),
        this::isFieldAllowed);
      if (fieldNames.size() == this.recordDefinition.getFieldCount()) {
        this.permissionRecordDefinition = this.recordDefinition;
      } else {
        this.permissionRecordDefinition = this.recordDefinition.cloneFields(fieldNames);
      }
    }
    return this.permissionRecordDefinition;
  }

  @Override
  public boolean isFieldAllowed(final String fieldName) {
    for (final RecordStoreSecurityPolicyForField policy : this.policies) {
      if (policy.isFieldAllowed(fieldName)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Record newRecord(final RecordDefinition recordDefinition) {
    return getRecordDefinition().newRecord();
  }

  @Override
  public String toString() {
    return Lists.toString(this.policies);
  }
}
