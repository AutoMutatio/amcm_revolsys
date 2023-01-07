package com.revolsys.record.schema;

import java.util.List;

import com.revolsys.collection.list.Lists;

public class MultiRecordFieldSecurityPolicy implements RecordFieldSecurityPolicy {

  private final List<RecordFieldSecurityPolicy> policies;

  MultiRecordFieldSecurityPolicy(final List<RecordFieldSecurityPolicy> policies) {
    this.policies = policies;
  }

  @Override
  public boolean canSetFieldName(final String fieldName) {
    for (final RecordFieldSecurityPolicy policy : this.policies) {
      if (policy.canSetFieldName(fieldName)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return Lists.toString(this.policies);
  }
}
