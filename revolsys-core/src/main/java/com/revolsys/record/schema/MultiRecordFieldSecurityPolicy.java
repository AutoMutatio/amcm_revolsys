package com.revolsys.record.schema;

import java.util.List;

import com.revolsys.collection.list.Lists;

public class MultiRecordFieldSecurityPolicy implements RecordStoreAccessTypeSecurityPolicy {

  private final List<RecordStoreAccessTypeSecurityPolicy> policies;

  MultiRecordFieldSecurityPolicy(final List<RecordStoreAccessTypeSecurityPolicy> policies) {
    this.policies = policies;
  }

  @Override
  public boolean isFieldAllowed(final String fieldName) {
    for (final RecordStoreAccessTypeSecurityPolicy policy : this.policies) {
      if (policy.isFieldAllowed(fieldName)) {
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
