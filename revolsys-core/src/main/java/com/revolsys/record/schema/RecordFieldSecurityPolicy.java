package com.revolsys.record.schema;

import java.util.List;
import java.util.Set;

import org.springframework.dao.PermissionDeniedDataAccessException;

import com.revolsys.collection.list.Lists;

public interface RecordFieldSecurityPolicy {

  RecordFieldSecurityPolicy ALLOW = fieldName -> true;

  RecordFieldSecurityPolicy DENY = fieldName -> false;

  public static RecordFieldSecurityPolicy create(final Set<RecordStoreSecurityPolicy> policies,
    final RecordAccessType accessType) {
    final int policyCount = policies.size();
    if (policyCount == 0) {
      throw new PermissionDeniedDataAccessException("No " + accessType + " permission", null);
    } else if (policyCount == 1) {
      return policies.iterator().next().getFieldPolicy(accessType);
    } else {
      final List<RecordFieldSecurityPolicy> fieldPolicies = Lists.map(policies,
        p -> p.getFieldPolicy(accessType));
      return new MultiRecordFieldSecurityPolicy(fieldPolicies);
    }
  }

  boolean canSetFieldName(String fieldName);

}
