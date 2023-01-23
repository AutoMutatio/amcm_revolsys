package com.revolsys.record.schema;

import java.util.List;
import java.util.Set;

import org.springframework.dao.PermissionDeniedDataAccessException;

import com.revolsys.collection.list.Lists;

public interface RecordStoreAccessTypeSecurityPolicy {

  RecordStoreAccessTypeSecurityPolicy ALLOW = fieldName -> true;

  RecordStoreAccessTypeSecurityPolicy DENY = fieldName -> false;

  public static RecordStoreAccessTypeSecurityPolicy create(final Set<RecordStoreSecurityPolicies> policies,
    final RecordAccessType accessType) {
    if (policies == null) {
      return ALLOW;
    }
    final int policyCount = policies.size();
    if (policyCount == 0) {
      throw new PermissionDeniedDataAccessException("No " + accessType + " permission", null);
    } else if (policyCount == 1) {
      return policies.iterator().next().getSecurityPolicy(accessType);
    } else {
      final List<RecordStoreAccessTypeSecurityPolicy> fieldPolicies = Lists.map(policies,
        p -> p.getSecurityPolicy(accessType));
      return new MultiRecordFieldSecurityPolicy(fieldPolicies);
    }
  }

  default void enforceAccessAllowed() {
  }

  default boolean isAccessAllowed() {
    return true;
  }

  boolean isFieldAllowed(String fieldName);
}
