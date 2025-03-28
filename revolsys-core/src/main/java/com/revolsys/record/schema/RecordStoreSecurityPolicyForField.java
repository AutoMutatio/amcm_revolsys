package com.revolsys.record.schema;

import java.util.List;
import java.util.Set;

import org.springframework.dao.PermissionDeniedDataAccessException;

import com.revolsys.collection.list.Lists;

public interface RecordStoreSecurityPolicyForField {

  RecordStoreSecurityPolicyForField DENY = fieldName -> false;

  public static RecordStoreSecurityPolicyForField allow(final RecordDefinition recordDefinition) {
    return new RecordStoreSecurityPolicyForField() {
      @Override
      public RecordDefinition getRecordDefinition() {
        return recordDefinition;
      }

      @Override
      public boolean isFieldAllowed(final String fieldName) {
        return true;
      }
    };
  }

  public static RecordStoreSecurityPolicyForField create(final RecordDefinition recordDefinition,
    final Set<RecordStoreSecurityPolicies> policies, final RecordAccessType accessType) {
    if (policies == null) {
      return allow(recordDefinition);
    }
    final int policyCount = policies.size();
    if (policyCount == 0) {
      throw new PermissionDeniedDataAccessException("No " + accessType + " permission", null);
    } else if (policyCount == 1) {
      return policies.iterator().next().getSecurityPolicy(accessType);
    } else {
      final List<RecordStoreSecurityPolicyForField> fieldPolicies = Lists.map(policies,
        p -> p.getSecurityPolicy(accessType));
      return new RecordStoreSecurityPolicyForFieldMultiple(recordDefinition, fieldPolicies);
    }
  }

  default void enforceAccessAllowed() {
  }

  default RecordDefinition getRecordDefinition() {
    return null;
  }

  default boolean isAccessAllowed() {
    return true;
  }

  boolean isFieldAllowed(String fieldName);
}
