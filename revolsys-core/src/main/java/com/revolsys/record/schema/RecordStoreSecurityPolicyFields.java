package com.revolsys.record.schema;

import java.util.Collection;

public interface RecordStoreSecurityPolicyFields {

  RecordStoreSecurityPolicyFields allow(String fieldName);

  RecordStoreSecurityPolicyFields allow(String... fieldNames);

  default RecordStoreSecurityPolicyFields allowAccess() {
    return setAccessAllowed(true);
  }

  RecordStoreSecurityPolicyFields deny(String fieldName);

  RecordStoreSecurityPolicyFields deny(String... fieldNames);

  default RecordStoreSecurityPolicyFields denyAccess() {
    return setAccessAllowed(false);
  }

  RecordStoreSecurityPolicyFields denyAll();

  RecordStoreSecurityPolicyFields setAccessAllowed(boolean accessAllowed);

  RecordStoreSecurityPolicyFields setAllowed(Collection<String> fieldNames);

  RecordStoreSecurityPolicyFields setAllowed(String... fieldNames);

  void setRecordDefinition(RecordDefinition recordDefinition);
}
