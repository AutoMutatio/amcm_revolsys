package com.revolsys.record.schema;

import java.util.Arrays;
import java.util.Collection;

public class RecordStoreSecurityPolicyFieldsMultiple implements RecordStoreSecurityPolicyFields {

  private final RecordStoreSecurityPolicyFields[] policies;

  public RecordStoreSecurityPolicyFieldsMultiple(
    final RecordStoreSecurityPolicyFields... policies) {
    this.policies = policies;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsMultiple allow(final String fieldName) {
    for (final RecordStoreSecurityPolicyFields policy : this.policies) {
      policy.allow(fieldName);
    }
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsMultiple allow(final String... fieldNames) {
    for (final RecordStoreSecurityPolicyFields policy : this.policies) {
      policy.allow(fieldNames);
    }
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsMultiple deny(final String fieldName) {
    for (final RecordStoreSecurityPolicyFields policy : this.policies) {
      policy.deny(fieldName);
    }
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsMultiple deny(final String... fieldNames) {
    for (final RecordStoreSecurityPolicyFields policy : this.policies) {
      policy.deny(fieldNames);
    }
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsMultiple denyAll() {
    for (final RecordStoreSecurityPolicyFields policy : this.policies) {
      policy.denyAll();
    }
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFields setAccessAllowed(final boolean accessAllowed) {
    for (final RecordStoreSecurityPolicyFields policy : this.policies) {
      policy.setAccessAllowed(accessAllowed);
    }
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFields setAllowed(final Collection<String> fieldNames) {
    for (final RecordStoreSecurityPolicyFields policy : this.policies) {
      policy.setAllowed(fieldNames);
    }
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsMultiple setAllowed(final String... fieldNames) {
    for (final RecordStoreSecurityPolicyFields policy : this.policies) {
      policy.setAllowed(fieldNames);
    }
    return this;
  }

  @Override
  public void setRecordDefinition(final RecordDefinition recordDefinition) {
    for (final RecordStoreSecurityPolicyFields policy : this.policies) {
      policy.setRecordDefinition(recordDefinition);
    }
  }

  @Override
  public String toString() {
    return Arrays.asList(this.policies).toString();
  }
}
