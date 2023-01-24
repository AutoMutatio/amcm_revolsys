package com.revolsys.record.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.springframework.dao.PermissionDeniedDataAccessException;

public final class RecordStoreSecurityPolicyFieldsSet
  implements RecordStoreSecurityPolicyForField, RecordStoreSecurityPolicyFields {
  private final Set<String> fieldNames = new TreeSet<>();

  private boolean accessAllowed = true;

  private final String label;

  private RecordDefinition recordDefinition;

  private RecordDefinition permissionRecordDefinition;

  public RecordStoreSecurityPolicyFieldsSet(final String label) {
    this.label = label;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsSet allow(final String fieldName) {
    this.permissionRecordDefinition = null;
    this.fieldNames.add(fieldName);
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsSet allow(final String... fieldNames) {
    this.permissionRecordDefinition = null;
    for (final String fieldName : fieldNames) {
      this.fieldNames.add(fieldName);
    }
    return this;
  }

  public void copyFrom(final RecordStoreSecurityPolicyFieldsSet policy) {
    this.accessAllowed = policy.accessAllowed;
    setAllowed(policy.fieldNames);
  }

  @Override
  public RecordStoreSecurityPolicyFieldsSet deny(final String fieldName) {
    this.permissionRecordDefinition = null;
    this.fieldNames.remove(fieldName);
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsSet deny(final String... fieldNames) {
    this.permissionRecordDefinition = null;
    for (final String fieldName : fieldNames) {
      this.fieldNames.remove(fieldName);
    }
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsSet denyAll() {
    this.permissionRecordDefinition = null;
    this.fieldNames.clear();
    return this;
  }

  @Override
  public void enforceAccessAllowed() {
    if (!this.accessAllowed) {
      throw new PermissionDeniedDataAccessException("No " + this.label + " Permission", null);
    }
  }

  public Collection<String> getAllowedFieldNames() {
    return new ArrayList<>(this.fieldNames);
  }

  public RecordDefinition getPermissionRecordDefinition() {
    if (this.permissionRecordDefinition == null) {
      this.permissionRecordDefinition = new RecordDefinitionBuilder(this.recordDefinition,
        this.fieldNames).getRecordDefinition();
    }
    return this.permissionRecordDefinition;
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    return this.recordDefinition.cloneFields(this.fieldNames);
  }

  @Override
  public boolean isAccessAllowed() {
    return this.accessAllowed;
  }

  @Override
  public boolean isFieldAllowed(final String fieldName) {
    return this.accessAllowed && this.fieldNames.contains(fieldName);
  }

  @Override
  public RecordStoreSecurityPolicyFieldsSet setAccessAllowed(final boolean accessAllowed) {
    this.accessAllowed = accessAllowed;
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsSet setAllowed(final Collection<String> fieldNames) {
    this.permissionRecordDefinition = null;
    this.fieldNames.clear();
    for (final String fieldName : fieldNames) {
      this.fieldNames.add(fieldName);
    }
    return this;
  }

  @Override
  public RecordStoreSecurityPolicyFieldsSet setAllowed(final String... fieldNames) {
    this.permissionRecordDefinition = null;
    this.fieldNames.clear();
    for (final String fieldName : fieldNames) {
      this.fieldNames.add(fieldName);
    }
    return this;
  }

  @Override
  public void setRecordDefinition(final RecordDefinition recordDefinition) {
    this.recordDefinition = recordDefinition;
    this.permissionRecordDefinition = null;
  }

  @Override
  public String toString() {
    return this.label + ": " + this.fieldNames.toString();
  }
}
