package com.revolsys.record.schema;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.dao.PermissionDeniedDataAccessException;

public class RecordStoreSecurityPolicy {
  private final Set<String> insertFieldNames = new LinkedHashSet<>();

  private String label;

  private final Set<String> updateFieldNames = new LinkedHashSet<>();

  private boolean recordInsertAllowed = true;

  private boolean recordReadAllowed = true;

  private boolean recordDeleteAllowed = true;

  private boolean recordUpdateAllowed = true;

  private final RecordFieldSecurityPolicy insertFieldPolicy = this::canInsertField;

  private final RecordFieldSecurityPolicy updateFieldPolicy = this::canUpdateField;

  private RecordDefinition recordDefinition;

  public RecordStoreSecurityPolicy() {
  }

  public RecordStoreSecurityPolicy(final RecordDefinition recordDefinition) {
    this();
    setRecordDefinition(recordDefinition);
  }

  public RecordStoreSecurityPolicy(final String label) {
    this();
    this.label = label;
  }

  public RecordStoreSecurityPolicy allowFieldChange(final String fieldName) {
    allowFieldInsert(fieldName);
    allowFieldUpdate(fieldName);
    return this;
  }

  public RecordStoreSecurityPolicy allowFieldInsert(final String fieldName) {
    this.insertFieldNames.add(fieldName);
    return this;
  }

  public RecordStoreSecurityPolicy allowFields(final Collection<String> fieldNames) {
    allowFieldsInsert(fieldNames);
    allowFieldsUpdate(fieldNames);
    return this;
  }

  public RecordStoreSecurityPolicy allowFields(final String... fieldNames) {
    allowFieldsInsert(fieldNames);
    allowFieldsUpdate(fieldNames);
    return this;
  }

  public RecordStoreSecurityPolicy allowFieldsInsert(final Collection<String> fieldNames) {
    for (final String fieldName : fieldNames) {
      allowFieldInsert(fieldName);
    }
    return this;
  }

  public RecordStoreSecurityPolicy allowFieldsInsert(final String... fieldNames) {
    for (final String fieldName : fieldNames) {
      allowFieldInsert(fieldName);
    }
    return this;
  }

  public RecordStoreSecurityPolicy allowFieldsUpdate(final Collection<String> fieldNames) {
    for (final String fieldName : fieldNames) {
      allowFieldUpdate(fieldName);
    }
    return this;
  }

  public RecordStoreSecurityPolicy allowFieldsUpdate(final String... fieldNames) {
    for (final String fieldName : fieldNames) {
      allowFieldUpdate(fieldName);
    }
    return this;
  }

  public RecordStoreSecurityPolicy allowFieldUpdate(final String fieldName) {
    this.updateFieldNames.add(fieldName);
    return this;
  }

  public RecordStoreSecurityPolicy allowRecordDelete() {
    this.recordDeleteAllowed = true;
    return this;
  }

  public RecordStoreSecurityPolicy allowRecordInsert() {
    this.recordInsertAllowed = true;
    return this;
  }

  public RecordStoreSecurityPolicy allowRecordRead() {
    this.recordReadAllowed = true;
    return this;
  }

  public RecordStoreSecurityPolicy allowRecordUpdate() {
    this.recordUpdateAllowed = true;
    return this;
  }

  public boolean canInsertField(final String fieldName) {
    return this.recordInsertAllowed && this.insertFieldNames.contains(fieldName);
  }

  public boolean canUpdateField(final String fieldName) {
    return this.recordUpdateAllowed && this.updateFieldNames.contains(fieldName);
  }

  @Override
  public RecordStoreSecurityPolicy clone() {
    final RecordStoreSecurityPolicy policy = new RecordStoreSecurityPolicy();
    policy.setAllowFieldsInsert(this.insertFieldNames);
    policy.setAllowFieldsUpdate(this.updateFieldNames);
    return policy;
  }

  public RecordStoreSecurityPolicy denyField(final String fieldName) {
    denyFieldInsert(fieldName);
    denyFieldUpdate(fieldName);
    return this;
  }

  public RecordStoreSecurityPolicy denyFieldInsert(final String fieldName) {
    this.insertFieldNames.remove(fieldName);
    return this;
  }

  public RecordStoreSecurityPolicy denyFields(final String... fieldNames) {
    for (final String fieldName : fieldNames) {
      denyField(fieldName);
    }
    return this;
  }

  public RecordStoreSecurityPolicy denyFieldsInsert(final String... fieldNames) {
    for (final String fieldName : fieldNames) {
      denyFieldInsert(fieldName);
    }
    return this;
  }

  public RecordStoreSecurityPolicy denyFieldsUpdate(final String... fieldNames) {
    for (final String fieldName : fieldNames) {
      denyFieldUpdate(fieldName);
    }
    return this;
  }

  public RecordStoreSecurityPolicy denyFieldUpdate(final String fieldName) {
    this.updateFieldNames.remove(fieldName);
    return this;
  }

  public RecordStoreSecurityPolicy denyRecordChange() {
    this.recordDeleteAllowed = false;
    this.recordInsertAllowed = false;
    this.recordUpdateAllowed = false;
    return this;
  }

  public RecordStoreSecurityPolicy denyRecordDelete() {
    this.recordDeleteAllowed = false;
    return this;
  }

  public RecordStoreSecurityPolicy denyRecordInsert() {
    this.recordInsertAllowed = true;
    return this;
  }

  public RecordStoreSecurityPolicy denyRecordRead() {
    this.recordReadAllowed = false;
    return this;
  }

  public RecordStoreSecurityPolicy denyRecordUpdate() {
    this.recordUpdateAllowed = true;
    return this;
  }

  public void enforceRecordDelete() {
    if (!this.recordDeleteAllowed) {
      throw new PermissionDeniedDataAccessException(
        "No Delete Permission on " + this.recordDefinition, null);
    }
  }

  public void enforceRecordInsert() {
    if (!this.recordInsertAllowed) {
      throw new PermissionDeniedDataAccessException(
        "No Insert Permission on " + this.recordDefinition, null);
    }
  }

  public void enforceRecordRead() {
    if (!this.recordReadAllowed) {
      throw new PermissionDeniedDataAccessException(
        "No Read Permission on " + this.recordDefinition, null);
    }
  }

  public void enforceRecordUpdate() {
    if (!this.recordUpdateAllowed) {
      throw new PermissionDeniedDataAccessException(
        "No Update Permission on " + this.recordDefinition, null);
    }
  }

  public RecordFieldSecurityPolicy getFieldPolicy(final RecordAccessType accessType) {
    if (accessType == null) {
      return RecordFieldSecurityPolicy.DENY;
    } else {
      switch (accessType) {
        case INSERT:
          return this.insertFieldPolicy;
        case UPDATE:
          return this.updateFieldPolicy;
        default:
          return RecordFieldSecurityPolicy.DENY;
      }
    }
  }

  public RecordFieldSecurityPolicy getInsertFieldPolicy() {
    return this.insertFieldPolicy;
  }

  public RecordFieldSecurityPolicy getUpdateFieldPolicy() {
    return this.updateFieldPolicy;
  }

  public boolean isRecordChangeAllowed(final RecordAccessType accessType) {
    switch (accessType) {
      case INSERT:
        return this.recordInsertAllowed;
      case UPDATE:
        return this.recordUpdateAllowed;
      case DELETE:
        return this.recordDeleteAllowed;
      case READ:
        return this.recordReadAllowed;
      default:
        return false;
    }
  }

  public boolean isRecordDeleteAllowed() {
    return this.recordDeleteAllowed;
  }

  public boolean isRecordInsertAllowed() {
    return this.recordInsertAllowed;
  }

  public boolean isRecordReadAllowed() {
    return this.recordReadAllowed;
  }

  public boolean isRecordUpdateAllowed() {
    return this.recordUpdateAllowed;
  }

  public RecordStoreSecurityPolicy setAllowFields(final Collection<String> fieldNames) {
    setAllowFieldsInsert(fieldNames);
    setAllowFieldsUpdate(fieldNames);
    return this;
  }

  public RecordStoreSecurityPolicy setAllowFields(final String... fieldNames) {
    setAllowFieldsInsert(fieldNames);
    setAllowFieldsUpdate(fieldNames);
    return this;
  }

  public RecordStoreSecurityPolicy setAllowFieldsInsert(final Collection<String> fieldNames) {
    this.insertFieldNames.clear();
    return allowFields(fieldNames);
  }

  public RecordStoreSecurityPolicy setAllowFieldsInsert(final String... fieldNames) {
    this.insertFieldNames.clear();
    return allowFields(fieldNames);
  }

  public RecordStoreSecurityPolicy setAllowFieldsUpdate(final Collection<String> fieldNames) {
    this.updateFieldNames.clear();
    return allowFieldsUpdate(fieldNames);
  }

  public RecordStoreSecurityPolicy setAllowFieldsUpdate(final String... fieldNames) {
    this.updateFieldNames.clear();
    return allowFieldsUpdate(fieldNames);
  }

  public RecordStoreSecurityPolicy setLabel(final String label) {
    this.label = label;
    return this;
  }

  public RecordStoreSecurityPolicy setRecordDefinition(final RecordDefinition recordDefinition) {
    this.recordDefinition = recordDefinition;
    final List<String> fieldNames = recordDefinition.getFieldNames();
    setAllowFields(fieldNames);
    return this;
  }

  @Override
  public String toString() {
    if (this.label == null) {
      return this.recordDefinition.toString();
    }
    return this.label.toString() + "\t" + this.recordDefinition;
  }
}
