package com.revolsys.record.schema;

import java.util.LinkedHashSet;
import java.util.Set;

public class FieldNameSet {
  private Set<String> fieldNames;

  private Set<String> ignoreFieldNames;

  private RecordDefinition recordDefinition;

  public FieldNameSet addFieldName(final String fieldName) {
    ensureAdd();
    this.fieldNames.add(fieldName);
    return this;
  }

  public FieldNameSet addFieldNames(final String... fieldNames) {
    ensureAdd();
    for (final String fieldName : fieldNames) {
      this.fieldNames.add(fieldName);
    }
    return this;
  }

  public boolean contains(final String fieldName) {
    if (this.ignoreFieldNames != null && this.ignoreFieldNames.contains(fieldName)) {
      return false;
    }
    if (this.fieldNames != null) {
      return this.fieldNames.contains(fieldName);
    }
    if (this.recordDefinition != null) {
      return this.recordDefinition.hasField(fieldName);
    }
    return false;
  }

  private void ensureAdd() {
    if (this.fieldNames == null) {
      this.fieldNames = new LinkedHashSet<>();
    }
  }

  private void ensureIgnore() {
    if (this.ignoreFieldNames == null) {
      this.ignoreFieldNames = new LinkedHashSet<>();
    }
  }

  public FieldNameSet ignoreFieldName(final String fieldName) {
    ensureIgnore();
    this.ignoreFieldNames.add(fieldName);
    return this;
  }

  public FieldNameSet ignoreFieldNames(final String... fieldNames) {
    ensureIgnore();
    for (final String fieldName : fieldNames) {
      this.ignoreFieldNames.add(fieldName);
    }
    return this;
  }

  public void setRecordDefinition(final RecordDefinition recordDefinition) {
    this.recordDefinition = recordDefinition;
  }
}
