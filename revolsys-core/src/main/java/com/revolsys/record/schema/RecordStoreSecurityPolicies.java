package com.revolsys.record.schema;

public class RecordStoreSecurityPolicies {
  private final RecordStoreSecurityPolicyFieldsSet insertPolicies = new RecordStoreSecurityPolicyFieldsSet(
    "Insert");

  private String label;

  private final RecordStoreSecurityPolicyFieldsSet updatePolicies = new RecordStoreSecurityPolicyFieldsSet(
    "Update");

  private final RecordStoreSecurityPolicyFieldsSet deletePolicies = new RecordStoreSecurityPolicyFieldsSet(
    "Delete");

  private final RecordStoreSecurityPolicyFieldsSet readPolicies = new RecordStoreSecurityPolicyFieldsSet(
    "Read");

  private final RecordStoreSecurityPolicyFieldsMultiple changePolicies = new RecordStoreSecurityPolicyFieldsMultiple(
    this.insertPolicies, this.updatePolicies, this.deletePolicies);

  private final RecordStoreSecurityPolicyFieldsMultiple allPolicies = new RecordStoreSecurityPolicyFieldsMultiple(
    this.readPolicies, this.insertPolicies, this.updatePolicies);

  private RecordDefinition recordDefinition;

  public RecordStoreSecurityPolicies() {
  }

  public RecordStoreSecurityPolicies(final RecordDefinition recordDefinition) {
    this();
    setRecordDefinition(recordDefinition);
  }

  public RecordStoreSecurityPolicies(final String label) {
    this();
    this.label = label;
  }

  @Override
  public RecordStoreSecurityPolicies clone() {
    final RecordStoreSecurityPolicies policy = new RecordStoreSecurityPolicies();
    policy.readPolicies.copyFrom(this.readPolicies);
    policy.insertPolicies.copyFrom(this.insertPolicies);
    policy.updatePolicies.copyFrom(this.updatePolicies);
    policy.deletePolicies.copyFrom(this.deletePolicies);
    return policy;
  }

  public void denyAll() {
    this.allPolicies.denyAll();
  }

  public RecordStoreSecurityPolicyFieldsMultiple getAllPolicies() {
    return this.allPolicies;
  }

  public RecordStoreSecurityPolicyFieldsMultiple getChangePolicies() {
    return this.changePolicies;
  }

  public RecordStoreSecurityPolicyFieldsSet getDeletePolicies() {
    return this.deletePolicies;
  }

  public RecordStoreSecurityPolicyFieldsSet getInsertPolicies() {
    return this.insertPolicies;
  }

  public RecordStoreSecurityPolicyFieldsSet getReadFieldPolicies() {
    return this.readPolicies;
  }

  public RecordStoreSecurityPolicyForField getSecurityPolicy(final RecordAccessType accessType) {
    if (accessType == null) {
      return RecordStoreSecurityPolicyForField.DENY;
    } else {
      switch (accessType) {
        case READ:
          return this.readPolicies;
        case INSERT:
          return this.insertPolicies;
        case UPDATE:
          return this.updatePolicies;
        case DELETE:
          return this.deletePolicies;
        default:
          return RecordStoreSecurityPolicyForField.DENY;
      }
    }
  }

  public RecordStoreSecurityPolicyFieldsSet getUpdatePolicies() {
    return this.updatePolicies;
  }

  public boolean isRecordChangeAllowed(final RecordAccessType accessType) {
    return getSecurityPolicy(accessType).isAccessAllowed();
  }

  public RecordStoreSecurityPolicies setLabel(final String label) {
    this.label = label;
    return this;
  }

  public RecordStoreSecurityPolicies setRecordDefinition(final RecordDefinition recordDefinition) {
    this.recordDefinition = recordDefinition;
    this.allPolicies.setRecordDefinition(recordDefinition);
    this.allPolicies.denyAll(); // Clear existing field policies
    for (final FieldDefinition field : recordDefinition.getFields()) {
      final String fieldName = field.getName();
      if (!field.isGenerated()) {
        this.changePolicies.allow(fieldName);
      }
      this.readPolicies.allow(fieldName);
    }
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
