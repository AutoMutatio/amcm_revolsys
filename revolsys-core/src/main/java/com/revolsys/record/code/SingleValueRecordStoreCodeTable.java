package com.revolsys.record.code;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.io.PathName;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.record.schema.RecordStore;

public class SingleValueRecordStoreCodeTable
  extends AbstractRecordStoreSingleValueCodeTable<SingleValueRecordStoreCodeTable>
  implements RecordDefinitionProxy {

  private RecordStore recordStore;

  private RecordDefinition recordDefinition;

  private PathName typePath;

  private String creationTimestampFieldName;

  private String modificationTimestampFieldName;

  public SingleValueRecordStoreCodeTable() {
  }

  public SingleValueRecordStoreCodeTable(final Map<String, ? extends Object> config) {
    setProperties(config);
  }

  @Override
  protected int calculateValueFieldLength() {
    return this.recordDefinition.getFieldLength(this.valueFieldName);
  }

  @Override
  public SingleValueRecordStoreCodeTable clone() {
    final SingleValueRecordStoreCodeTable clone = super.clone();
    clone.recordDefinition = null;
    return clone;
  }

  @Override
  protected QueryValue fieldPathToSelect(final Query query, final String idFieldName) {
    return this.recordDefinition.getColumn(idFieldName);
  }

  public String getCreationTimestampFieldName() {
    return this.creationTimestampFieldName;
  }

  @Override
  public String getIdFieldName() {
    var idFieldName = super.getIdFieldName();
    if (idFieldName != null) {
      return idFieldName;
    } else if (this.recordDefinition == null) {
      return "";
    } else {
      idFieldName = this.recordDefinition.getIdFieldName();
      if (idFieldName == null) {
        return this.recordDefinition.getFieldName(0);
      } else {
        return idFieldName;
      }
    }
  }

  public String getModificationTimestampFieldName() {
    return this.modificationTimestampFieldName;
  }

  @Override
  public Record getRecord(final Identifier id) {
    return this.recordStore.getRecord(this.typePath, id);
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    return this.recordDefinition;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R extends RecordStore> R getRecordStore() {
    return (R)this.recordStore;
  }

  public String getTypeName() {
    return this.typePath.getPath();
  }

  public PathName getTypePath() {
    return this.typePath;
  }

  @Override
  protected Identifier newIdentifier(final Object value) {
    if (isCreateMissingCodes()) {
      // TODO prevent duplicates from other threads/processes
      final Record code = this.recordStore.newRecord(this.typePath);
      final RecordDefinition recordDefinition = code.getRecordDefinition();
      Identifier id = this.recordStore.newPrimaryIdentifier(this.typePath);
      if (id == null) {
        final FieldDefinition idField = recordDefinition.getIdField();
        if (idField != null) {
          if (Number.class.isAssignableFrom(idField.getDataType()
            .getJavaClass())) {
            id = Identifier.newIdentifier(getNextId());
          } else {
            id = Identifier.newIdentifier(UUID.randomUUID()
              .toString());
          }
        }
      }
      code.setIdentifier(id);
      code.setValue(this.valueFieldName, value);

      final Instant now = Instant.now();
      if (this.creationTimestampFieldName != null) {
        code.setValue(this.creationTimestampFieldName, now);
      }
      if (this.modificationTimestampFieldName != null) {
        code.setValue(this.modificationTimestampFieldName, now);
      }

      this.recordStore.insertRecord(code);
      return code.getIdentifier();
    } else {
      return null;
    }
  }

  @Override
  protected Query newQueryDo() {
    return this.recordStore//
      .newQuery(this.typePath);
  }

  @Override
  protected Record recordByIdDo(final Object id) {
    final Record code;
    if (id instanceof Identifier) {
      final Identifier identifier = (Identifier)id;
      code = this.recordStore.getRecord(this.typePath, identifier);
    } else {
      code = this.recordStore.getRecord(this.typePath, id);
    }
    return code;
  }

  public SingleValueRecordStoreCodeTable setCreationTimestampFieldName(
    final String creationTimestampFieldName) {
    this.creationTimestampFieldName = creationTimestampFieldName;
    return self();
  }

  public SingleValueRecordStoreCodeTable setModificationTimestampFieldName(
    final String modificationTimestampFieldName) {
    this.modificationTimestampFieldName = modificationTimestampFieldName;
    return self();
  }

  public void setRecordDefinition(final RecordDefinition recordDefinition) {
    if (this.recordDefinition != recordDefinition) {
      if (this.recordDefinition != null) {
        setRecordDefinitionBefore(this.recordDefinition);
      }
      this.recordDefinition = recordDefinition;
      if (recordDefinition == null) {
        this.recordStore = null;
        this.typePath = null;
      } else {
        this.typePath = recordDefinition.getPathName();
        final String name = this.typePath.getName();
        setName(name);
        if (this.idFieldName == null) {
          this.idFieldName = recordDefinition.getIdFieldName();
        }
        this.recordStore = this.recordDefinition.getRecordStore();
        setValueFieldDefinition(recordDefinition.getField(this.valueFieldName));
        setRecordDefinitionAfter(recordDefinition);
      }
    }
  }

  @Override
  public String toString() {
    return this.typePath + " " + getIdFieldName() + " " + this.valueFieldName;

  }

}
