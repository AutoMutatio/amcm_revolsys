package com.revolsys.record.query;

import java.util.Arrays;

import com.revolsys.record.RecordDataType;
import com.revolsys.record.schema.AbstractTableRecordStore;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.CaseConverter;
import com.revolsys.util.Strings;

public abstract class BaseJoinBuilder<SELF extends BaseJoinBuilder<SELF>> {

  private boolean readonly = false;

  protected TableReferenceProxy joinTable;

  public BaseJoinBuilder() {
  }

  public SELF addVirtualField(AbstractTableRecordStore recordStore, String name) {
    return addVirtualField(recordStore, name, name);
  }

  /**
   * Add a new virtual field
   * @param recordStore The record store the add the field to.
   * @param virtualFieldName The name of the virtual field to add
   * @param fieldNameFromJoinTable The field name in the join table to use as the field value
   * @return self
   */
  @SuppressWarnings("unchecked")
  public SELF addVirtualField(AbstractTableRecordStore recordStore, String virtualFieldName,
    String fieldNameFromJoinTable) {
    final var column = this.joinTable.getColumn(fieldNameFromJoinTable);
    recordStore.addVirtualField(virtualFieldName, column.getDataType(), true, (query, _, _) -> {
      final var join = getJoin(query);
      return join.getColumn(fieldNameFromJoinTable);
    });
    return (SELF)this;
  }

  public SELF addVirtualFieldTable(AbstractTableRecordStore recordStore) {
    final var virtualFieldName = CaseConverter.toLowerFirstChar(this.joinTable.getTableReference()
      .getTablePath()
      .getName());
    return addVirtualFieldTable(recordStore, virtualFieldName);

  }

  @SuppressWarnings("unchecked")
  public SELF addVirtualFieldTable(AbstractTableRecordStore recordStore, String virtualFieldName) {
    final var recordDefinition = (RecordDefinition)this.joinTable.getTableReference();
    final var dataType = RecordDataType.of(recordDefinition);
    recordStore.addVirtualField(virtualFieldName, dataType, false, (query, _, path) -> {
      final var join = getJoin(query);
      if (path.length > 1) {
        final var subPath = Strings.toString(".", Arrays.copyOfRange(path, 1, path.length));
        return ((AbstractTableRecordStore)joinTable).fieldPathToQueryValue(query, join, subPath);
      } else {
        throw new IllegalArgumentException("Cannot select a table as a field");
      }
    });
    return (SELF)this;
  }

  protected void ensureEditible() {
    if (readonly) {
      throw new IllegalStateException("Readonly");
    }
  }

  public abstract Join getJoin(final Query query);

  public TableReferenceProxy joinTable() {
    return joinTable;
  }

  @SuppressWarnings("unchecked")
  public SELF joinTable(TableReferenceProxy joinTable) {
    ensureEditible();
    this.joinTable = joinTable;
    return (SELF)this;
  }

  @SuppressWarnings("unchecked")
  public SELF makeReadonly() {
    this.readonly = true;
    return (SELF)this;
  }

}
