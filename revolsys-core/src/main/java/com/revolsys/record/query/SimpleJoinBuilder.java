package com.revolsys.record.query;

import java.util.Arrays;

import com.revolsys.record.RecordDataType;
import com.revolsys.record.schema.AbstractTableRecordStore;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.CaseConverter;
import com.revolsys.util.Strings;

public class SimpleJoinBuilder {

  private String fromJoinFieldName;

  private String toJoinFieldName;

  private TableReferenceProxy joinTable;

  private String joinAlias;

  private boolean readonly = false;

  public SimpleJoinBuilder addVirtualField(AbstractTableRecordStore recordStore, String name) {
    return addVirtualField(recordStore, name, name);
  }

  /**
   * Add a new virtual field
   * @param recordStore The record store the add the field to.
   * @param virtualFieldName The name of the virtual field to add
   * @param fieldNameFromJoinTable The field name in the join table to use as the field value
   * @return self
   */
  public SimpleJoinBuilder addVirtualField(AbstractTableRecordStore recordStore,
    String virtualFieldName, String fieldNameFromJoinTable) {
    final var column = this.joinTable.getColumn(fieldNameFromJoinTable);
    recordStore.addVirtualField(virtualFieldName, column.getDataType(), true, (query, _, _) -> {
      final var join = getJoin(query);
      return join.getColumn(fieldNameFromJoinTable);
    });
    return this;
  }

  public SimpleJoinBuilder addVirtualFieldTable(AbstractTableRecordStore recordStore) {
    final var virtualFieldName = CaseConverter
      .toLowerFirstChar(this.joinTable.getTableReference().getTablePath().getName());
    return addVirtualFieldTable(recordStore, virtualFieldName);

  }

  public SimpleJoinBuilder addVirtualFieldTable(AbstractTableRecordStore recordStore,
    String virtualFieldName) {
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
    return this;
  }

  private void ensureEditible() {
    if (readonly) {
      throw new IllegalStateException("Readonly");
    }
  }

  public String fromJoinFieldName() {
    return fromJoinFieldName;
  }

  public SimpleJoinBuilder fromJoinFieldName(String fromJoinFieldName) {
    ensureEditible();
    this.fromJoinFieldName = fromJoinFieldName;
    return this;
  }

  public Join getJoin(final Query query) {
    var join = query.getJoin(joinTable, joinAlias);
    if (join == null) {
      join = query.join(JoinType.LEFT_OUTER_JOIN)
        .table(joinTable)//
        .setAlias(joinAlias);
      final var fromJoinColumn = query.getColumn(fromJoinFieldName);
      final var toJoinColumn = join.getColumn(toJoinFieldName);
      join.on(fromJoinColumn, toJoinColumn);
    }
    return join;
  }

  public TableReferenceProxy getJoinTable() {
    return joinTable;
  }

  public String joinAlias() {
    return joinAlias;
  }

  public SimpleJoinBuilder joinAlias(String joinAlias) {
    ensureEditible();
    this.joinAlias = joinAlias;
    return this;
  }

  public SimpleJoinBuilder joinTable(TableReferenceProxy joinTable) {
    ensureEditible();
    this.joinTable = joinTable;
    return this;
  }

  public SimpleJoinBuilder makeReadonly() {
    this.readonly = true;
    return this;
  }

  public String toJoinFieldName() {
    return toJoinFieldName;
  }

  public SimpleJoinBuilder toJoinFieldName(String toJoinFieldName) {
    ensureEditible();
    this.toJoinFieldName = toJoinFieldName;
    return this;
  }
}
