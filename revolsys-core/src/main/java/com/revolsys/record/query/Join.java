package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.collection.map.MapEx;
import com.revolsys.exception.Exceptions;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.record.schema.TableRecordStoreFactory;
import com.revolsys.record.schema.TableReferenceFactory;

public class Join implements QueryValue, TableReferenceProxy {

  private final JoinType joinType;

  private String tableName;

  private TableReference table;

  private QueryValue statement;

  private Condition condition = Condition.ALL;

  private String alias;

  public Join(final JoinType joinType) {
    this.joinType = joinType;
  }

  public Join and(final Condition condition) {
    this.condition = this.condition.and(condition);
    return this;
  }

  @Override
  public void appendDefaultSql(final Query query, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append(' ');
    sql.append(this.joinType.toString());
    sql.append(' ');
    if (this.table != null) {
      if (this.alias == null) {
        this.table.appendFromWithAlias(sql);
      } else {
        this.table.appendFromWithAlias(sql, this.alias);
      }
    } else if (this.tableName != null) {
      sql.append(this.tableName);
    }
    if (this.statement != null) {
      this.statement.appendDefaultSelect(query, recordStore, sql);
      if (this.alias != null) {
        sql.append(" ");
        sql.append('"');
        sql.append(this.alias);
        sql.append('"');
      }
    }
    if (!this.condition.isEmpty()) {
      sql.append(" ON ");
      this.condition.appendSql(query, recordStore, sql);
    }
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return this.condition.appendParameters(index, statement);
  }

  private void appendSql(final SqlAppendable sql) {
    sql.append(' ');
    sql.append(this.joinType);
    sql.append(' ');
    if (this.table != null) {
      this.table.appendFromWithAlias(sql);
    }
    if (this.statement != null) {
      this.statement.appendSql(null, null, sql);
    }
    if (!this.condition.isEmpty()) {
      sql.append(" ON ");
      sql.append(this.condition);
    }
  }

  @Override
  public Join clone() {
    try {
      final Join join = (Join)super.clone();
      join.condition = this.condition.clone();
      return join;
    } catch (final CloneNotSupportedException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @Override
  public Join clone(final TableReference oldTable, final TableReference newTable) {
    final Join join = clone();
    join.condition = this.condition.clone(oldTable, newTable);
    return join;
  }

  public Join condition(final Condition condition) {
    if (condition == null) {
      this.condition = Condition.ALL;
    } else {
      this.condition = condition;
    }
    return this;
  }

  @Override
  public ColumnReference getColumn(final CharSequence name) {
    if (this.table == null) {
      return new ColumnWithPrefix(this.tableName, new Column(this, name));
    } else if (this.table.hasColumn(name)) {
      return new Column(this, name);
    }
    throw new IllegalArgumentException(
      "Column not found: " + this.table.getTableReference().getTablePath() + "." + name);
  }

  public Condition getCondition() {
    return this.condition;
  }

  @Override
  public FieldDefinition getField(final CharSequence name) {
    if (this.table == null) {
      return null;
    } else {
      return this.table.getField(name);
    }
  }

  public TableReference getTable() {
    return this.table;
  }

  @Override
  public String getTableAlias() {
    if (this.alias == null) {
      return this.table.getTableAlias();
    } else {
      return this.alias;
    }
  }

  @Override
  public TableReference getTableReference() {
    return this.table;
  }

  @Override
  public <V> V getValue(final MapEx record) {
    return null;
  }

  public Join on(final String fromFieldName, final Object value) {
    final Condition condition = this.table.equal(fromFieldName, value);
    return and(condition);
  }

  public Join on(final String fieldName, final Query query) {
    final TableReference toTable = query.getTable();
    return on(fieldName, toTable);
  }

  public Join on(final String fromFieldName, final Query query, final String toFieldName) {
    final TableReference toTable = query.getTable();
    return on(fromFieldName, toTable, toFieldName);
  }

  public Join on(final String fromFieldName, final TableReferenceFactory factory,
    final CharSequence tableName, final String toFieldName) {
    final TableReference recordDefinition = factory.getTableReference(tableName);
    return on(fromFieldName, recordDefinition, toFieldName);
  }

  public Join on(final String fieldName, final TableReferenceProxy toTable) {
    return on(fieldName, toTable, fieldName);
  }

  public Join on(final String fromFieldName, final TableReferenceProxy toTable,
    final String toFieldName) {
    final ColumnReference fromColumn = getColumn(fromFieldName);
    final ColumnReference toColumn = toTable.getColumn(toFieldName);
    final Equal condition = new Equal(fromColumn, toColumn);
    return and(condition);
  }

  public Join or(final Condition condition) {
    this.condition = this.condition.or(condition);
    return this;
  }

  public Join recordDefinition(final RecordDefinitionProxy recordDefinition) {
    this.table = recordDefinition.getRecordDefinition();
    return this;
  }

  public Join setAlias(final String alias) {
    this.alias = alias;
    return this;
  }

  public Join statement(final QueryValue statement) {
    this.statement = statement;
    return this;
  }

  public Join table(final TableRecordStoreFactory tableFactory, final CharSequence pathName) {
    this.table = tableFactory.getRecordDefinition(pathName);
    return this;
  }

  public Join table(final TableReferenceProxy table) {
    this.table = table.getTableReference();
    return this;
  }

  public Join tableName(final String tableName) {
    this.tableName = tableName;
    return this;
  }

  public String toSql() {
    final StringBuilderSqlAppendable string = SqlAppendable.stringBuilder();
    appendSql(string);
    return string.toString();
  }

  @Override
  public String toString() {
    return toSql();
  }

}
