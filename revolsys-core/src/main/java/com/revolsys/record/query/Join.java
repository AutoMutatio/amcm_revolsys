package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.function.Consumer;

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
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append(' ');
    sql.append(this.joinType.toString());
    sql.append(' ');
    if (this.table != null) {
      if (this.alias == null) {
        statement.appendFromWithAlias(sql, this.table);
      } else {
        statement.appendFromWithAlias(sql, new FromAlias(this.table, this.alias));
      }
    } else if (this.tableName != null) {
      sql.append(this.tableName);
    }
    if (this.statement != null) {
      this.statement.appendDefaultSelect(statement, recordStore, sql);
      if (this.alias != null) {
        sql.append(" ");
        sql.append('"');
        sql.append(this.alias);
        sql.append('"');
      }
    }
    if (!this.condition.isEmpty()) {
      sql.append(" ON ");
      this.condition.appendSql(statement, recordStore, sql);
    }
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return this.condition.appendParameters(index, statement);
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
    } else {
      final var column = this.table.getColumn(name);
      if (column != null) {
        return new ColumnWithPrefix(this.alias, column);
      }
    }
    throw new IllegalArgumentException("Column not found: " + this.table.getTableReference()
      .getTablePath() + "." + name);
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

  public QueryValue getStatement() {
    return this.statement;
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

  public JoinType joinType() {
    return this.joinType;
  }

  public Join on(final Consumer<WhereConditionBuilder> action) {
    final WhereConditionBuilder builder = new WhereConditionBuilder(this, this.condition);
    this.condition = builder.build(action);
    return this;
  }

  public Join on(final QueryValue left, final QueryValue right) {
    final Equal condition = new Equal(left, right);
    return and(condition);
  }

  public Join on(final String fromFieldName, final Object value) {
    final var fromField = getColumn(fromFieldName);
    final var condition = Q.equal(fromField, value);
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
    final var fromColumn = getColumn(fromFieldName);
    final var toColumn = toTable.getColumn(toFieldName);
    return on(fromColumn, toColumn);
  }

  public Join or(final Condition condition) {
    this.condition = this.condition.or(condition);
    return this;
  }

  public Join recordDefinition(final RecordDefinitionProxy recordDefinition) {
    return table(recordDefinition.getRecordDefinition());
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
    return table(tableFactory.getRecordDefinition(pathName));
  }

  public Join table(final TableReference table) {
    this.table = table;
    if (table != null && this.alias == null) {
      this.alias = table.getTableAlias();
    }
    return this;
  }

  public Join table(final TableReferenceProxy table) {
    return table(table.getTableReference());
  }

  public Join tableName(final String tableName) {
    this.tableName = tableName;
    return this;
  }

  @Override
  public String toString() {
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder();
    sql.append(' ');
    sql.append(this.joinType);
    sql.append(' ');
    if (this.table != null) {
      new Query().appendFromWithAlias(sql, this.table);
    }
    if (this.statement != null) {
      this.statement.appendSql(null, null, sql);
    }
    if (!this.condition.isEmpty()) {
      sql.append(" ON ");
      sql.append(this.condition);
    }
    return sql.toString();
  }

}
