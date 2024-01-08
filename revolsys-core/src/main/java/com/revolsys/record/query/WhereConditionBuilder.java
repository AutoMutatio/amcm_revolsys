package com.revolsys.record.query;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import com.revolsys.io.PathName;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Property;

public class WhereConditionBuilder implements TableReferenceProxy {

  private TableReference table;

  private Condition condition;

  public WhereConditionBuilder(final TableReference table) {
    this(table, Condition.ALL);
  }

  public WhereConditionBuilder(final TableReference table, final Condition condition) {
    this.table = table;
    this.condition = condition;
  }

  public WhereConditionBuilder and(final ColumnReference left, final Object value) {
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue) value;
      } else {
        right = new Value(left, value);
      }
      condition = new Equal(left, right);
    }
    return and(condition);
  }

  public WhereConditionBuilder and(final Condition condition) {
    if (condition != null && !condition.isEmpty()) {
      final RecordDefinition recordDefinition = getRecordDefinition();
      condition.changeRecordDefinition(recordDefinition, recordDefinition);
      this.condition = this.condition.and(condition);
    }
    return this;
  }

  public WhereConditionBuilder and(final Condition... conditions) {
    if (conditions != null) {
      Condition whereCondition = getWhereCondition();
      for (final Condition condition : conditions) {
        if (Property.hasValue(condition)) {
          whereCondition = whereCondition.and(condition);
        }
      }
      setWhereCondition(whereCondition);
    }
    return this;
  }

  public WhereConditionBuilder and(final Iterable<? extends Condition> conditions) {
    if (conditions != null) {
      Condition whereCondition = getWhereCondition();
      for (final Condition condition : conditions) {
        if (Property.hasValue(condition)) {
          whereCondition = whereCondition.and(condition);
        }
      }
      setWhereCondition(whereCondition);
    }
    return this;
  }

  public WhereConditionBuilder and(final String fieldName,
      final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    final Condition condition = newCondition(fieldName, operator, value);
    return and(condition);
  }

  public WhereConditionBuilder and(final String fieldName,
      final java.util.function.Function<QueryValue, Condition> operator) {
    final Condition condition = newCondition(fieldName, operator);
    return and(condition);
  }

  public WhereConditionBuilder and(final String fieldName, final Object value) {
    final ColumnReference left = this.table.getColumn(fieldName);
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue) value;
      } else {
        right = new Value(left, value);
      }
      condition = new Equal(left, right);
    }
    return and(condition);
  }

  public WhereConditionBuilder and(final TableReferenceProxy table, final String columnName,
      final Object value) {
    final ColumnReference column = table.getColumn(columnName);
    return and(column, value);
  }

  public WhereConditionBuilder andEqualId(final Object id) {
    final RecordDefinition recordDefinition = getRecordDefinition();
    final String idFieldName = recordDefinition.getIdFieldName();
    return and(idFieldName, Q.EQUAL, id);
  }

  /**
   * Create an Or from the conditions and and it to this query;
   *
   * @param conditions
   * @return
   */
  public WhereConditionBuilder andOr(final Condition... conditions) {
    if (conditions != null && conditions.length > 0) {
      final Or or = new Or(conditions);
      if (!or.isEmpty()) {
        and(or);
      }
    }
    return this;
  }

  public Condition build(final Consumer<WhereConditionBuilder> action) {
    action.accept(this);
    return this.condition;
  }

  public Condition build(final Query query, final BiConsumer<Query, WhereConditionBuilder> action) {
    action.accept(query, this);
    return this.condition;
  }

  public Condition equal(final CharSequence fieldName, final Object value) {
    return newCondition(fieldName, Q.EQUAL, value);
  }

  private RecordDefinition getRecordDefinition() {
    if (this.table instanceof final RecordDefinition recordDefinition) {
      return recordDefinition;
    }
    return null;
  }

  public PathName getTablePath() {
    if (this.table == null) {
      return null;
    } else {
      return this.table.getTablePath();
    }
  }

  @Override
  public TableReference getTableReference() {
    return this.table;
  }

  public String getWhere() {
    return this.condition.toFormattedString();
  }

  public Condition getWhereCondition() {
    return this.condition;
  }

  @Override
  public Condition newCondition(final CharSequence fieldName,
      final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    final ColumnReference left = this.table.getColumn(fieldName);
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue) value;
      } else {
        right = new Value(left, value);
      }
      condition = operator.apply(left, right);
    }
    return condition;
  }

  @Override
  public Condition newCondition(final CharSequence fieldName,
      final java.util.function.Function<QueryValue, Condition> operator) {
    final ColumnReference column = this.table.getColumn(fieldName);
    final Condition condition = operator.apply(column);
    return condition;
  }

  @Override
  public Condition newCondition(final QueryValue left,
      final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue) value;
      } else {
        if (left instanceof ColumnReference) {
          right = new Value((ColumnReference) left, value);
        } else {
          right = Value.newValue(value);
        }
      }
      condition = operator.apply(left, right);
    }
    return condition;
  }

  public <QV extends QueryValue> QV newQueryValue(final CharSequence fieldName,
      final BiFunction<QueryValue, QueryValue, QV> operator, final Object value) {
    final ColumnReference left = this.table.getColumn(fieldName);
    QueryValue right;
    if (value instanceof QueryValue) {
      right = (QueryValue) value;
    } else {
      right = new Value(left, value);
    }
    return operator.apply(left, right);
  }

  public <QV extends QueryValue> QV newQueryValue(final CharSequence fieldName,
      final java.util.function.Function<QueryValue, QV> operator) {
    final ColumnReference column = this.table.getColumn(fieldName);
    return operator.apply(column);
  }

  public WhereConditionBuilder or(final CharSequence fieldName,
      final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    final ColumnReference left = this.table.getColumn(fieldName);
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue) value;
      } else {
        right = Value.newValue(value);
      }
      condition = operator.apply(left, right);
    }
    return or(condition);
  }

  public WhereConditionBuilder or(final CharSequence fieldName,
      final java.util.function.Function<QueryValue, Condition> operator) {
    final Condition condition = newCondition(fieldName, operator);
    return or(condition);
  }

  public WhereConditionBuilder or(final Condition condition) {
    final Condition whereCondition = getWhereCondition();
    if (whereCondition.isEmpty()) {
      setWhereCondition(condition);
    } else if (whereCondition instanceof Or) {
      final Or or = (Or) whereCondition;
      or.or(condition);
    } else {
      setWhereCondition(new Or(whereCondition, condition));
    }
    return this;
  }

  public void or(final Condition... conditions) {
    and(Q.or(conditions));
  }

  public void setTable(final TableReference table) {
    this.table = table;
  }

  public WhereConditionBuilder setWhere(final String where) {
    final RecordDefinition recordDefinition = getRecordDefinition();
    final Condition whereCondition = QueryValue.parseWhere(recordDefinition, where);
    return setWhereCondition(whereCondition);
  }

  public WhereConditionBuilder setWhereCondition(final Condition whereCondition) {
    if (whereCondition == null || whereCondition instanceof NoCondition) {
      this.condition = Condition.ALL;
    } else {
      this.condition = whereCondition;
      final RecordDefinition recordDefinition = getRecordDefinition();
      if (recordDefinition != null) {
        whereCondition.changeRecordDefinition(recordDefinition, recordDefinition);
      }
    }
    return this;
  }

  @Override
  public String toString() {
    return this.condition.toString();
  }

}
