package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.List;

import com.revolsys.data.type.DataType;
import com.revolsys.record.schema.RecordStore;

public abstract class AbstractMultiCondition extends AbstractMultiQueryValue
  implements Condition, ConditionBuilder {

  private final String operator;

  public AbstractMultiCondition(final String operator,
    final Iterable<? extends Condition> conditions) {
    super(conditions);
    this.operator = operator;
  }

  @Override
  public AbstractMultiCondition addCondition(final Condition condition) {
    if (condition != null && !(condition instanceof NoCondition)) {
      addValue(condition);
    }
    return this;
  }

  public void addCondition(final String sql) {
    final SqlCondition value = new SqlCondition(sql);
    addCondition(value);
  }

  public AbstractMultiCondition addConditions(final AbstractMultiCondition condition) {
    if (condition != null) {
      for (final Condition subCondition : condition.getConditions()) {
        addValue(subCondition);
      }
    }
    return this;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append("(");
    boolean first = true;

    for (final QueryValue value : this.values) {
      if (!(value instanceof final Condition condition) || !condition.isEmpty()) {
        if (first) {
          first = false;
        } else {
          buffer.append(" ");
          buffer.append(this.operator);
          buffer.append(" ");
        }
        if (value == null) {
          buffer.append("NULL");
        } else {
          value.appendSql(statement, recordStore, buffer);
        }
      }
    }
    buffer.append(")");
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    for (final QueryValue value : this.values) {
      if (value != null) {
        index = value.appendParameters(index, statement);
      }
    }
    return index;
  }

  @Override
  public AbstractMultiCondition clone() {
    return (AbstractMultiCondition)super.clone();
  }

  @Override
  public AbstractMultiCondition clone(final TableReference oldTable,
    final TableReference newTable) {
    return (AbstractMultiCondition)super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof AbstractMultiCondition) {
      final AbstractMultiCondition multiCondition = (AbstractMultiCondition)obj;
      if (DataType.equal(getOperator(), multiCondition.getOperator())) {
        return super.equals(multiCondition);
      }
    }
    return false;
  }

  @Override
  public Condition getCondition() {
    return this;
  }

  @SuppressWarnings("unchecked")
  public List<Condition> getConditions() {
    return (List)super.getQueryValues();
  }

  public String getOperator() {
    return this.operator;
  }

  public void removeCondition(final Condition condition) {
    removeValue(condition);
  }

  @Override
  public String toString() {
    final StringBuilder string = new StringBuilder();
    boolean first = true;
    for (final QueryValue value : this.values) {
      if (first) {
        first = false;
      } else {
        string.append(' ');
        string.append(this.operator);
        string.append(' ');
      }
      if (value instanceof Or && !(this instanceof Or)) {
        string.append('(');
        string.append(value);
        string.append(')');

      } else {
        string.append(value);
      }
    }
    return string.toString();
  }
}
