package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.revolsys.data.type.DataType;
import com.revolsys.record.schema.RecordStore;

public abstract class AbstractBinaryQueryValue implements QueryValue {

  private QueryValue left;

  private QueryValue right;

  public AbstractBinaryQueryValue(final List<QueryValue> parameters) {
    final int parameterCount = parameters.size();
    if (parameterCount == 2) {
      final QueryValue left = parameters.get(0);
      final QueryValue right = parameters.get(0);
      init(left, right);
    } else {
      throw new IllegalArgumentException(
        getClass() + "  requires 2 arguments not " + parameterCount + ": " + parameters);
    }
  }

  public AbstractBinaryQueryValue(final QueryValue left, final QueryValue right) {
    init(left, right);
  }

  protected void appendLeft(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    if (this.left == null) {
      sql.append("NULL");
    } else {
      this.left.appendSql(statement, recordStore, sql);
    }
    sql.append(" ");
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    if (this.left != null) {
      if (this.right instanceof final ColumnReference column
        && !(this.left instanceof ColumnReference)) {
        this.left.setColumn(column);
      }
      index = this.left.appendParameters(index, statement);
    }
    if (this.right != null) {
      if (this.left instanceof final ColumnReference column
        && !(this.right instanceof ColumnReference)) {
        this.right.setColumn(column);
      }
      index = this.right.appendParameters(index, statement);
    }
    return index;
  }

  protected void appendRight(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append(" ");
    if (this.right == null) {
      sql.append("NULL");
    } else {
      this.right.appendSql(statement, recordStore, sql);
    }
  }

  @Override
  public AbstractBinaryQueryValue clone() {
    try {
      return (AbstractBinaryQueryValue)super.clone();
    } catch (final CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public AbstractBinaryQueryValue clone(final TableReference oldTable,
    final TableReference newTable) {
    final AbstractBinaryQueryValue clone = clone();
    QueryValue left = this.left.clone(oldTable, newTable);
    QueryValue right = this.right.clone(oldTable, newTable);
    if (left instanceof ColumnReference && right instanceof Value) {
      final ColumnReference column = (ColumnReference)left;
      final Object rightValue = ((Value)right).getValue();
      right = new Value(column, rightValue);
    } else if (left instanceof Value && right instanceof ColumnReference) {
      final ColumnReference column = (ColumnReference)right;
      final Object leftValue = ((Value)left).getValue();
      left = new Value(column, leftValue);
    }
    clone.left = left;
    clone.right = right;
    return clone;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof AbstractBinaryQueryValue) {
      final AbstractBinaryQueryValue binary = (AbstractBinaryQueryValue)obj;
      if (DataType.equal(binary.getLeft(), this.getLeft())) {
        if (DataType.equal(binary.getRight(), this.getRight())) {
          return true;
        }
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public <V extends QueryValue> V getLeft() {
    return (V)this.left;
  }

  @Override
  public List<QueryValue> getQueryValues() {
    return Arrays.asList(this.left, this.right);
  }

  @SuppressWarnings("unchecked")
  public <V extends QueryValue> V getRight() {
    return (V)this.right;
  }

  private void init(final QueryValue left, final QueryValue right) {
    this.left = left;
    this.right = right;
    if (left != null && right instanceof final Value value) {
      final var column = left.getColumn();
      if (column != null) {
        value.setColumn(column);
      }
    }
  }

  public void setLeft(final QueryValue left) {
    this.left = left;
  }

  public void setRight(final QueryValue right) {
    this.right = right;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <QV extends QueryValue> QV updateQueryValues(final TableReference oldTable,
    final TableReference newTable, final Function<QueryValue, QueryValue> valueHandler) {
    final QueryValue left = valueHandler.apply(this.left.clone(oldTable, newTable));
    final QueryValue right = valueHandler.apply(this.right.clone(oldTable, newTable));
    if (left == this.left && right == this.right) {
      return (QV)this;
    } else {
      final AbstractBinaryQueryValue clone = clone(oldTable, newTable);
      clone.left = left;
      clone.right = right;
      return (QV)clone;
    }
  }
}
