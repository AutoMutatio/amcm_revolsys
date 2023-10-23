package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.exception.Exceptions;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.field.JdbcFieldDefinitions;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.util.Strings;

public class CollectionValue extends AbstractMultiQueryValue {
  private FieldDefinition field;

  private JdbcFieldDefinition jdbcField;

  private CodeTable codeTable;

  public CollectionValue(final Collection<? extends Object> values) {
    this(null, values);
  }

  public CollectionValue(final ColumnReference field) {
    setColumn(field);
  }

  public CollectionValue(final ColumnReference field, final Collection<? extends Object> values) {
    this(field);
    for (final Object value : values) {
      QueryValue queryValue;
      if (value instanceof QueryValue) {
        queryValue = (QueryValue)value;
      } else {
        queryValue = Value.newValue(value);
      }
      addValue(queryValue);
    }
  }

  @Override
  public boolean addValue(final QueryValue value) {
    return super.addValue(value);
  }

  @Override
  public void appendDefaultSql(final Query query, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append('(');

    final QueryValue[] values = this.values;
    final int valueCount = values.length;
    for (int i = 0; i < valueCount; i++) {
      if (i > 0) {
        sql.append(", ");
      }

      final QueryValue queryValue = values[i];
      if (queryValue instanceof Value) {
        final Value value = (Value)queryValue;
        value.appendDefaultSql(query, recordStore, sql);
      } else {
        queryValue.appendSql(query, recordStore, sql);
      }

    }
    sql.append(')');
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    for (final QueryValue queryValue : this.values) {
      JdbcFieldDefinition jdbcField = this.jdbcField;
      if (queryValue instanceof Value) {
        final Value valueWrapper = (Value)queryValue;
        final Object value = valueWrapper.getQueryValue();
        if (jdbcField == null) {
          jdbcField = JdbcFieldDefinitions.newFieldDefinition(value);
        }
        try {
          index = jdbcField.setPreparedStatementValue(statement, index, value);
        } catch (final SQLException e) {
          throw Exceptions.wrap(e);
        }
      } else {
        index = queryValue.appendParameters(index, statement);
      }
    }
    return index;
  }

  @Override
  public CollectionValue clone() {
    final CollectionValue clone = (CollectionValue)super.clone();
    return clone;
  }

  public boolean containsValue(final Object valueTest) {
    final CodeTable codeTable = this.codeTable;
    for (final QueryValue queryValue : this.values) {
      Object value;
      if (queryValue instanceof Value) {
        final Value valueWrapper = (Value)queryValue;
        value = valueWrapper.getValue();
      } else {
        value = queryValue;
      }
      if (value != null) {
        if (codeTable != null) {
          value = codeTable.getIdentifier(value);
        }
        value = Value.toValue(value);
        if (DataType.equal(valueTest, value)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof CollectionValue) {
      final CollectionValue condition = (CollectionValue)obj;
      return super.equals(condition);
    } else {
      return false;
    }
  }

  public ColumnReference getField() {
    return this.field;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V getValue(final MapEx record) {
    final List<Object> values = new ArrayList<>();
    for (final QueryValue queryValue : this.values) {
      final Object value = queryValue.getValue(record);
      values.add(value);
    }
    return (V)values;
  }

  public List<Object> getValues() {
    final CodeTable codeTable = this.codeTable;
    final List<Object> values = new ArrayList<>();
    for (final QueryValue queryValue : this.values) {
      Object value;
      if (queryValue instanceof Value) {
        final Value valueWrapper = (Value)queryValue;
        value = valueWrapper.getValue();
      } else {
        value = queryValue;
      }
      if (value != null) {
        if (codeTable != null) {
          value = codeTable.getIdentifier(value);
        }
        value = Value.toValue(value);
        values.add(value);
      }
    }
    return values;
  }

  @Override
  public void setColumn(final ColumnReference column) {
    this.jdbcField = null;
    if (column == null) {
      this.field = null;
    } else {
      this.field = column.getFieldDefinition();
      if (this.field != null) {
        this.codeTable = this.field.getCodeTable();
        if (column instanceof JdbcFieldDefinition) {
          this.jdbcField = (JdbcFieldDefinition)column;
        } else {
          this.jdbcField = null;
        }
        for (final QueryValue queryValue : this.values) {
          if (queryValue instanceof Value) {
            final Value value = (Value)queryValue;
            value.setColumn(column);
          }
        }
      }
    }
  }

  @Override
  public String toString() {
    return "(" + Strings.toString(getQueryValues()) + ")";
  }
}
