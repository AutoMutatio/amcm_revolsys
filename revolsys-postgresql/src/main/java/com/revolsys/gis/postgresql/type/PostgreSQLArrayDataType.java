package com.revolsys.gis.postgresql.type;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import org.postgresql.jdbc.PgConnection;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.data.type.CollectionDataType;
import com.revolsys.data.type.DataType;
import com.revolsys.jdbc.field.JdbcDataType;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class PostgreSQLArrayDataType implements JdbcDataType {

  private final DataType arrayDataType;

  private final JdbcDataType elementDataType;

  private final String elementDbDataType;

  public PostgreSQLArrayDataType(final CollectionDataType arrayDataType,
    final JdbcDataType elementDataType, final String elementDbDataType) {
    this.arrayDataType = arrayDataType;
    this.elementDataType = elementDataType;
    this.elementDbDataType = elementDbDataType;
  }

  @Override
  public void addSelectStatementPlaceHolder(final SqlAppendable sql) {
    sql.append('?');
  }

  @Override
  public void appendSqlValue(final SqlAppendable sql, final RecordStore recordStore,
    final Object queryValue) {
    if (recordStore == null) {
      RecordStore.appendDefaultSql(sql, queryValue);
    } else {
      recordStore.appendSqlValue(sql, queryValue);
    }
  }

  @Override
  public DataType dataType() {
    return this.arrayDataType;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition, final int fieldIndex,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    final Object value = resultSet.getObject(indexes.incrementAndGet());
    if (value instanceof Array) {
      final Array array = (Array)value;
      final ListEx<Object> values = Lists.newArray();
      final ResultSet arrayResultSet = array.getResultSet();
      final ColumnIndexes columnIndex = new ColumnIndexes();
      while (arrayResultSet.next()) {
        columnIndex.columnIndex = 1;
        final Object elementValue = this.elementDataType.getValueFromResultSet(recordDefinition,
          fieldIndex, arrayResultSet, columnIndex, internStrings);
        values.add(elementValue);
      }
      return values;
    }
    return value;
  }

  @Override
  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    Array array;
    if (value instanceof Array) {
      array = (Array)value;
    } else {
      final Object[] values;
      if (value instanceof Collection) {
        final var elements = (Collection<?>)value;
        final int size = elements.size();
        values = new Object[size];
        int i = 0;
        for (final Object element : elements) {
          values[i++] = this.elementDataType.toObject(element);
        }
      } else {
        values = new Object[] {
          this.elementDataType.toObject(value)
        };
      }
      final PgConnection connection = statement.getConnection()
        .unwrap(PgConnection.class);
      array = connection.createArrayOf(this.elementDbDataType, values);
    }

    statement.setArray(parameterIndex, array);
    return parameterIndex + 1;
  }

  @Override
  public int sqlType() {
    return Types.ARRAY;
  }

  @Override
  public String toString() {
    return this.elementDataType.toString() + "[]";
  }
}
