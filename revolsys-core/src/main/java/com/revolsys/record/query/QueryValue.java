package com.revolsys.record.query;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.geometry.model.BoundingBox;
import com.revolsys.geometry.model.BoundingBoxProxy;
import com.revolsys.geometry.model.editor.BoundingBoxEditor;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.io.AbstractJdbcRecordStore;
import com.revolsys.jdbc.io.JdbcRecordDefinition;
import com.revolsys.record.query.functions.EnvelopeIntersects;
import com.revolsys.record.query.functions.WithinDistance;
import com.revolsys.record.query.parser.JSqlParser;
import com.revolsys.record.query.parser.SqlParser;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.util.Property;

public interface QueryValue extends Cloneable, SqlAppendParameters {
  static <V extends QueryValue> List<V> cloneQueryValues(final TableReference oldTable,
    final TableReference newTable, final List<V> values) {
    final List<V> clonedValues = new ArrayList<>();
    for (final QueryValue value : values) {
      if (value == null) {
        clonedValues.add(null);
      } else {
        @SuppressWarnings("unchecked")
        final V clonedValue = (V)value.clone(oldTable, newTable);
        clonedValues.add(clonedValue);
      }
    }
    return clonedValues;
  }

  static QueryValue[] cloneQueryValues(final TableReference oldTable, final TableReference newTable,
    final QueryValue[] oldValues) {
    if (oldValues == null || oldValues.length == 0) {
      return oldValues;
    } else {
      final QueryValue[] clonedValues = new QueryValue[oldValues.length];
      for (int i = 0; i < oldValues.length; i++) {
        final QueryValue value = oldValues[i];
        clonedValues[i] = value.clone(oldTable, newTable);
      }
      return clonedValues;
    }
  }

  static BoundingBox getBoundingBox(final Query query) {
    final Condition whereCondition = query.getWhereCondition();
    return getBoundingBox(whereCondition);
  }

  static BoundingBox getBoundingBox(final QueryValue queryValue) {
    boolean hasBbox = false;
    final BoundingBoxEditor boundingBox = new BoundingBoxEditor();
    if (queryValue != null) {
      for (final QueryValue childValue : queryValue.getQueryValues()) {
        if (childValue instanceof EnvelopeIntersects) {
          final EnvelopeIntersects intersects = (EnvelopeIntersects)childValue;
          final BoundingBox boundingBox1 = getBoundingBox(intersects.getBoundingBox1Value());
          final BoundingBox boundingBox2 = getBoundingBox(intersects.getBoundingBox2Value());
          hasBbox = true;
          boundingBox.addAllBbox(boundingBox1, boundingBox2);
        } else if (childValue instanceof WithinDistance) {
          final WithinDistance withinDistance = (WithinDistance)childValue;
          final BoundingBox boundingBox1 = getBoundingBox(withinDistance.getGeometry1Value());
          final BoundingBox boundingBox2 = getBoundingBox(withinDistance.getGeometry2Value());
          final BoundingBoxEditor withinBoundingBox = BoundingBox.bboxEditor(boundingBox1,
            boundingBox2);
          final double distance = ((Number)((Value)withinDistance.getDistanceValue()).getValue())
            .doubleValue();

          hasBbox = true;
          boundingBox.addBbox(withinBoundingBox.expandDelta(distance));
        } else if (childValue instanceof Value) {
          final Value valueContainer = (Value)childValue;
          final Object value = valueContainer.getValue();
          if (value instanceof BoundingBoxProxy) {
            hasBbox = true;
            boundingBox.addBbox((BoundingBox)value);
          }
        }
      }
    }
    if (hasBbox) {
      return boundingBox;
    } else {
      return null;
    }
  }

  static Condition parseWhere(final RecordDefinition recordDefinition, final String whereClause) {
    if (Property.hasValue(whereClause)) {
      final SqlParser parser = new JSqlParser(recordDefinition);
      return parser.whereToCondition(whereClause);
    } else {
      return null;
    }
  }

  default JdbcFieldDefinition addField(final AbstractJdbcRecordStore recordStore,
    final JdbcRecordDefinition recordDefinition, final ResultSetMetaData metaData,
    final ColumnIndexes columnIndexes) throws SQLException {
    final int columnIndex = columnIndexes.incrementAndGet();
    return recordStore.addField(recordDefinition, metaData, columnIndex);
  }

  default void appendDefaultSelect(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    recordStore.appendQueryValue(statement, sql, this);
  }

  void appendDefaultSql(QueryStatement statement, RecordStore recordStore, SqlAppendable sql);

  default void appendSelect(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    if (recordStore == null) {
      appendDefaultSelect(statement, null, sql);
    } else {
      recordStore.appendSelect(statement, sql, this);
    }
  }

  default void appendSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    if (recordStore == null) {
      appendDefaultSql(statement, null, sql);
    } else {
      recordStore.appendQueryValue(statement, sql, this);
    }
  }

  default void appendSql(final QueryStatement statement, final SqlAppendable sql) {
    final var recordStore = statement.getRecordStore();
    appendSql(statement, recordStore, sql);
  }

  default void changeRecordDefinition(final RecordDefinition oldRecordDefinition,
    final RecordDefinition newRecordDefinition) {
    if (newRecordDefinition != null) {
      for (final QueryValue queryValue : getQueryValues()) {
        if (queryValue != null) {
          queryValue.changeRecordDefinition(oldRecordDefinition, newRecordDefinition);
        }
      }
    }
  }

  QueryValue clone(TableReference oldTable, TableReference newTable);

  default int getFieldIndex() {
    return -1;
  }

  default List<QueryValue> getQueryValues() {
    return Collections.emptyList();
  }

  default String getStringValue(final MapEx record) {
    final Object value = getValue(record);
    return DataTypes.toString(value);
  }

  <V> V getValue(MapEx record);

  default <V> V getValue(final MapEx record, final DataType dataType) {
    final Object value = getValue(record);
    return dataType.toObject(value);
  }

  default Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    throw new UnsupportedOperationException("getValueFromResultSet not implemented");
  }

  default void setColumn(final ColumnReference column) {
  }

  default String toFormattedString() {
    return toString();
  }

  @SuppressWarnings("unchecked")
  default <QV extends QueryValue> QV updateQueryValues(final TableReference oldTable,
    final TableReference newTable,
    final java.util.function.Function<QueryValue, QueryValue> valueHandler) {
    return (QV)this;
  }

}
