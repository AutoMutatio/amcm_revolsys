package com.revolsys.record.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.identifier.ListIdentifier;
import com.revolsys.data.type.DataType;
import com.revolsys.geometry.model.BoundingBox;
import com.revolsys.geometry.model.ClockDirection;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.geometry.model.GeometryFactoryProxy;
import com.revolsys.io.MapSerializer;
import com.revolsys.io.PathName;
import com.revolsys.jdbc.JdbcUtils;
import com.revolsys.record.Record;
import com.revolsys.record.RecordFactory;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.property.RecordDefinitionProperty;
import com.revolsys.record.query.DeleteStatement;
import com.revolsys.record.query.InsertStatement;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.TableReference;
import com.revolsys.util.CaseConverter;

public interface RecordDefinition extends Cloneable, GeometryFactoryProxy, RecordStoreSchemaElement,
  MapSerializer, RecordDefinitionProxy, RecordFactory<Record>, TableReference {

  static RecordDefinitionBuilder builder() {
    return new RecordDefinitionBuilder();
  }

  static RecordDefinition newRecordDefinition(final GeometryFactory geometryFactory,
    final DataType dataType) {
    final String name = dataType.getName();
    return new RecordDefinitionBuilder(name) //
      .addField(dataType) //
      .setGeometryFactory(geometryFactory) //
      .getRecordDefinition() //
    ;
  }

  default void addCodeTable(final String fieldName, final CodeTable codeTable) {
    final FieldDefinition field = getField(fieldName);
    if (field != null) {
      field.setCodeTable(codeTable);
    }
  }

  RecordDefinition addDefaultValue(String fieldName, Object defaultValue);

  void addProperty(RecordDefinitionProperty property);

  @Override
  default void appendQueryValue(final QueryStatement statement, final SqlAppendable sql,
    final QueryValue queryValue) {
    final RecordStore recordStore = getRecordStore();
    queryValue.appendSql(statement, recordStore, sql);
  }

  @Override
  default void appendSelect(final QueryStatement statement, final SqlAppendable sql,
    final QueryValue queryValue) {
    final RecordStore recordStore = getRecordStore();
    queryValue.appendSelect(statement, recordStore, sql);
  }

  @Override
  default void appendSelectAll(final QueryStatement statement, final SqlAppendable sql) {
    boolean first = true;
    for (final FieldDefinition field : getFields()) {
      if (first) {
        first = false;
      } else {
        sql.append(", ");
      }
      final RecordStore recordStore = getRecordStore();
      field.appendSelect(statement, recordStore, sql);
    }
  }

  default void deleteRecord(final Identifier id) {
    getRecordStore().deleteRecord(getTablePath(), id);
  }

  void deleteRecord(Record record);;

  default DeleteStatement deleteStatement() {
    return getRecordStore().deleteStatement(getPathName());
  }

  void destroy();

  BoundingBox getBoundingBox();

  <CT extends CodeTable> CT getCodeTable();

  default CodeTable getCodeTable(final String codeTableName) {
    CodeTable codeTable = getCodeTableByFieldName(codeTableName);
    if (codeTable == null) {
      final RecordStore recordStore = getRecordStore();
      if (recordStore != null) {
        codeTable = recordStore.getCodeTable(codeTableName);
      }
    }
    return codeTable;
  }

  CodeTable getCodeTableByFieldName(CharSequence fieldName);

  String getDbTableName();

  Object getDefaultValue(String fieldName);

  Map<String, Object> getDefaultValues();

  @Override
  FieldDefinition getField(CharSequence name);

  FieldDefinition getField(int index);

  Class<?> getFieldClass(CharSequence name);

  Class<?> getFieldClass(int index);

  /**
   * Get the number of fields supported by the type.
   *
   * @return The number of fields.
   */
  @Override
  int getFieldCount();

  /**
   * Get the index of the named field within the list of fields for the
   * type.
   *
   * @param name The field name.
   * @return The index.
   */
  int getFieldIndex(String name);

  /**
   * Get the maximum length of the field.
   *
   * @param index The field index.
   * @return The maximum length.
   */
  int getFieldLength(int index);

  int getFieldLength(String valueFieldName);

  /**
   * Get the name of the field at the specified index.
   *
   * @param index The field index.
   * @return The field name.
   */
  @Override
  String getFieldName(int index);

  /**
   * Get the names of all the fields supported by the type.
   *
   * @return The field names.
   */
  @Override
  ListEx<String> getFieldNames();

  Set<String> getFieldNamesSet();

  @Override
  ListEx<FieldDefinition> getFields();

  /**
   * Get the maximum number of decimal places of the field
   *
   * @param index The field index.
   * @return The maximum number of decimal places.
   */
  int getFieldScale(int index);

  @Override
  default String getFieldTitle(final String fieldName) {
    final FieldDefinition field = getField(fieldName);
    if (field == null) {
      return CaseConverter.toCapitalizedWords(fieldName);
    } else {
      return field.getTitle();
    }
  }

  List<String> getFieldTitles();

  DataType getFieldType(CharSequence name);

  /**
   * Get the type name of the field at the specified index.
   *
   * @param index The field index.
   * @return The field type name.
   */
  DataType getFieldType(int index);

  default DataType getGeometryDataType() {
    final FieldDefinition field = getGeometryField();
    if (field == null) {
      return null;
    } else {
      return field.getDataType();
    }
  }

  @Override
  FieldDefinition getGeometryField();

  /**
   * Get the index of the primary Geometry field.
   *
   * @return The primary geometry index.
   */
  int getGeometryFieldIndex();

  /**
   * Get the index of all Geometry fields.
   *
   * @return The geometry indexes.
   */
  List<Integer> getGeometryFieldIndexes();

  /**
   * Get the name of the primary Geometry field.
   *
   * @return The primary geometry name.
   */
  @Override
  String getGeometryFieldName();

  /**
   * Get the name of the all Geometry fields.
   *
   * @return The geometry names.
   */
  List<String> getGeometryFieldNames();

  default List<FieldDefinition> getGeometryFields() {
    final List<FieldDefinition> fields = new ArrayList<>();
    for (final int fieldIndex : getGeometryFieldIndexes()) {
      final FieldDefinition field = getField(fieldIndex);
      fields.add(field);
    }
    return fields;
  }

  default Identifier getIdentifier(final MapEx values) {
    if (values != null && hasIdField()) {
      final int idFieldCount = getIdFieldCount();
      if (idFieldCount == 1) {
        final String fieldName = getIdFieldName();
        final Object value = values.getValue(fieldName);
        if (value == null) {
          return Identifier.newIdentifier(value);
        }
      } else if (idFieldCount > 0) {
        final List<String> fieldNames = getIdFieldNames();
        boolean notNull = false;
        final Object[] idValues = new Object[idFieldCount];
        int i = 0;
        for (final String fieldName : fieldNames) {
          final Object value = values.getValue(fieldName);
          if (value != null) {
            notNull = true;
          }
          idValues[i++] = value;
        }
        if (notNull) {
          return new ListIdentifier(idValues);
        }
      }
    }
    return null;
  }

  FieldDefinition getIdField();

  int getIdFieldCount();

  /**
   * Get the index of the Unique identifier field.
   *
   * @return The unique id index.
   */
  int getIdFieldIndex();

  /**
   * Get the index of all ID fields.
   *
   * @return The ID indexes.
   */
  List<Integer> getIdFieldIndexes();

  /**
   * Get the name of the Unique identifier field.
   *
   * @return The unique id name.
   */
  @Override
  String getIdFieldName();

  /**
   * Get the name of the all ID fields.
   *
   * @return The id names.
   */
  @Override
  List<String> getIdFieldNames();

  List<FieldDefinition> getIdFields();

  ClockDirection getPolygonRingDirection();

  @Override
  default String getQualifiedTableName() {
    final PathName pathName = getPathName();
    return JdbcUtils.getQualifiedTableName(pathName.toString());
  }

  @Override
  default RecordDefinition getRecordDefinition() {
    return this;
  }

  RecordDefinitionFactory getRecordDefinitionFactory();

  @Override
  <R extends Record> RecordFactory<R> getRecordFactory();

  @Override
  <V extends RecordStore> V getRecordStore();

  @Override
  default RecordDefinition getTableReference() {
    return this;
  }

  default String getTitle() {
    final PathName typeName = getPathName();
    final String name = typeName.getName();
    return CaseConverter.toCapitalizedWords(name);
  }

  /**
   * Check to see if the type has the specified field name.
   *
   * @param name The name of the field.
   * @return True id the type has the field, false otherwise.
   */
  @Override
  boolean hasField(CharSequence name);

  boolean hasGeometryField();

  @Override
  boolean hasIdField();

  default InsertStatement insertStatement() {
    return getRecordStore().insertStatement(getPathName());
  }

  boolean isFieldRequired(CharSequence name);

  /**
   * Return true if a value for the field is required.
   *
   * @param index The field index.
   * @return True if the field is required, false otherwise.
   */
  boolean isFieldRequired(int index);

  boolean isInstanceOf(RecordDefinition classDefinition);

  Record newRecord();

  default Record newRecord(final MapEx values) {
    final Record newRecord = newRecord();
    newRecord.setValues(values);
    return newRecord;
  }

  default Record newRecord(final Record record) {
    final Record newRecord = newRecord();
    newRecord.setValues(record);
    return newRecord;
  }

  @Override
  default Record newRecord(final RecordDefinition recordDefinition) {
    return newRecord();
  }

  void setDefaultValues(Map<String, ? extends Object> defaultValues);

  void setGeometryFactory(com.revolsys.geometry.model.GeometryFactory geometryFactory);

  void setTableAlias(String tableAlias);

  default void validateRecord(final MapEx record) {
    for (final FieldDefinition field : getFields()) {
      field.validate(record);
    }
  }

}
