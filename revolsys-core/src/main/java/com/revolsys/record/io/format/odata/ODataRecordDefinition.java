package com.revolsys.record.io.format.odata;

import java.util.List;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.ListDataType;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.io.PathName;
import com.revolsys.record.Record;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinitionImpl;

public class ODataRecordDefinition extends RecordDefinitionImpl {

  public ODataRecordDefinition(final ODataRecordStoreSchema schema, final PathName pathName,
    final JsonObject metadata, String entityTypeName) {
    super(schema, pathName);
    final int dotIndex = entityTypeName.lastIndexOf('.');
    final String entitySchemaName = entityTypeName.substring(0, dotIndex);
    entityTypeName = entityTypeName.substring(dotIndex + 1);
    final JsonObject entityTypeMap = metadata.getJsonObject(entitySchemaName);
    final JsonObject entityType = entityTypeMap.getJsonObject(entityTypeName);
    if (entityType.equalValue("$Kind", "EntityType")) {
      for (final String fieldName : entityType.keySet()) {

        if (!fieldName.startsWith("$")) {
          final JsonObject entityField = entityType.getJsonObject(fieldName);
          final String type = entityField.getString("$Type");
          DataType dataType = OData.getDataTypeFromEdm(type);
          if (entityField.isTrue("$Collection")) {
            dataType = new ListDataType(List.class, dataType);
          }
          final boolean required = !entityField.getBoolean("$Nullable", true);
          final FieldDefinition fieldDefinition = new FieldDefinition(fieldName, dataType,
            required);
          final int length = entityField.getInteger("$MaxLength", 0);
          if (length != 0) {
            fieldDefinition.setLength(length);
          }
          final int precision = entityField.getInteger("$Precision", 0);
          final int scale = entityField.getInteger("$Scale", 0);
          if (scale == 0) {
            if (precision > 0) {
              fieldDefinition.setLength(precision);
            }
          } else {
            if (precision > 0) {
              fieldDefinition.setLength(precision - scale);
            }
            fieldDefinition.setScale(scale);
          }

          final int srid = entityField.getInteger("$SRID", 0);
          if (srid != 0) {
            final int axisCount = entityField.getJsonObject("@Geometry.axisCount")
              .getInteger("$Int", 2);
            final double scaleX = entityField.getJsonObject("@Geometry.scaleX")
              .getDouble("$Float", 0);
            final double scaleY = entityField.getJsonObject("@Geometry.scaleY")
              .getDouble("$Float", 0);
            final double scaleZ = entityField.getJsonObject("@Geometry.scaleZ")
              .getDouble("$Float", 0);
            final GeometryFactory geometryFactory = GeometryFactory.fixed(srid, axisCount, scaleX,
              scaleY, scaleZ);

            fieldDefinition.setGeometryFactory(geometryFactory);
          }
          addField(fieldDefinition);
        }
      }
      final List<String> idFieldNames = entityType.getValue("$Key");
      if (idFieldNames != null) {
        setIdFieldNames(idFieldNames);
      }
    }
  }

  @Override
  public Record newRecord(final MapEx values) {
    final Record record = newRecord();
    for (final FieldDefinition field : getFields()) {
      final String fieldName = field.getName();
      if (values.hasValue(fieldName)) {
        final Object value = values.getValue(field);
        record.setValue(field, value);
      }
    }
    return record;
  }
}
