package com.revolsys.record.io.format.odata;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.ListDataType;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.io.PathName;
import com.revolsys.record.Record;
import com.revolsys.record.RecordDataType;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionImpl;

public class ODataRecordDefinition extends RecordDefinitionImpl implements ODataTypeDefinition {

  private final String odataName;

  private final JsonObject entityType;

  public ODataRecordDefinition(final ODataRecordStoreSchema schema, final PathName pathName,
    final JsonObject entityType, final String odataName) {
    super(schema, pathName);
    this.entityType = entityType;
    this.odataName = odataName;
  }

  @Override
  public String getODataName() {
    return this.odataName;
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

  @Override
  public void odataInitialize() {
    if (getFieldCount() == 0) {
      final var baseTypeName = this.entityType.getString("BaseType");
      if (baseTypeName != null) {
        final ODataRecordStore recordStore = getRecordStore();
        final var baseType = recordStore.getODataType(baseTypeName);
        if (baseType instanceof final ODataRecordDefinition recordDefinition) {
          baseType.odataInitialize();
          for (final FieldDefinition field : recordDefinition.getFields()) {
            addField(field.clone());
          }
        }
      }

      for (final var entityField : this.entityType.<JsonObject> getList("Property")) {
        final var fieldName = entityField.getString("Name");
        boolean collection = false;
        String type = entityField.getString("Type");
        if (type.startsWith("Collection(")) {
          collection = true;
          type = type.substring(11, type.length() - 1);
        }
        final ODataRecordStore recordStore = getRecordStore();

        final var odataType = recordStore.getODataType(type);
        DataType dataType;
        if (odataType instanceof final RecordDefinition recordDefinition) {
          dataType = RecordDataType.of(recordDefinition);
        } else if (odataType instanceof final ODataCodeTable codeTable) {
          dataType = codeTable.getUnderlyingType();
        } else {
          dataType = OData.getDataTypeFromEdm(type);
        }
        if (collection) {
          dataType = ListDataType.of(dataType);
        }

        final boolean required = !entityField.getBoolean("Nullable", true);
        final var fieldDefinition = new FieldDefinition(fieldName, dataType, required);
        final int length = entityField.getInteger("MaxLength", 0);
        if (length != 0) {
          fieldDefinition.setLength(length);
        }
        final int precision = entityField.getInteger("Precision", 0);
        try {
          final int scale = entityField.getInteger("Scale", 0);
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
        } catch (final NumberFormatException e) {
        }

        final int srid = entityField.getInteger("SRID", 0);
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
          throw new IllegalStateException("SRID parameters not yet supported");
        }
        if (odataType instanceof final CodeTable codeTable) {
          fieldDefinition.setCodeTable(codeTable);
        }
        addField(fieldDefinition);
      }

      final var idFields = this.entityType.getJsonObject("Key")
        .<JsonObject> getList("PropertyRef");
      if (!idFields.isEmpty()) {
        final var idFieldNames = idFields.map(n -> n.getString("Name"))
          .toList();
        setIdFieldNames(idFieldNames);
      }
    }
  }
}
