package com.revolsys.gis.postgresql;

import java.io.PrintWriter;
import java.util.List;
import java.util.UUID;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.CollectionDataType;
import org.jeometry.common.data.type.DataType;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.io.PathName;

import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryDataTypes;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.io.PathUtil;
import com.revolsys.jdbc.JdbcUtils;
import com.revolsys.jdbc.io.JdbcDdlWriter;
import com.revolsys.record.Record;
import com.revolsys.record.io.format.json.Json;
import com.revolsys.record.property.ShortNameProperty;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Property;

public class PostgreSQLDdlWriter extends JdbcDdlWriter {
  public PostgreSQLDdlWriter() {
  }

  public PostgreSQLDdlWriter(final PrintWriter out) {
    super(out);
  }

  protected List<PathName> getParents(final RecordDefinition recordDefinition) {
    final List<PathName> parents = recordDefinition.getRecordStore()
      .newQuery()
      .setSql(String.format("""
            select
            rn.nspname "schemaName",
            rc.relname "tableName",
            pn.nspname "parentSchemaName",
            pc.relname "parentTableName",
            i.inhseqno
          from
            pg_inherits i
              join pg_class rc on rc."oid" = i.inhrelid
                join pg_namespace  rn on rc.relnamespace  = rn.oid
              join pg_class pc on pc."oid" = i.inhparent
                join pg_namespace  pn on pc.relnamespace  = pn.oid
            where rn.nspname = '%1s' and rc.relname = '%2s'
            order by i.inhseqno
            """, recordDefinition.getSchema().getName(), recordDefinition.getName()))
      .getRecords()
      .map(r -> PathName.newPathName(r.getString("parentSchemaName"))
        .newChild(r.getString("parentTableName")));
    return parents;
  }

  @Override
  public String getSequenceName(final RecordDefinition recordDefinition) {
    final String typePath = recordDefinition.getPath();
    final String schema = JdbcUtils.getSchemaName(typePath);
    final ShortNameProperty shortNameProperty = ShortNameProperty.getProperty(recordDefinition);
    String shortName = null;
    if (shortNameProperty != null) {
      shortName = shortNameProperty.getShortName();
    }
    if (Property.hasValue(shortName) && shortNameProperty.isUseForSequence()) {
      final String sequenceName = schema + "." + shortName.toLowerCase() + "_seq";
      return sequenceName;
    } else {
      final String tableName = PathUtil.getName(typePath).toLowerCase();
      final String idFieldName = recordDefinition.getIdFieldName().toLowerCase();
      return schema + "." + tableName + "_" + idFieldName + "_seq";
    }
  }

  public void writeAddGeometryColumn(final RecordDefinition recordDefinition) {
    final PrintWriter out = getOut();
    final String typePath = recordDefinition.getPath();
    String schemaName = JdbcUtils.getSchemaName(typePath);
    if (schemaName.length() == 0) {
      schemaName = "public";
    }
    final String tableName = PathUtil.getName(typePath);
    final FieldDefinition geometryField = recordDefinition.getGeometryField();
    if (geometryField != null) {
      final GeometryFactory geometryFactory = geometryField.getGeometryFactory();
      final String name = geometryField.getName();
      String geometryType = "GEOMETRY";
      final DataType dataType = geometryField.getDataType();
      if (dataType == GeometryDataTypes.POINT) {
        geometryType = "POINT";
      } else if (dataType == GeometryDataTypes.LINE_STRING) {
        geometryType = "LINESTRING";
      } else if (dataType == GeometryDataTypes.POLYGON) {
        geometryType = "POLYGON";
      } else if (dataType == GeometryDataTypes.MULTI_POINT) {
        geometryType = "MULTIPOINT";
      } else if (dataType == GeometryDataTypes.MULTI_LINE_STRING) {
        geometryType = "MULTILINESTRING";
      } else if (dataType == GeometryDataTypes.MULTI_POLYGON) {
        geometryType = "MULTIPOLYGON";
      }
      out.print("select addgeometrycolumn('");
      out.print(schemaName.toLowerCase());
      out.print("', '");
      out.print(tableName.toLowerCase());
      out.print("','");
      out.print(name.toLowerCase());
      out.print("',");
      out.print(geometryFactory.getHorizontalCoordinateSystemId());
      out.print(",'");
      out.print(geometryType);
      out.print("', ");
      out.print(geometryFactory.getAxisCount());
      out.println(");");

    }
  }

  public void writeAlterOwner(final String objectType, final String objectName,
    final String owner) {
    final PrintWriter out = getOut();
    out.print("ALTER ");
    out.print(objectType);
    out.print(" ");
    out.print(objectName);
    out.print(" OWNER TO ");
    out.print(owner);
    out.println(";");
  }

  public void writeAlterTableOwner(final String typePath, final String owner) {
    final PrintWriter out = getOut();
    out.print("ALTER ");
    final String objectType = "TABLE";
    out.print(objectType);
    out.print(" ");
    writeTableName(typePath);
    out.print(" OWNER TO ");
    out.print(owner);
    out.println(";");
  }

  @Override
  public void writeColumnDataType(final FieldDefinition attribute) {
    final DataType dataType = attribute.getDataType();
    writeColumnDataType(attribute, dataType);
  }

  protected void writeColumnDataType(final FieldDefinition attribute, final DataType dataType) {
    final int length = attribute.getLength();
    final PrintWriter out = getOut();
    if (dataType == DataTypes.BOOLEAN) {
      out.print("boolean");
    } else if (dataType == DataTypes.BYTE) {
      out.print("NUMBER(3)");
    } else if (dataType == DataTypes.SHORT) {
      out.print("smallint");
    } else if (dataType == DataTypes.INT) {
      out.print("integer");
    } else if (dataType == DataTypes.LONG) {
      out.print("bigint");
    } else if (dataType == DataTypes.FLOAT) {
      out.print("real");
    } else if (dataType == DataTypes.DOUBLE) {
      out.print("double precision");
    } else if (dataType == DataTypes.SQL_DATE) {
      out.print("date");
    } else if (dataType == DataTypes.DATE_TIME || dataType == DataTypes.UTIL_DATE) {
      out.print("timestamp");
    } else if (dataType == DataTypes.BIG_INTEGER) {
      out.print("NUMERIC(");
      out.print(attribute.getLength());
      out.print(')');
    } else if (dataType == DataTypes.DECIMAL) {
      out.print("NUMERIC(");
      out.print(attribute.getLength());
      final int scale = attribute.getScale();
      if (scale >= 0) {
        out.print(',');
        out.print(scale);
      }
      out.print(')');
    } else if (dataType == DataTypes.STRING_CASE_INSESITIVE) {
      out.print("citext");
    } else if (dataType == DataTypes.STRING) {
      if (length == 0) {
        out.print("text");
      } else {
        out.print("varchar(");
        out.print(length);
        out.print(")");
      }
    } else if (dataType == DataTypes.INSTANT) {
      out.print("timestamp with timezone");
    } else if (dataType == Json.JSON_TYPE) {
      out.print("jsonb");
    } else if (Geometry.class.isAssignableFrom(dataType.getJavaClass())) {
      out.print("geometry");
    } else if (UUID.class.isAssignableFrom(dataType.getJavaClass())) {
      out.print("uuid");
    } else if (dataType instanceof final CollectionDataType colType) {
      final DataType contentType = colType.getContentType();
      writeColumnDataType(attribute, contentType);
      out.print("[]");
    } else {
      throw new IllegalArgumentException("Unknown data type " + dataType);
    }
  }

  @Override
  public void writeCreateSchema(final String schemaName) {
    final PrintWriter out = getOut();
    out.print("CREATE SCHEMA ");
    out.print(schemaName);
    out.println(";");
  }

  @Override
  public void writeCreateTableAfter(final RecordDefinition recordDefinition) {
    final List<PathName> parents = getParents(recordDefinition);
    if (!parents.isEmpty()) {
      final PrintWriter out = getOut();
      out.print(" INHERITS (");
      boolean first = true;
      for (final PathName parent : parents) {
        if (first) {
          first = false;
        } else {
          out.print(", ");
        }
        writeTableName(parent);
      }
      out.print(")");
    }
  }

  public void writeGeneratedDropAdd(final RecordDefinition recordDefinition) {
    final PrintWriter out = getOut();
    final String typePath = recordDefinition.getPath();
    final List<PathName> parents = getParents(recordDefinition);

    if (!parents.isEmpty()) {
      out.print("ALTER TABLE ");
      writeTableName(typePath);
      boolean first = true;
      for (final PathName parent : parents) {
        if (first) {
          first = false;
        } else {
          out.println(",");
        }
        out.println();
        out.print("  NO INHERIT ");
        writeTableName(parent);
      }
      out.println();
      out.println(";");
    }

    out.println();
    for (int i = 0; i < recordDefinition.getFieldCount(); i++) {
      final FieldDefinition field = recordDefinition.getField(i);

      String defaultStatement = field.getDefaultStatement();
      if (defaultStatement != null) {
        defaultStatement = defaultStatement.replaceAll("\\:\\:\\w+$", "");
        if (field.isGenerated()) {
          out.print("ALTER TABLE ");
          writeTableName(typePath);
          out.println();
          out.print("  DROP IF EXISTS ");
          writeFieldName(out, field);
          out.println(',');
          out.print("  ADD ");
          writeFieldName(out, field);
          out.print(" GENERATED ALWAYS AS (");
          out.print(defaultStatement);
          out.print(") STORED");
          if (field.isRequired()) {
            out.print(" NOT NULL");
          }
          out.println();
          out.println(";");
        }
      }

    }
    if (!parents.isEmpty()) {
      out.print("ALTER TABLE ");
      writeTableName(typePath);
      boolean first = true;
      for (final PathName parent : parents) {
        if (first) {
          first = false;
        } else {
          out.print(",");
        }
        out.println();
        out.print("  INHERIT ");
        writeTableName(parent);
      }
      out.println();
      out.println(";");
    }
  }

  @Override
  public void writeGeometryRecordDefinition(final RecordDefinition recordDefinition) {
    final PrintWriter out = getOut();
    final String typePath = recordDefinition.getPath();
    String schemaName = JdbcUtils.getSchemaName(typePath);
    if (schemaName.length() == 0) {
      schemaName = "public";
    }
    final String tableName = PathUtil.getName(typePath);
    final FieldDefinition geometryField = recordDefinition.getGeometryField();
    if (geometryField != null) {
      final GeometryFactory geometryFactory = geometryField.getGeometryFactory();
      final String name = geometryField.getName();
      String geometryType = "GEOMETRY";
      final DataType dataType = geometryField.getDataType();
      if (dataType == GeometryDataTypes.POINT) {
        geometryType = "POINT";
      } else if (dataType == GeometryDataTypes.LINE_STRING) {
        geometryType = "LINESTRING";
      } else if (dataType == GeometryDataTypes.POLYGON) {
        geometryType = "POLYGON";
      } else if (dataType == GeometryDataTypes.MULTI_POINT) {
        geometryType = "MULTIPOINT";
      } else if (dataType == GeometryDataTypes.MULTI_LINE_STRING) {
        geometryType = "MULTILINESTRING";
      } else if (dataType == GeometryDataTypes.MULTI_POLYGON) {
        geometryType = "MULTIPOLYGON";
      }
      out.print(
        "INSERT INTO geometry_columns(f_table_catalog, f_table_schema, f_table_name, f_geometry_column, coord_dimension, srid, \"type\") VALUES ('','");
      out.print(schemaName.toLowerCase());
      out.print("', '");
      out.print(tableName.toLowerCase());
      out.print("','");
      out.print(name.toLowerCase());
      out.print("', ");
      out.print(geometryFactory.getAxisCount());
      out.print(",");
      out.print(geometryFactory.getHorizontalCoordinateSystemId());
      out.print(",'");
      out.print(geometryType);
      out.println("');");
    }
  }

  @Override
  public void writeResetSequence(final RecordDefinition recordDefinition,
    final List<Record> records) {
    final PrintWriter out = getOut();
    Long nextValue = 0L;
    for (final Record record : records) {
      final Identifier id = record.getIdentifier();
      for (int i = 0; i < id.getValueCount(); i++) {
        final Object value = id.getValue(i);
        if (value instanceof Number) {
          final Number number = (Number)value;
          final long longValue = number.longValue();
          if (longValue > nextValue) {
            nextValue = longValue;
          }
        }
      }
    }
    nextValue++;
    final String sequeneName = getSequenceName(recordDefinition);
    out.print("ALTER SEQUENCE ");
    out.print(sequeneName);
    out.print(" RESTART WITH ");
    out.print(nextValue);
    out.println(";");
  }
}
