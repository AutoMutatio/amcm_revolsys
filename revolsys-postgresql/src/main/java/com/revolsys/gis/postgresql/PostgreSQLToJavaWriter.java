package com.revolsys.gis.postgresql;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.UUID;

import org.jeometry.common.data.type.CollectionDataType;
import org.jeometry.common.data.type.DataType;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.io.PathName;

import com.revolsys.collection.list.ListEx;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.jdbc.io.JdbcToJavaWriter;
import com.revolsys.record.io.format.json.Json;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionImpl;

public class PostgreSQLToJavaWriter extends JdbcToJavaWriter {

  public PostgreSQLToJavaWriter(final String packageName, final Path file) throws IOException {
    super(packageName, file);
  }

  protected ListEx<PathName> getParents(final RecordDefinition recordDefinition) {
    final String schemaName = recordDefinition.getSchema().getName();
    final String tableName = recordDefinition.getName();
    return recordDefinition.getRecordStore()
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
            """, schemaName, tableName))
      .getRecords()
      .map(r -> PathName.newPathName(r.getString("parentSchemaName"))
        .newChild(r.getString("parentTableName")));
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
  public void writeRecordDefinitionAfter(final RecordDefinition recordDefinition) {
    final ListEx<PathName> parents = getParents(recordDefinition);
    ((RecordDefinitionImpl)recordDefinition).setParentPathNames(parents);
    if (!parents.isEmpty()) {
      final PrintWriter out = getOut();
      out.print("      .setParentPathNames(\"");
      out.print(parents.join("\", \""));
      out.println("\")");

    }
  }

}
