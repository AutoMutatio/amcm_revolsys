package com.revolsys.jdbc.field;

import java.util.Map;

import com.revolsys.record.schema.FieldDefinition;

public interface JdbcFieldFactory {
  FieldDefinition newField(final String dbName, final String name, final int sqlType,
    final String dataType, final int length, final int scale, final boolean required,
    final String description, Map<String, Object> properties);
}
