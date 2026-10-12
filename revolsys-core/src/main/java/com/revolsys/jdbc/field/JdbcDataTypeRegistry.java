package com.revolsys.jdbc.field;

import java.util.HashMap;
import java.util.Map;

import com.revolsys.data.type.DataType;
import com.revolsys.util.Debug;

public class JdbcDataTypeRegistry {

  private final Map<DataType, JdbcDataType> byDataType = new HashMap<>();

  private final Map<Class<?>, JdbcDataType> byClass = new HashMap<>();

  private final Map<String, JdbcDataType> byName = new HashMap<>();

  private final Map<Integer, JdbcDataType> bySqlType = new HashMap<>();

  private JdbcDataTypeRegistry defaultRegistry;

  public JdbcDataTypeRegistry() {
  }

  public JdbcDataTypeRegistry(final JdbcDataTypeRegistry parent) {
    this.defaultRegistry = parent;
  }

  public void addType(final JdbcDataType type) {
    final var dataType = type.getDataType();
    if (!this.byDataType.containsKey(dataType)) {
      this.byDataType.put(dataType, type);
    } else {
      Debug.noOp();
    }
    final var javaClass = dataType.getJavaClass();
    if (!this.byClass.containsKey(javaClass)) {
      this.byClass.put(javaClass, type);
    } else {
      Debug.noOp();
    }
    final var sqlType = type.sqlType();
    if (!this.bySqlType.containsKey(sqlType)) {
      this.bySqlType.put(sqlType, type);
    } else {
      Debug.noOp();
    }
  }

  public void addType(final JdbcDataType type, final String... names) {
    for (final var name : names) {
      if (!this.byName.containsKey(name)) {
        this.byName.put(name, type);
      } else {
        Debug.noOp();
      }
    }
  }

  public JdbcDataType byJavaClass(final Class<?> javaClass) {
    var jdbcType = this.byClass.get(javaClass);
    if (jdbcType == null && this.defaultRegistry != null) {
      jdbcType = this.defaultRegistry.byJavaClass(javaClass);
    }
    return jdbcType;
  }

  public JdbcDataType byName(final String name) {
    var jdbcType = this.byName.get(name);
    if (jdbcType == null && this.defaultRegistry != null) {
      jdbcType = this.defaultRegistry.byName(name);
    }
    return jdbcType;
  }

  public JdbcDataType bySqlType(final int sqlType) {
    var jdbcType = this.bySqlType.get(sqlType);
    if (jdbcType == null && this.defaultRegistry != null) {
      jdbcType = this.defaultRegistry.bySqlType(sqlType);
    }
    return jdbcType;
  }

  public JdbcDataType byType(final DataType dataType) {
    if (dataType instanceof final JdbcDataType type) {
      return type;
    } else {
      var jdbcType = this.byDataType.get(dataType);
      if (jdbcType == null && this.defaultRegistry != null) {
        jdbcType = this.defaultRegistry.byType(dataType);
      }
      return jdbcType;
    }
  }

  public JdbcDataTypeRegistry newRegistry() {
    return new JdbcDataTypeRegistry(this);
  }
}
