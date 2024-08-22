package com.revolsys.record;

import java.util.Collection;
import java.util.Map;

import com.revolsys.data.type.AbstractDataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.record.schema.RecordDefinition;

public class RecordDataType extends AbstractDataType {
  public static RecordDataType of(final RecordDefinition recordDefinition) {
    return new RecordDataType(recordDefinition);
  }

  private RecordDefinition recordDefinition;

  public RecordDataType() {
    super("record", Record.class, true);
  }

  public RecordDataType(final RecordDefinition recordDefinition) {
    super(recordDefinition.getQualifiedTableName(), Record.class, true);
    this.recordDefinition = recordDefinition;
  }

  @Override
  public boolean equals(final Object value1, final Object value2) {
    final Record record = toObject(value1);
    final Map<String, Object> map = DataTypes.MAP.toObject(value2);
    return record.equalValuesAll(map);
  }

  @Override
  public boolean equals(final Object value1, final Object value2,
    final Collection<? extends CharSequence> excludeFieldNames) {
    final Record record = toObject(value1);
    final Map<String, Object> map = DataTypes.MAP.toObject(value2);
    return record.equalValuesExclude(map, excludeFieldNames);
  }

  public RecordDefinition getRecordDefinition() {
    return this.recordDefinition;
  }
}
