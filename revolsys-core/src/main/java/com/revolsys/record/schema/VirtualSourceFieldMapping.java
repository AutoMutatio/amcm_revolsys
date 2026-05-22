package com.revolsys.record.schema;

import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.function.Function4;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.schema.AbstractTableRecordStore.VirtualField;

public record VirtualSourceFieldMapping(String virtualFieldName, String sourceFieldName,
  DataType dataType) {
  public VirtualSourceFieldMapping(final String virtualFieldName, final String sourceFieldName) {
    this(virtualFieldName, sourceFieldName, DataTypes.STRING);
  }

  public void addVirtualField(final AbstractTableRecordStore recordStore,
    final Function4<Query, VirtualField, String, String[], QueryValue> newQueryValue) {
    final var field = new VirtualField(recordStore, this.virtualFieldName,
      rd -> rd.addField(this.virtualFieldName, this.dataType), (query, virtualField,
        path) -> newQueryValue.apply(query, virtualField, this.sourceFieldName, path));
    recordStore.addVirtualField(field);
  }
}
